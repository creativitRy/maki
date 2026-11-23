use std::time::Duration;

use maki_config::{
    DEFAULT_MAX_RETRIES, DEFAULT_MAX_TIMEOUT_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_MAX_MS,
    ProviderConfig,
};
use tracing::warn;

/// Ceiling for a server sent `Retry-After`. Generous, because the whole point
/// of obeying the header is to sit out a window only the server knows the
/// length of, but `Retry-After: 86400` is a real thing servers send and parking
/// a headless or acp turn for a day with nobody around to press esc is not an
/// option.
const RETRY_AFTER_CAP: Duration = Duration::from_secs(60 * 60);
const FAIL_FAST_TRANSIENT_HINT_MAX: Duration = Duration::from_secs(5);
const FAIL_FAST_MAX_TRANSIENT_RETRIES: u32 = 2;

/// What kind of failure a retry is paying for. The ways out of here are not
/// the same: some errors say "the provider is having a bad day", one says "you
/// are out of money until next month", one says "we may already have billed
/// you for this", and one says "there may be nothing at that address at all".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryKind {
    /// 5xx and network failures on a connection that was made. Nothing came
    /// back, so trying again only costs time.
    Transient,
    /// A connection that was never established: refused, a host name that
    /// does not resolve, a handshake that failed, or a proxy that refused the
    /// tunnel.
    Connect,
    /// A 429. Whether the server sent a `Retry-After` decides which budget it
    /// draws from, so [`RetryState`] splits it further.
    RateLimit,
    /// A stream that went quiet. The request was accepted and output tokens may
    /// already be on the bill, so every retry costs real money.
    Timeout,
}

/// Which counter a failure draws from, and whether that counter has a ceiling.
#[derive(Debug, Clone, Copy)]
enum Budget {
    /// Unbounded: a provider outage can outlast any attempt count, and an agent
    /// left running overnight should still be there in the morning.
    Transient,
    /// Unbounded: the server named the moment to come back, so waiting it out
    /// is what it asked for. Anthropic's Claude Code workspace limits arrive
    /// this way and clear on their own.
    HintedRateLimit,
    /// Bounded: a 429 with no `Retry-After` is how a spend cap reads, and that
    /// one does not clear until the next billing period, so retrying is
    /// documented not to help.
    RateLimit,
    /// Bounded: see [`RetryKind::Timeout`].
    Timeout,
    /// Bounded, on the same limit as [`Self::RateLimit`] but its own counter.
    ///
    /// "Could not connect" is ambiguous: a cloud provider whose edge is down
    /// reads exactly like a local server nobody started. We bound it because
    /// nothing listening is by far the more common cause, and a typo in a
    /// `base_url` has to end in an error rather than a run that never stops
    /// trying.
    Connect,
}

const BUDGETS: usize = 5;

impl Budget {
    fn of(kind: RetryKind, hint: Option<Duration>) -> Self {
        match kind {
            RetryKind::Transient => Self::Transient,
            RetryKind::Connect => Self::Connect,
            RetryKind::RateLimit if hint.is_some() => Self::HintedRateLimit,
            RetryKind::RateLimit => Self::RateLimit,
            RetryKind::Timeout => Self::Timeout,
        }
    }

    fn limit(self, policy: &RetryPolicy) -> Option<u32> {
        match self {
            Self::Transient => policy.max_transient_retries,
            Self::HintedRateLimit => policy.max_hinted_rate_limit_retries,
            Self::RateLimit => Some(policy.max_rate_limit_retries.unwrap_or(policy.max_retries)),
            Self::Connect => Some(policy.max_retries),
            Self::Timeout => Some(policy.max_timeout_retries),
        }
    }
}

/// How patient a run is with a provider that keeps failing. Read from user
/// config, because a rate limited cloud endpoint and a local llama.cpp want
/// very different numbers.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Budget for rate limits the server gave no `Retry-After` for, and,
    /// counted separately, for connections that were never established. `0`
    /// never retries either.
    pub max_retries: u32,
    pub max_timeout_retries: u32,
    pub max_transient_retries: Option<u32>,
    pub max_hinted_rate_limit_retries: Option<u32>,
    /// Cap on server-sent `Retry-After` waits; `None` trims hints to
    /// [`RETRY_AFTER_CAP`], while `Some(max)` fails fast above `max` and
    /// honors waits up to `max`.
    pub max_retry_after: Option<Duration>,
    pub max_rate_limit_retries: Option<u32>,
}

impl RetryPolicy {
    pub fn apply_fail_fast_rate_limit(&mut self) {
        self.max_rate_limit_retries = Some(0);
        self.max_hinted_rate_limit_retries = Some(0);
        self.max_transient_retries = Some(
            self.max_transient_retries
                .map_or(FAIL_FAST_MAX_TRANSIENT_RETRIES, |limit| {
                    limit.min(FAIL_FAST_MAX_TRANSIENT_RETRIES)
                }),
        );
        self.max_retry_after = Some(
            self.max_retry_after
                .map_or(FAIL_FAST_TRANSIENT_HINT_MAX, |limit| {
                    limit.min(FAIL_FAST_TRANSIENT_HINT_MAX)
                }),
        );
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(DEFAULT_RETRY_BASE_MS),
            max_delay: Duration::from_millis(DEFAULT_RETRY_MAX_MS),
            max_retries: DEFAULT_MAX_RETRIES,
            max_timeout_retries: DEFAULT_MAX_TIMEOUT_RETRIES,
            max_transient_retries: None,
            max_hinted_rate_limit_retries: None,
            max_retry_after: None,
            max_rate_limit_retries: None,
        }
    }
}

impl From<&ProviderConfig> for RetryPolicy {
    fn from(config: &ProviderConfig) -> Self {
        Self {
            base_delay: Duration::from_millis(config.retry_base_ms),
            max_delay: Duration::from_millis(config.retry_max_ms),
            max_retries: config.max_retries,
            max_timeout_retries: config.max_timeout_retries,
            max_transient_retries: config.max_transient_retries,
            max_hinted_rate_limit_retries: config.max_hinted_rate_limit_retries,
            max_retry_after: config.max_retry_after,
            max_rate_limit_retries: None,
        }
    }
}

#[derive(Debug)]
pub struct RetryState {
    policy: RetryPolicy,
    spent: [u32; BUDGETS],
    /// Keys left to try, which is not a budget: a fresh key cures a different
    /// problem than waiting does, so a rotation costs no time and no retry.
    ///
    /// Counted per request because `KeyPool`'s index is shared. Two turns
    /// rotating at once shove that index around, and neither could tell it had
    /// been all the way around the pool. A local count bounds the walk
    /// regardless: the worst a jumping index costs is a skipped key, never a
    /// spin.
    rotations_left: u32,
}

impl RetryState {
    /// `key_count` is how many keys the provider has; one means there is
    /// nowhere to rotate to.
    pub fn new(policy: RetryPolicy, key_count: usize) -> Self {
        Self {
            policy,
            spent: [0; BUDGETS],
            rotations_left: u32::try_from(key_count)
                .unwrap_or(u32::MAX)
                .saturating_sub(1),
        }
    }

    /// Books a step of the key walk, or `false` once every key has been tried.
    pub fn book_rotation(&mut self) -> bool {
        if self.rotations_left == 0 {
            return false;
        }
        self.rotations_left -= 1;
        true
    }

    /// Books the next retry and says how long to wait, or `None` once this
    /// failure's budget is spent.
    ///
    /// Budgets never touch: a flaky connection that burns its whole budget must
    /// not leave a later rate limit with nothing left to spend. `hint` is a
    /// `Retry-After` the server sent, and it knows its own window better than
    /// we do, within [`RetryPolicy::base_delay`] and [`RETRY_AFTER_CAP`].
    pub fn next_delay(&mut self, kind: RetryKind, hint: Option<Duration>) -> Option<Duration> {
        if let (Some(after), Some(max_after)) = (hint, self.policy.max_retry_after)
            && (max_after.is_zero() || after > max_after)
        {
            return None;
        }
        let budget = Budget::of(kind, hint);
        let spent = &mut self.spent[budget as usize];
        if budget
            .limit(&self.policy)
            .is_some_and(|limit| *spent >= limit)
        {
            return None;
        }
        *spent += 1;
        let attempt = *spent;

        // The hint is obeyed for any retryable error, not only a 429, so a 503
        // carrying `Retry-After: 3600` parks an unbounded retry for up to
        // `RETRY_AFTER_CAP`. The server named the moment to come back, and Esc
        // ends the wait, so we take it at its word.
        let Some(after) = hint else {
            return Some(self.backoff(attempt));
        };
        let cap = self.policy.max_retry_after.unwrap_or(RETRY_AFTER_CAP);
        if after > cap {
            warn!(
                retry_after_ms = after.as_millis() as u64,
                cap_ms = cap.as_millis() as u64,
                "Retry-After above cap, trimming"
            );
        }
        // The floor matters as much as the cap: this budget is unbounded, and a
        // server answering `Retry-After: 1` forever would otherwise become a
        // one-request-per-second hammer on the rate limiter that is already
        // saying no.
        Some(after.clamp(self.policy.base_delay.min(cap), cap))
    }

    /// Half the backoff, plus jitter on top, so a fleet of clients knocked back
    /// by the same 429 does not all come back at the same moment.
    fn backoff(&self, attempt: u32) -> Duration {
        let half = self
            .policy
            .base_delay
            .saturating_mul(attempt)
            .min(self.policy.max_delay)
            / 2;
        let jitter = Duration::from_millis(fastrand::u64(0..=half.as_millis() as u64));
        half + jitter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const BASE: Duration = Duration::from_millis(100);
    const MAX: Duration = Duration::from_millis(250);
    /// Enough rounds that anything unbounded is obviously unbounded, cheap
    /// because nothing here sleeps.
    const MANY: u32 = 1_000;
    /// A pool with nowhere to rotate to, which is what every budget test wants.
    const ONE_KEY: usize = 1;
    /// A pool big enough that a walk over it is obviously more than one step.
    const KEYS: usize = 3;
    const SHORT_HINT: Duration = Duration::from_secs(1);
    const LONG_HINT: Duration = Duration::from_secs(6);
    const BELOW_BASE_CAP: Duration = Duration::from_millis(50);
    const TINY_HINT: Duration = Duration::from_millis(10);
    const TEST_RETRY_BUDGET: u32 = 4;
    const TEST_TIMEOUT_BUDGET: u32 = 3;
    const BOUNDED_RETRIES: u32 = 2;
    const CONFIG_TRANSIENT: u32 = 3;
    const CONFIG_HINTED: u32 = 1;
    const CONFIG_MAX_AFTER: Duration = Duration::from_secs(12);
    const STRICT_TRANSIENT: u32 = 1;
    const STRICT_WAIT: Duration = Duration::from_secs(2);
    const EXPLICIT_HIGH_CAP: Duration = Duration::from_secs(7_200);
    const WITHIN_HIGH_CAP_HINT: Duration = Duration::from_secs(5_000);
    const OVER_HIGH_CAP_HINT: Duration = Duration::from_secs(7_201);

    fn state(max_retries: u32, max_timeout_retries: u32) -> RetryState {
        RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retries,
                max_timeout_retries,
                ..RetryPolicy::default()
            },
            ONE_KEY,
        )
    }

    /// Retries until the budget runs out or `MANY` rounds pass, checking on the
    /// way that every delay lands in the half-to-full band the backoff promises
    /// for that attempt.
    fn spend(state: &mut RetryState, kind: RetryKind) -> u32 {
        let mut retried = 0;
        while retried < MANY
            && let Some(delay) = state.next_delay(kind, None)
        {
            let want = BASE.saturating_mul(retried + 1).min(MAX) / 2;
            assert!((want..=want * 2).contains(&delay), "{delay:?}");
            retried += 1;
        }
        retried
    }

    #[test_case(0, RetryKind::RateLimit, 0    ; "zero_budget_never_retries_an_unhinted_rate_limit")]
    #[test_case(2, RetryKind::RateLimit, 2    ; "an_unhinted_rate_limit_stops_at_its_budget")]
    #[test_case(2, RetryKind::Timeout, 3      ; "timeouts_have_their_own_budget")]
    #[test_case(0, RetryKind::Transient, MANY ; "a_5xx_outlives_every_budget")]
    #[test_case(0, RetryKind::Connect, 0      ; "zero_budget_never_retries_a_refused_connection")]
    #[test_case(2, RetryKind::Connect, 2      ; "a_refused_connection_stops_at_its_budget")]
    fn budget_is_spent_then_exhausted(max_retries: u32, kind: RetryKind, expected: u32) {
        assert_eq!(spend(&mut state(max_retries, 3), kind), expected);
    }

    #[test]
    fn budgets_cannot_eat_each_other() {
        let mut state = state(2, 3);
        assert_eq!(spend(&mut state, RetryKind::Timeout), 3);
        assert_eq!(spend(&mut state, RetryKind::Connect), 2);
        assert_eq!(spend(&mut state, RetryKind::RateLimit), 2);
    }

    /// A hinted 429 is unbounded and must not spend the budget kept for the
    /// unhinted kind, which is the one that means "out of money".
    #[test]
    fn a_hinted_rate_limit_leaves_the_unhinted_budget_alone() {
        let mut state = state(2, 3);
        let hint = Some(Duration::from_secs(5));
        for _ in 0..MANY {
            assert!(state.next_delay(RetryKind::RateLimit, hint).is_some());
        }
        assert_eq!(spend(&mut state, RetryKind::RateLimit), 2);
    }

    #[test_case(Duration::from_secs(5), Duration::from_secs(5)  ; "sane_hint_beats_the_guess")]
    #[test_case(Duration::from_secs(86_400), RETRY_AFTER_CAP    ; "absurd_hint_is_trimmed")]
    // An unbounded budget plus a zero or near-zero hint is a tight loop against
    // a rate limiter, so the floor is load bearing, not decoration.
    #[test_case(Duration::ZERO, BASE                            ; "zero_hint_is_raised_to_the_base_delay")]
    #[test_case(Duration::from_millis(1), BASE                  ; "tiny_hint_is_raised_to_the_base_delay")]
    fn server_hint_is_honored_between_the_floor_and_the_cap(hint: Duration, expected: Duration) {
        assert_eq!(
            state(1, 1).next_delay(RetryKind::RateLimit, Some(hint)),
            Some(expected)
        );
    }

    /// A base delay above the cap must clamp, not panic on an inverted range.
    #[test]
    fn a_base_delay_above_the_cap_still_clamps() {
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: RETRY_AFTER_CAP * 2,
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        let hint = Some(Duration::from_secs(1));
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, hint),
            Some(RETRY_AFTER_CAP)
        );
    }

    /// Walks the pool until `book_rotation` says stop, which must be once every
    /// key has been tried: the one the request started on, plus one per step.
    fn walk(state: &mut RetryState) -> u32 {
        let mut rotations = 0;
        while rotations < MANY && state.book_rotation() {
            rotations += 1;
        }
        rotations
    }

    #[test_case(1, 0  ; "a_single_key_has_nowhere_to_rotate_to")]
    #[test_case(2, 1  ; "a_pair_of_keys_rotates_once")]
    #[test_case(10, 9 ; "ten_keys_rotate_nine_times")]
    fn the_key_walk_visits_every_key_once(key_count: usize, expected: u32) {
        assert_eq!(
            walk(&mut RetryState::new(RetryPolicy::default(), key_count)),
            expected
        );
    }

    /// Rotating is a different remedy than waiting, so a walked out pool has to
    /// leave the error every budget it started with.
    #[test]
    fn a_walked_pool_costs_no_budget() {
        const RETRIES: u32 = 2;
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retries: RETRIES,
                max_timeout_retries: RETRIES,
                ..RetryPolicy::default()
            },
            KEYS,
        );
        assert_eq!(walk(&mut state), KEYS as u32 - 1);
        assert_eq!(spend(&mut state, RetryKind::RateLimit), RETRIES);
        assert_eq!(spend(&mut state, RetryKind::Timeout), RETRIES);
    }

    fn count_retries(state: &mut RetryState, kind: RetryKind, hint: Option<Duration>) -> u32 {
        (0..MANY)
            .take_while(|_| state.next_delay(kind, hint).is_some())
            .count() as u32
    }

    #[test_case(None, None, MANY ; "unbounded_unhinted")]
    #[test_case(Some(0), None, 0 ; "zero_disables_unhinted")]
    #[test_case(Some(0), Some(SHORT_HINT), 0 ; "zero_disables_hinted")]
    #[test_case(Some(BOUNDED_RETRIES), None, BOUNDED_RETRIES ; "capped_unhinted")]
    #[test_case(Some(BOUNDED_RETRIES), Some(SHORT_HINT), BOUNDED_RETRIES ; "capped_hinted")]
    fn max_transient_retries_bounds_transient_attempts(
        cap: Option<u32>,
        hint: Option<Duration>,
        expected: u32,
    ) {
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_transient_retries: cap,
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            count_retries(&mut state, RetryKind::Transient, hint),
            expected
        );
    }

    #[test_case(None, MANY ; "unbounded_hinted_rate_limit")]
    #[test_case(Some(0), 0 ; "zero_fails_fast_on_hinted_rate_limit")]
    #[test_case(Some(BOUNDED_RETRIES), BOUNDED_RETRIES ; "capped_hinted_rate_limit")]
    fn max_hinted_rate_limit_retries_leaves_unhinted_alone(cap: Option<u32>, expected: u32) {
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retries: TEST_RETRY_BUDGET,
                max_hinted_rate_limit_retries: cap,
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            count_retries(&mut state, RetryKind::RateLimit, Some(SHORT_HINT)),
            expected
        );
        assert_eq!(
            count_retries(&mut state, RetryKind::RateLimit, None),
            TEST_RETRY_BUDGET
        );
    }

    #[test]
    fn max_retry_after_zero_rejects_hints_without_spending_budget() {
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retries: TEST_RETRY_BUDGET,
                max_retry_after: Some(Duration::ZERO),
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, Some(Duration::ZERO)),
            None
        );
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, Some(SHORT_HINT)),
            None
        );
        assert_eq!(
            state.next_delay(RetryKind::Transient, Some(Duration::ZERO)),
            None
        );
        assert_eq!(
            count_retries(&mut state, RetryKind::RateLimit, None),
            TEST_RETRY_BUDGET
        );
    }

    #[test]
    fn max_retry_after_enforces_threshold_and_sub_base_cap() {
        let mut bounded = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retries: TEST_RETRY_BUDGET,
                max_transient_retries: Some(BOUNDED_RETRIES),
                max_retry_after: Some(FAIL_FAST_TRANSIENT_HINT_MAX),
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            bounded.next_delay(RetryKind::Transient, Some(LONG_HINT)),
            None
        );
        assert_eq!(
            count_retries(
                &mut bounded,
                RetryKind::Transient,
                Some(FAIL_FAST_TRANSIENT_HINT_MAX),
            ),
            BOUNDED_RETRIES
        );

        let mut below_base = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retry_after: Some(BELOW_BASE_CAP),
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            below_base.next_delay(RetryKind::RateLimit, Some(TINY_HINT)),
            Some(BELOW_BASE_CAP)
        );
        assert_eq!(
            below_base.next_delay(RetryKind::RateLimit, Some(SHORT_HINT)),
            None
        );
    }

    #[test]
    fn from_provider_config_forwards_retry_bounds() {
        let config = ProviderConfig {
            max_transient_retries: Some(CONFIG_TRANSIENT),
            max_hinted_rate_limit_retries: Some(CONFIG_HINTED),
            max_retry_after: Some(CONFIG_MAX_AFTER),
            ..ProviderConfig::default()
        };
        let policy = RetryPolicy::from(&config);
        assert_eq!(policy.max_transient_retries, Some(CONFIG_TRANSIENT));
        assert_eq!(policy.max_hinted_rate_limit_retries, Some(CONFIG_HINTED));
        assert_eq!(policy.max_retry_after, Some(CONFIG_MAX_AFTER));
        assert_eq!(policy.max_rate_limit_retries, None);
    }

    #[test]
    fn explicit_max_retry_after_can_exceed_default_cap() {
        let mut state = RetryState::new(
            RetryPolicy {
                base_delay: BASE,
                max_delay: MAX,
                max_retry_after: Some(EXPLICIT_HIGH_CAP),
                ..RetryPolicy::default()
            },
            ONE_KEY,
        );
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, Some(WITHIN_HIGH_CAP_HINT)),
            Some(WITHIN_HIGH_CAP_HINT)
        );
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, Some(OVER_HIGH_CAP_HINT)),
            None
        );
    }

    fn fail_fast_policy() -> RetryPolicy {
        let mut policy = RetryPolicy {
            base_delay: BASE,
            max_delay: MAX,
            max_retries: TEST_RETRY_BUDGET,
            max_timeout_retries: TEST_TIMEOUT_BUDGET,
            ..RetryPolicy::default()
        };
        policy.apply_fail_fast_rate_limit();
        policy
    }

    #[test_case(RetryKind::RateLimit, None, 0 ; "unhinted_rate_limit_fails_fast")]
    #[test_case(RetryKind::RateLimit, Some(SHORT_HINT), 0 ; "hinted_rate_limit_fails_fast")]
    #[test_case(RetryKind::Transient, Some(LONG_HINT), 0 ; "transient_with_long_hint_fails_fast")]
    #[test_case(RetryKind::Transient, None, FAIL_FAST_MAX_TRANSIENT_RETRIES ; "unhinted_transient_caps_at_two")]
    #[test_case(RetryKind::Transient, Some(FAIL_FAST_TRANSIENT_HINT_MAX), FAIL_FAST_MAX_TRANSIENT_RETRIES ; "short_hint_transient_caps_at_two")]
    #[test_case(RetryKind::Connect, None, TEST_RETRY_BUDGET ; "connect_budget_unchanged")]
    #[test_case(RetryKind::Timeout, None, TEST_TIMEOUT_BUDGET ; "timeout_budget_unchanged")]
    fn apply_fail_fast_rate_limit_bounds_expected_categories(
        kind: RetryKind,
        hint: Option<Duration>,
        expected: u32,
    ) {
        let mut state = RetryState::new(fail_fast_policy(), ONE_KEY);
        assert_eq!(count_retries(&mut state, kind, hint), expected);
    }

    #[test]
    fn apply_fail_fast_rate_limit_preserves_stricter_config() {
        let mut policy = RetryPolicy {
            max_transient_retries: Some(STRICT_TRANSIENT),
            max_retry_after: Some(STRICT_WAIT),
            ..RetryPolicy::default()
        };
        policy.apply_fail_fast_rate_limit();

        assert_eq!(policy.max_transient_retries, Some(STRICT_TRANSIENT));
        assert_eq!(policy.max_retry_after, Some(STRICT_WAIT));
        assert_eq!(policy.max_rate_limit_retries, Some(0));
        assert_eq!(policy.max_hinted_rate_limit_retries, Some(0));
    }
}
