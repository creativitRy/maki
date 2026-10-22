use serde_json::Value;

/// Subset match of a JSON argument pattern against serialized MCP tool input.
///
/// Object keys omitted from the pattern match any value, arrays match a prefix
/// of the argument array, and `*` inside a pattern string is a wildcard, so a
/// literal `*` in an argument cannot be expressed. Everything else compares by
/// equality. Unparseable input never matches.
pub fn matches(pattern: &str, args: &str) -> bool {
    match (serde_json::from_str::<Value>(pattern), serde_json::from_str::<Value>(args)) {
        (Ok(pattern), Ok(args)) => value_matches(&pattern, &args),
        _ => false,
    }
}

fn value_matches(pattern: &Value, value: &Value) -> bool {
    match pattern {
        Value::Object(expected) => {
            let Value::Object(actual) = value else {
                return false;
            };
            expected
                .iter()
                .all(|(key, pattern)| actual.get(key).is_some_and(|v| value_matches(pattern, v)))
        }
        Value::Array(expected) => {
            let Value::Array(actual) = value else {
                return false;
            };
            if expected.len() > actual.len() {
                return false;
            }
            expected.iter().zip(actual).all(|(pattern, value)| value_matches(pattern, value))
        }
        Value::String(pattern) => value
            .as_str()
            .is_some_and(|value| glob_matches(pattern, value)),
        other => other == value,
    }
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let (pattern, value) = (pattern.as_bytes(), value.as_bytes());
    let (mut pi, mut vi) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while vi < value.len() {
        match pattern.get(pi).copied() {
            Some(b'*') => {
                star = Some((pi + 1, vi));
                pi += 1;
            }
            Some(byte) if byte == value[vi] => {
                pi += 1;
                vi += 1;
            }
            _ => match star {
                Some((star_pi, star_vi)) => {
                    star = Some((star_pi, star_vi + 1));
                    pi = star_pi;
                    vi = star_vi + 1;
                }
                None => return false,
            },
        }
    }
    pattern[pi..].iter().all(|&byte| byte == b'*')
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::{glob_matches, matches};

    #[test_case(r#"{}"#, r#"{}"# ; "empty")]
    #[test_case(r#"{"a":1}"#, r#"{"a":1}"# ; "same_object")]
    #[test_case(r#"{"a":1}"#, r#"{"a":1,"b":2}"# ; "extra_keys_ignored")]
    #[test_case(r#"{}"#, r#"{"a":1}"# ; "empty_pattern_matches_anything")]
    #[test_case(r#"{"a":{"b":"x"}}"#, r#"{"a":{"b":"x","c":1}}"# ; "nested_object")]
    #[test_case(r#"{"a":[{"b":"*"}]}"#, r#"{"a":[{"b":"any","c":2}]}"# ; "object_in_array")]
    #[test_case(r#"{"a":[]}"#, r#"{"a":[1]}"# ; "empty_array_prefix")]
    #[test_case(r#"{"a":[1,2]}"#, r#"{"a":[1,2,3]}"# ; "array_prefix")]
    #[test_case(r#"{"cmd":"open *"}"#, r#"{"cmd":"open src/lib.rs"}"# ; "glob_infix")]
    #[test_case(r#"{"path":"*.rs"}"#, r#"{"path":"src/lib.rs"}"# ; "glob_leading")]
    #[test_case(r#"{"path":"src/*"}"#, r#"{"path":"src/lib/main.rs"}"# ; "glob_trailing")]
    #[test_case(r#"{"path":"*"}"#, r#"{"path":"anything"}"# ; "glob_alone")]
    #[test_case(r#"{"path":"*"}"#, r#"{"path":""}"# ; "glob_alone_empty_string")]
    #[test_case(r#"{"cmd":"a*b"}"#, r#"{"cmd":"a*cb"}"# ; "glob_absorbs_literal_star")]
    #[test_case(r#"{"n":1}"#, r#"{"n":1}"# ; "number")]
    #[test_case(r#"{"b":true}"#, r#"{"b":true}"# ; "bool")]
    #[test_case(r#"{"x":null}"#, r#"{"x":null}"# ; "null")]
    #[test_case(r#"{"cmd":"echo \"hi\""}"#, r#"{"cmd":"echo \"hi\""}"# ; "escaped_quotes")]
    #[test_case(r#"{"cmd":"héllo *"}"#, r#"{"cmd":"héllo wörld"}"# ; "unicode")]
    fn matches_subset(pattern: &str, args: &str) {
        assert!(matches(pattern, args));
    }

    #[test_case(r#"{"a":1}"#, r#"{"b":1}"# ; "missing_key")]
    #[test_case(r#"{"a":1}"#, r#"{"a":2}"# ; "different_value")]
    #[test_case(r#"{"a":1}"#, r#"{"a":"1"}"# ; "number_vs_string")]
    #[test_case(r#"{"a":1}"#, r#"{"a":[1]}"# ; "number_vs_array")]
    #[test_case(r#"{"x":null}"#, r#"{"x":"null"}"# ; "null_vs_string")]
    #[test_case(r#"{"a":1}"#, r#"[1]"# ; "object_vs_array")]
    #[test_case(r#"{"a":"*"}"#, r#"{"a":1}"# ; "string_leaf_vs_number")]
    #[test_case(r#"{"a":"*"}"#, r#"{"a":[]}"# ; "string_leaf_vs_array")]
    #[test_case(r#"{"a":[1,2,3]}"#, r#"{"a":[1,2]}"# ; "value_array_shorter")]
    #[test_case(r#"{"a":[1,2]}"#, r#"{"a":[1,3]}"# ; "array_element_mismatch")]
    #[test_case(r#"{"a":[1]}"#, r#"{"a":1}"# ; "array_vs_number")]
    #[test_case(r#"{"cmd":"open *"}"#, r#"{"cmd":"close src/lib.rs"}"# ; "glob_prefix_mismatch")]
    #[test_case(r#"{"path":"*.rs"}"#, r#"{"path":"src/lib.py"}"# ; "glob_suffix_mismatch")]
    #[test_case(r#"not json"#, r#"{}"# ; "invalid_pattern")]
    #[test_case(r#"{"a":"#, r#"{"a":1}"# ; "truncated_pattern")]
    #[test_case(r#"{}"#, r#"not json"# ; "invalid_args")]
    fn rejects(pattern: &str, args: &str) {
        assert!(!matches(pattern, args));
    }

    #[test_case("a*b", "ab_b" ; "backtracks_across_a_star")]
    #[test_case("*", "" ; "star_matches_empty")]
    #[test_case("a*a", "aa" ; "middle_star_empty")]
    fn glob(pattern: &str, value: &str) {
        assert!(glob_matches(pattern, value));
    }

    #[test_case("abc", "" ; "empty_value")]
    #[test_case("a*a", "a" ; "middle_star_needs_two_literals")]
    #[test_case("a*b", "ba" ; "reordered")]
    fn glob_rejects(pattern: &str, value: &str) {
        assert!(!glob_matches(pattern, value));
    }
}
