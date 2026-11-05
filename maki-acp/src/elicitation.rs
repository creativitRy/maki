//! Maps the `question` tool onto ACP form elicitation (`elicitation/create`).
//! The tool keeps its Lua-advertised schema; only execution is rerouted here,
//! so Zed and friends render native forms instead of a spinner nobody can answer.

use std::collections::BTreeMap;

use agent_client_protocol_schema::v1::{
    ClientCapabilities, CreateElicitationRequest, ElicitationContentValue, ElicitationFormMode,
    ElicitationPropertySchema, ElicitationSchema, ElicitationSessionScope, EnumOption,
    MultiSelectPropertySchema, SessionId, StringPropertySchema, ToolCallId,
};
use serde::Deserialize;
use serde_json::Value;

/// Mirrors the Lua question tool's dismiss output so the model sees the same
/// text regardless of frontend.
pub const DISMISSED: &str = "(question dismissed by user)";
const NO_ANSWER: &str = "(no answer)";
const CUSTOM_KEY_SUFFIX: &str = "_custom";
const CUSTOM_TITLE: &str = "Other";
const CUSTOM_DESCRIPTION: &str =
    "Type your own answer instead of choosing one of the options (optional).";
const JETBRAINS_META_KEY: &str = "jetbrains";
const AIR_META_KEY: &str = "air";
const AIR_CUSTOM_ANSWER_KEY: &str = "customAnswer";
const LEGACY_AIR_CUSTOM_ANSWER_KEY: &str = "_askUserQuestionCustomAnswer";
const AIR_EXTENSION_VERSION_KEY: &str = "version";
const AIR_EXTENSION_VERSION: u64 = 1;

#[derive(Deserialize)]
struct Question {
    question: String,
    #[serde(default)]
    header: String,
    #[serde(default)]
    options: Vec<QuestionOption>,
    #[serde(default, rename = "multiSelect", alias = "multiple")]
    multi_select: bool,
}

#[derive(Deserialize)]
struct QuestionOption {
    label: String,
    #[serde(default)]
    description: String,
}

pub fn supports_form(caps: &ClientCapabilities) -> bool {
    caps.elicitation.as_ref().is_some_and(|e| e.form.is_some())
}

fn parse_questions(input: &Value) -> Result<Vec<Question>, String> {
    let questions = input.get("questions").cloned().unwrap_or(Value::Null);
    serde_json::from_value(questions).map_err(|e| format!("invalid questions: {e}"))
}

fn enum_options(options: &[QuestionOption]) -> Vec<EnumOption> {
    options
        .iter()
        .map(|opt| {
            let title = if opt.description.is_empty() {
                opt.label.clone()
            } else {
                format!("{} - {}", opt.label, opt.description)
            };
            EnumOption::new(opt.label.clone(), title)
        })
        .collect()
}

fn property(q: &Question) -> ElicitationPropertySchema {
    let title = q.question.clone();
    if q.options.is_empty() {
        ElicitationPropertySchema::String(StringPropertySchema::new().title(title))
    } else if q.multi_select {
        ElicitationPropertySchema::Array(
            MultiSelectPropertySchema::titled(enum_options(&q.options)).title(title),
        )
    } else {
        ElicitationPropertySchema::String(
            StringPropertySchema::new()
                .title(title)
                .one_of(enum_options(&q.options)),
        )
    }
}

/// The write-in row clients render beside the options. Without a plain string
/// sibling, a single-select renders as radio buttons only.
///
/// The `_meta` marker is what makes a JetBrains client fold the field into the
/// preceding select as its write-in; unmarked, the field is a form page of its
/// own. AIR does not advertise `jetbrains.air` in its capabilities, so the
/// marker goes out unconditionally — `_meta` is an extension object other
/// clients ignore.
fn custom_property(question_key: &str) -> ElicitationPropertySchema {
    let marker = serde_json::json!({ "questionId": question_key, "isCustomAnswer": true });
    let mut air = serde_json::Map::new();
    air.insert(AIR_EXTENSION_VERSION_KEY.to_owned(), AIR_EXTENSION_VERSION.into());
    air.insert(AIR_CUSTOM_ANSWER_KEY.to_owned(), marker.clone());
    let mut meta = serde_json::Map::new();
    meta.insert(LEGACY_AIR_CUSTOM_ANSWER_KEY.to_owned(), marker);
    meta.insert(
        JETBRAINS_META_KEY.to_owned(),
        serde_json::json!({ AIR_META_KEY: serde_json::Value::Object(air) }),
    );
    ElicitationPropertySchema::String(
        StringPropertySchema::new()
            .title(CUSTOM_TITLE)
            .description(CUSTOM_DESCRIPTION)
            .meta(meta),
    )
}

/// Property keys are positional (`question_0`, `question_1`, ...) so answers
/// map back to questions even when headers repeat or are missing. The ids stay
/// opaque to clients: the write-in is recognized by its `_meta`, not its name.
fn key(index: usize) -> String {
    format!("question_{index}")
}

fn custom_key(index: usize) -> String {
    key(index) + CUSTOM_KEY_SUFFIX
}
pub fn form_request(
    session_id: &str,
    tool_call_id: Option<String>,
    input: &Value,
) -> Result<CreateElicitationRequest, String> {
    let questions = parse_questions(input)?;
    if questions.is_empty() {
        return Err("at least one question is required".to_owned());
    }

    let mut schema = ElicitationSchema::new();
    for (i, question) in questions.iter().enumerate() {
        let property_key = key(i);
        schema
            .properties
            .insert(property_key.clone(), property(question));
        if !question.options.is_empty() {
            schema
                .properties
                .insert(custom_key(i), custom_property(&property_key));
        }
    }

    let scope = ElicitationSessionScope::new(SessionId::from(session_id.to_owned()))
        .tool_call_id(tool_call_id.map(ToolCallId::from));
    let message = match questions.as_slice() {
        [only] => only.question.clone(),
        many => format!("{} questions", many.len()),
    };
    Ok(CreateElicitationRequest::new(
        ElicitationFormMode::new(scope, schema),
        message,
    ))
}

fn answer_text(value: Option<&ElicitationContentValue>) -> Option<String> {
    let text = match value? {
        ElicitationContentValue::String(s) => s.trim().to_owned(),
        ElicitationContentValue::StringArray(items) => items
            .iter()
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>()
            .join(", "),
        ElicitationContentValue::Integer(n) => n.to_string(),
        ElicitationContentValue::Number(n) => n.to_string(),
        ElicitationContentValue::Boolean(b) => b.to_string(),
        _ => return None,
    };
    (!text.is_empty()).then_some(text)
}

/// Turns the client's `elicitation/create` result into the same markdown the
/// Lua tool feeds the model: one `header: labels` line per question.
pub fn format_response(input: &Value, raw_result: &str) -> String {
    let Ok(response) = serde_json::from_str::<Value>(raw_result) else {
        return DISMISSED.to_owned();
    };
    if response["action"] != "accept" {
        return DISMISSED.to_owned();
    }
    // Values parse per key: nothing in the schema is `required`, so clients
    // may null out skipped fields, and one unreadable value should cost one
    // answer, not the whole form.
    let content: BTreeMap<String, ElicitationContentValue> = response["content"]
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| Some((k.clone(), serde_json::from_value(v.clone()).ok()?)))
                .collect()
        })
        .unwrap_or_default();
    let questions = parse_questions(input).unwrap_or_default();

    questions
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let label = if q.header.is_empty() {
                format!("Q{}", i + 1)
            } else {
                q.header.clone()
            };
            // The pick and the write-in are one answer: a client that folds
            // the typed text into the select field sends one, a client that
            // keeps the companion field sends both.
            let parts = [key(i), custom_key(i)]
                .into_iter()
                .filter_map(|k| answer_text(content.get(&k)))
                .collect::<Vec<_>>();
            let answer = if parts.is_empty() {
                NO_ANSWER.to_owned()
            } else {
                parts.join(", ")
            };
            format!("{label}: {answer}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use agent_client_protocol_schema::v1::{
        ElicitationCapabilities, ElicitationFormCapabilities, ElicitationScope,
        ElicitationUrlCapabilities,
    };
    use test_case::test_case;

    use super::*;

    fn questions_input() -> Value {
        serde_json::json!({
            "questions": [
                {
                    "question": "Pick a framework",
                    "header": "Framework",
                    "options": [
                        { "label": "axum", "description": "tokio based" },
                        { "label": "actix" }
                    ]
                },
                {
                    "question": "Which features?",
                    "header": "Features",
                    "multiSelect": true,
                    "options": [{ "label": "auth" }, { "label": "uploads" }]
                },
                { "question": "Anything else?" }
            ]
        })
    }

    #[test]
    fn form_request_maps_questions_to_schema() {
        let req = form_request("sess_1", Some("tool_1".to_owned()), &questions_input()).unwrap();
        assert_eq!(req.message, "3 questions");

        let ElicitationScope::Session(scope) = req.scope() else {
            panic!("expected session scope");
        };
        assert_eq!(scope.session_id.0.as_ref(), "sess_1");
        assert_eq!(
            scope.tool_call_id.as_ref().map(|t| t.0.as_ref()),
            Some("tool_1")
        );

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["mode"], "form");
        let props = &json["requestedSchema"]["properties"];
        assert_eq!(props["question_0"]["type"], "string");
        assert_eq!(props["question_0"]["oneOf"][0]["const"], "axum");
        assert_eq!(props["question_1"]["type"], "array");
        assert_eq!(props["question_2"]["type"], "string");
        assert!(props["question_2"].get("oneOf").is_none());
        for index in 0..2 {
            let custom = &props[custom_key(index)];
            assert_eq!(custom["type"], "string");
            assert_eq!(custom["title"], CUSTOM_TITLE);
            assert!(custom.get("oneOf").is_none());
            let marker = &custom["_meta"][JETBRAINS_META_KEY][AIR_META_KEY][AIR_CUSTOM_ANSWER_KEY];
            assert_eq!(marker["questionId"], key(index));
            assert_eq!(marker["isCustomAnswer"], true);
            assert_eq!(
                custom["_meta"][LEGACY_AIR_CUSTOM_ANSWER_KEY]["isCustomAnswer"],
                true
            );
        }
        assert!(props.get(custom_key(2)).is_none());
    }

    #[test]
    fn single_question_is_the_message() {
        let input = serde_json::json!({ "questions": [{ "question": "Proceed?" }] });
        let req = form_request("sess_1", None, &input).unwrap();
        assert_eq!(req.message, "Proceed?");
    }

    #[test_case(serde_json::json!({}) ; "missing_questions")]
    #[test_case(serde_json::json!({ "questions": [] }) ; "empty_questions")]
    fn form_request_rejects_bad_input(input: Value) {
        assert!(form_request("sess_1", None, &input).is_err());
    }

    #[test]
    fn accept_response_formats_answers() {
        let raw = serde_json::json!({
            "action": "accept",
            "content": { "question_0": "axum", "question_1": ["auth", "uploads"] }
        })
        .to_string();
        assert_eq!(
            format_response(&questions_input(), &raw),
            "Framework: axum\nFeatures: auth, uploads\nQ3: (no answer)"
        );
    }

    #[test]
    fn nulled_out_field_costs_one_answer_not_the_form() {
        let raw = serde_json::json!({
            "action": "accept",
            "content": { "question_0": "axum", "question_1": null }
        })
        .to_string();
        assert_eq!(
            format_response(&questions_input(), &raw),
            "Framework: axum\nFeatures: (no answer)\nQ3: (no answer)"
        );
    }

    #[test_case(
        serde_json::json!({ "question_0_custom": "something else" }),
        "Framework: something else\nFeatures: (no answer)\nQ3: (no answer)" ; "write_in_only"
    )]
    #[test_case(
        serde_json::json!({ "question_0": "axum", "question_0_custom": "but add tests" }),
        "Framework: axum, but add tests\nFeatures: (no answer)\nQ3: (no answer)" ; "pick_and_write_in"
    )]
    #[test_case(
        serde_json::json!({ "question_1": [], "question_1_custom": "custom" }),
        "Framework: (no answer)\nFeatures: custom\nQ3: (no answer)" ; "empty_pick_with_write_in"
    )]
    #[test_case(
        serde_json::json!({ "question_0_custom": "   " }),
        "Framework: (no answer)\nFeatures: (no answer)\nQ3: (no answer)" ; "whitespace_only_write_in"
    )]
    #[test_case(
        serde_json::json!({ "question_1": [" auth ", ""] }),
        "Framework: (no answer)\nFeatures: auth\nQ3: (no answer)" ; "picks_are_trimmed"
    )]
    fn accept_response_folds_write_in_into_the_answer(content: Value, expected: &str) {
        let raw = serde_json::json!({ "action": "accept", "content": content }).to_string();
        assert_eq!(format_response(&questions_input(), &raw), expected);
    }

    #[test]
    fn write_in_is_ignored_when_the_form_is_declined() {
        let raw = serde_json::json!({
            "action": "decline",
            "content": { "question_0_custom": "never sent" }
        })
        .to_string();
        assert_eq!(format_response(&questions_input(), &raw), DISMISSED);
    }

    #[test_case(r#"{"action":"decline"}"# ; "decline")]
    #[test_case(r#"{"action":"cancel"}"# ; "cancel")]
    #[test_case("not json" ; "unparsable")]
    #[test_case("null" ; "jsonrpc_error_forwarded_as_null")]
    fn non_accept_is_dismissed(raw: &str) {
        assert_eq!(format_response(&questions_input(), raw), DISMISSED);
    }

    #[test]
    fn supports_form_requires_form_capability() {
        let with_form = ClientCapabilities::new()
            .elicitation(ElicitationCapabilities::new().form(ElicitationFormCapabilities::new()));
        let url_only = ClientCapabilities::new()
            .elicitation(ElicitationCapabilities::new().url(ElicitationUrlCapabilities::new()));

        assert!(!supports_form(&ClientCapabilities::default()));
        assert!(supports_form(&with_form));
        assert!(!supports_form(&url_only));
    }
}
