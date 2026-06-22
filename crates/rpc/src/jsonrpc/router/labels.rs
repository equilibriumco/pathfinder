//! Cross-cutting metric labels derived from a request at the router boundary.

use serde_json::value::RawValue;

/// How a request targeted a block, derived from the shape of its `block_id`
/// parameter. Used as the `block_target` metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockTarget {
    Latest,
    Pending,
    ByNumber,
    ByHash,
    /// No `block_id` parameter: a method that takes none, or an empty
    /// positional argument list.
    None,
    /// A non-empty positional argument list, or an unrecognised `block_id`
    /// shape.
    Unknown,
}

impl BlockTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockTarget::Latest => "latest",
            BlockTarget::Pending => "pending",
            BlockTarget::ByNumber => "by_number",
            BlockTarget::ByHash => "by_hash",
            BlockTarget::None => "none",
            BlockTarget::Unknown => "unknown",
        }
    }
}

/// Classify the `block_id` of a request from its raw params, without depending
/// on a version-specific deserializer — keys off the stable JSON shape only.
pub fn classify_block_target(params: Option<&RawValue>) -> BlockTarget {
    let Some(raw) = params else {
        return BlockTarget::None;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw.get()) else {
        return BlockTarget::Unknown;
    };
    match value {
        // Positional params: an empty list carries no block_id; a non-empty
        // list can't be classified by field name.
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                BlockTarget::None
            } else {
                BlockTarget::Unknown
            }
        }
        // Named params: classify by the shape of the `block_id` field.
        serde_json::Value::Object(obj) => match obj.get("block_id") {
            None => BlockTarget::None,
            Some(serde_json::Value::String(s)) if s == "latest" => BlockTarget::Latest,
            Some(serde_json::Value::String(s)) if s == "pending" => BlockTarget::Pending,
            Some(serde_json::Value::Object(o)) if o.contains_key("block_number") => {
                BlockTarget::ByNumber
            }
            Some(serde_json::Value::Object(o)) if o.contains_key("block_hash") => {
                BlockTarget::ByHash
            }
            Some(_) => BlockTarget::Unknown,
        },
        _ => BlockTarget::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::value::RawValue;

    use super::*;

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    #[test]
    fn classifies_block_id_shapes() {
        let cases = [
            (r#"{"block_id":"latest"}"#, BlockTarget::Latest),
            (r#"{"block_id":"pending"}"#, BlockTarget::Pending),
            (r#"{"block_id":{"block_number":5}}"#, BlockTarget::ByNumber),
            (r#"{"block_id":{"block_hash":"0x1"}}"#, BlockTarget::ByHash),
            (r#"{"contract_address":"0x1"}"#, BlockTarget::None),
            (r#"{"block_id":"l1_accepted"}"#, BlockTarget::Unknown),
        ];
        for (json, expected) in cases {
            let r = raw(json);
            assert_eq!(classify_block_target(Some(&r)), expected, "json: {json}");
        }
    }

    #[test]
    fn absent_or_empty_params_are_none() {
        assert_eq!(classify_block_target(None), BlockTarget::None);
        let empty = raw("[]");
        assert_eq!(classify_block_target(Some(&empty)), BlockTarget::None);
    }

    #[test]
    fn non_empty_positional_params_are_unknown() {
        let r = raw(r#"["latest","0x1"]"#);
        assert_eq!(classify_block_target(Some(&r)), BlockTarget::Unknown);
    }
}
