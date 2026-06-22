use std::borrow::Cow;

use serde_json::{json, Value};

use crate::RpcVersion;

#[derive(Debug)]
pub enum RpcError {
    ParseError(String),
    InvalidRequest(String),
    MethodNotFound,
    InvalidParams(String),
    InternalError(anyhow::Error),
    ApplicationError(crate::error::ApplicationError),
    WebsocketSubscriptionClosed {
        subscription_id: u32,
        reason: String,
    },
}

impl PartialEq for RpcError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InternalError(l0), Self::InternalError(r0)) => l0.to_string() == r0.to_string(),
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl RpcError {
    pub fn code(&self, version: RpcVersion) -> i32 {
        // From the json-rpc specification: https://www.jsonrpc.org/specification#error_object
        match self {
            RpcError::ParseError(..) => -32700,
            RpcError::InvalidRequest(..) => -32600,
            RpcError::MethodNotFound => -32601,
            RpcError::InvalidParams(..) => -32602,
            RpcError::InternalError(_) => -32603,
            RpcError::ApplicationError(err) => err.code(version),
            RpcError::WebsocketSubscriptionClosed { .. } => -32099,
        }
    }

    /// Stable, low-cardinality label for the `error_kind` metric dimension.
    pub fn metric_label(&self) -> &'static str {
        match self {
            RpcError::ParseError(_) => "parse_error",
            RpcError::InvalidRequest(_) => "invalid_request",
            RpcError::MethodNotFound => "method_not_found",
            RpcError::InvalidParams(_) => "invalid_params",
            RpcError::InternalError(_) => "internal_error",
            RpcError::ApplicationError(e) => e.metric_label(),
            RpcError::WebsocketSubscriptionClosed { .. } => "ws_subscription_closed",
        }
    }

    pub fn message(&self, version: RpcVersion) -> Cow<'_, str> {
        match self {
            RpcError::ParseError(..) => "Parse error".into(),
            RpcError::InvalidRequest(..) => "Invalid request".into(),
            RpcError::MethodNotFound => "Method not found".into(),
            RpcError::InvalidParams(..) => "Invalid params".into(),
            RpcError::InternalError(_) => "Internal error".into(),
            RpcError::ApplicationError(e) => e.message(version).into(),
            RpcError::WebsocketSubscriptionClosed { .. } => "Websocket subscription closed".into(),
        }
    }

    pub fn data(&self, version: RpcVersion) -> Option<Value> {
        match self {
            RpcError::WebsocketSubscriptionClosed {
                subscription_id,
                reason,
            } => Some(json!({
                "id": subscription_id,
                "reason": reason,
            })),
            RpcError::ApplicationError(e) => e.data(version),
            RpcError::InternalError(_) => None,
            RpcError::MethodNotFound => None,
            RpcError::ParseError(e) | RpcError::InvalidRequest(e) | RpcError::InvalidParams(e) => {
                Some(json!({
                    "reason": e
                }))
            }
        }
    }
}

impl crate::dto::SerializeForVersion for RpcError {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut obj = serializer.serialize_struct()?;
        obj.serialize_field("code", &self.code(serializer.version))?;
        obj.serialize_field("message", &self.message(serializer.version).as_ref())?;

        if let Some(data) = self.data(serializer.version) {
            obj.serialize_field("data", &data)?;
        }

        obj.end()
    }
}

impl<E> From<E> for RpcError
where
    E: Into<crate::error::ApplicationError>,
{
    fn from(value: E) -> Self {
        Self::ApplicationError(value.into())
    }
}

impl From<pathfinder_storage::StorageError> for RpcError {
    fn from(value: pathfinder_storage::StorageError) -> Self {
        Self::InternalError(value.into())
    }
}

#[cfg(test)]
mod metric_label_tests {
    use super::*;
    use crate::error::ApplicationError;

    #[test]
    fn rpc_error_categories() {
        assert_eq!(
            RpcError::ParseError("x".into()).metric_label(),
            "parse_error"
        );
        assert_eq!(
            RpcError::InvalidRequest("x".into()).metric_label(),
            "invalid_request"
        );
        assert_eq!(RpcError::MethodNotFound.metric_label(), "method_not_found");
        assert_eq!(
            RpcError::InvalidParams("x".into()).metric_label(),
            "invalid_params"
        );
        assert_eq!(
            RpcError::InternalError(anyhow::anyhow!("x")).metric_label(),
            "internal_error"
        );
        assert_eq!(
            RpcError::WebsocketSubscriptionClosed {
                subscription_id: 1,
                reason: "x".into()
            }
            .metric_label(),
            "ws_subscription_closed"
        );
    }

    #[test]
    fn application_error_uses_variant_name() {
        assert_eq!(
            RpcError::ApplicationError(ApplicationError::BlockNotFound).metric_label(),
            "block_not_found"
        );
        assert_eq!(
            RpcError::ApplicationError(ApplicationError::ContractNotFound).metric_label(),
            "contract_not_found"
        );
    }
}
