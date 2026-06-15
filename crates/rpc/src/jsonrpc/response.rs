use axum::response::IntoResponse;
use serde_json::Value;

use crate::dto::SerializeForVersion;
use crate::error::ApplicationError;
use crate::jsonrpc::error::RpcError;
use crate::jsonrpc::RequestId;
use crate::RpcVersion;

#[derive(Debug, PartialEq)]
pub struct RpcResponse {
    pub output: RpcResult,
    pub id: RequestId,
    pub version: RpcVersion,
}

impl RpcResponse {
    pub const fn parse_error(error: String, version: RpcVersion) -> RpcResponse {
        Self {
            output: Err(RpcError::ParseError(error)),
            id: RequestId::Null,
            version,
        }
    }

    pub const fn invalid_request(error: String, version: RpcVersion) -> RpcResponse {
        Self {
            output: Err(RpcError::InvalidRequest(error)),
            id: RequestId::Null,
            version,
        }
    }

    pub const fn method_not_found(id: RequestId, version: RpcVersion) -> RpcResponse {
        Self {
            output: Err(RpcError::MethodNotFound),
            id,
            version,
        }
    }

    pub const fn invalid_params(id: RequestId, error: String, version: RpcVersion) -> RpcResponse {
        Self {
            output: Err(RpcError::InvalidParams(error)),
            id,
            version,
        }
    }

    pub fn internal_error(id: RequestId, error: String, version: RpcVersion) -> RpcResponse {
        Self {
            output: Err(RpcError::InternalError(anyhow::Error::msg(error))),
            id,
            version,
        }
    }
}

pub type RpcResult = Result<Value, RpcError>;

impl crate::dto::SerializeForVersion for RpcResponse {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut obj = serializer.serialize_struct()?;
        obj.serialize_field("jsonrpc", &"2.0")?;

        match &self.output {
            Ok(x) => obj.serialize_field("result", &x)?,
            Err(e) => obj.serialize_field("error", e)?,
        };

        match &self.id {
            RequestId::Number(x) => obj.serialize_field("id", x)?,
            RequestId::String(x) => obj.serialize_field("id", &x)?,
            RequestId::Null => obj.serialize_field("id", &Value::Null)?,
            RequestId::Notification => {}
        };

        obj.end()
    }
}

impl crate::dto::SerializeForVersion for &RpcResponse {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut obj = serializer.serialize_struct()?;
        obj.serialize_field("jsonrpc", &"2.0")?;

        match &self.output {
            Ok(x) => obj.serialize_field("result", &x)?,
            Err(e) => obj.serialize_field("error", e)?,
        };

        match &self.id {
            RequestId::Number(x) => obj.serialize_field("id", x)?,
            RequestId::String(x) => obj.serialize_field("id", &x)?,
            RequestId::Null => obj.serialize_field("id", &Value::Null)?,
            RequestId::Notification => {}
        };

        obj.end()
    }
}

impl IntoResponse for RpcResponse {
    fn into_response(self) -> axum::response::Response {
        // Log internal errors.
        match &self.output {
            Err(RpcError::InternalError(e))
            | Err(RpcError::ApplicationError(ApplicationError::Internal(e))) => {
                tracing::warn!(backtrace = ?e, "Internal error");
            }
            Err(RpcError::ApplicationError(ApplicationError::Custom(e))) => {
                tracing::debug!(backtrace = ?e, "Custom error");
            }
            _ => {}
        }

        let mut response = serde_json::to_vec(
            &self
                .serialize(crate::dto::Serializer::new(self.version))
                .unwrap(),
        )
        .unwrap()
        .into_response();
        if let Err(RpcError::ApplicationError(ApplicationError::ForwardedError(error))) =
            self.output
        {
            if let Some(status) = error.status() {
                *response.status_mut() = status;
            } else {
                tracing::warn!(?error, "Forwarded error has no status");
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn output_is_error() {
        let parsing_err = serde_json::from_str::<u32>("invalid")
            .unwrap_err()
            .to_string();
        let response = RpcResponse {
            output: Err(RpcError::InvalidParams(parsing_err.clone())),
            id: RequestId::Number(1),
            version: RpcVersion::V09,
        };
        let parsing_err = RpcError::InvalidParams(parsing_err);

        let serialized = response
            .serialize(crate::dto::Serializer::new(RpcVersion::V09))
            .unwrap();

        let expected = json!({
            "jsonrpc": "2.0",
            "error": {
                "code": parsing_err.code(RpcVersion::V09),
                "message": parsing_err.message(RpcVersion::V09),
                "data": parsing_err.data(RpcVersion::V09),
            },
            "id": 1,
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn output_is_ok() {
        let serialized = RpcResponse {
            output: Ok(Value::String("foobar".to_owned())),
            id: RequestId::Number(1),
            version: RpcVersion::V09,
        }
        .serialize(crate::dto::Serializer::new(RpcVersion::V09))
        .unwrap();

        let expected = json!({
            "jsonrpc": "2.0",
            "result": "foobar",
            "id": 1,
        });

        assert_eq!(serialized, expected);
    }
}
