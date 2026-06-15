use pathfinder_common::AllowedOrigins;
use tower_http::cors::{AllowOrigin, CorsLayer};

pub fn with_allowed_origins(allowed_origins: AllowedOrigins) -> CorsLayer {
    let allowed_origins = match allowed_origins {
        AllowedOrigins::Any => AllowOrigin::any(),
        AllowedOrigins::List(x) => AllowOrigin::list(x.into_iter().map(|s| {
            http::HeaderValue::from_maybe_shared(s.into_bytes())
                .expect("passed type is 'shared' (i.e. owned byte buffer)")
        })),
    };

    CorsLayer::new()
        .allow_methods([hyper::Method::POST])
        .allow_origin(allowed_origins)
        .allow_headers([hyper::header::CONTENT_TYPE])
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use reqwest::header::HeaderValue;

    use crate::context::RpcContext;
    use crate::{RpcServer, RpcVersion};

    #[tokio::test]
    async fn preflight() {
        for (allowed, expected, line) in [
            // Successful
            (Some("*"), Some("*"), line!()),
            (Some("http://a.com"), Some("http://a.com"), line!()),
            // Not on the list
            (Some("http://b.com"), None, line!()),
            // Disabled
            (None, None, line!()),
        ] {
            let context = RpcContext::for_tests();
            let server = RpcServer::new("127.0.0.1:0".parse().unwrap(), context, RpcVersion::V09);
            let server = match allowed {
                Some(allowed) => server.with_cors(allowed.into()),
                None => server,
            };

            let (_server_handle, address) = server.spawn(&PathBuf::default()).await.unwrap();

            let resp = reqwest::Client::new()
                .request(reqwest::Method::OPTIONS, format!("http://{address}"))
                .header("Access-Control-Request-Headers", "content-type")
                .header("Access-Control-Request-Method", "POST")
                .header("Origin", "http://a.com")
                .body("")
                .send()
                .await
                .unwrap();

            let h = resp.headers();

            assert_eq!(
                h.get("access-control-allow-headers"),
                allowed.and(Some(&HeaderValue::from_static("content-type"))),
                "line: {line}"
            );
            assert_eq!(
                h.get("access-control-allow-methods"),
                allowed.and(Some(&HeaderValue::from_static("POST"))),
                "line: {line}"
            );
            assert_eq!(
                h.get("access-control-allow-origin"),
                expected.map(HeaderValue::from_static).as_ref(),
                "line: {line}"
            );
        }
    }
}
