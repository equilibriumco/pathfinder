use std::collections::HashMap;

use goose::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::requests::v09::*;

/// Script from Starkware that contain some heavy-weight calls mixed with
/// wallet-like calls.
const MAINNET_SCRIPT: &str = include_str!("mainnet_script.txt");
/// A slight variation of `MAINNET_SCRIPT` that doesn't contain the heavy-weight
/// calls.
const MAINNET_SCRIPT_WITHOUT_HUGE_CALLS: &str =
    include_str!("mainnet_script_without_huge_calls.txt");

pub async fn mainnet_scripted(user: &mut GooseUser) -> TransactionResult {
    scripted(user, MAINNET_SCRIPT).await
}

pub async fn mainnet_scripted_without_huge_calls(user: &mut GooseUser) -> TransactionResult {
    scripted(user, MAINNET_SCRIPT_WITHOUT_HUGE_CALLS).await
}

async fn scripted(user: &mut GooseUser, script: &'static str) -> TransactionResult {
    let mut time_taken = HashMap::new();

    for (i, request) in script.lines().enumerate() {
        let line_number = i + 1;
        let request = request.trim_matches([' ', '\t', '\'']);
        if request.is_empty() {
            continue;
        }

        let t_start = std::time::Instant::now();

        let result = post_request::<serde_json::Value>(user, request).await?;

        let elapsed = t_start.elapsed();
        time_taken.insert(i, elapsed);

        match result {
            RpcResult::Result(_result) => {}
            RpcResult::Error(error) => {
                println!(
                    "Request failed: {}, line_number: {}, request: {}",
                    error.message, line_number, request
                );
            }
        }
    }

    let (max_i, max_time) = time_taken
        .iter()
        .max_by_key(|(_, elapsed)| *elapsed)
        .unwrap();
    println!(
        "Slowest request: line_number: {}, elapsed: {:?}",
        max_i + 1,
        max_time
    );

    let (min_i, min_time) = time_taken
        .iter()
        .min_by_key(|(_, elapsed)| *elapsed)
        .unwrap();
    println!(
        "Fastest request: line_number: {}, elapsed: {:?}",
        min_i + 1,
        min_time
    );

    Ok(())
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RpcResponse<T> {
    id: serde_json::Value,
    jsonrpc: String,
    #[serde(flatten)]
    result: RpcResult<T>,
}

#[derive(Deserialize)]
enum RpcResult<T> {
    #[serde(rename = "result")]
    Result(T),
    #[serde(rename = "error")]
    Error(RpcError),
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

async fn post_request<T: DeserializeOwned>(
    user: &mut GooseUser,
    request: &'static str,
) -> MethodResult<RpcResult<T>> {
    let request_builder = user
        .get_request_builder(&GooseMethod::Post, "/rpc/v0_9")?
        .header("Content-Type", "application/json")
        .body(request);
    let goose_request = GooseRequest::builder()
        .set_request_builder(request_builder)
        .expect_status_code(200)
        .build();
    let response = user
        .request(goose_request)
        .await?
        .response
        .map_err(|e| Box::new(e.into()))?;

    let response: RpcResponse<T> = response.json().await.map_err(|e| Box::new(e.into()))?;

    Ok(response.result)
}
