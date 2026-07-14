//! This test was separated because the `metrics` crate uses a singleton
//! recorder, so keeping a test that relies on metric values in a separate
//! binary makes more sense than using an inter-test locking mechanism which can
//! cause weird test failures without any obvious clue to what might have caused
//! those failures in the first place.

use std::sync::{Arc, Mutex};

use futures::stream::StreamExt;
use pathfinder_common::BlockNumber;
use pretty_assertions_sorted::assert_eq;
use starknet_gateway_client::{BlockId, Client, GatewayApi};
use starknet_gateway_types::error::{test_response_from, KnownStarknetErrorCode};
use wiremock::{matchers, Mock, MockServer, Request, Respond, ResponseTemplate};

struct VariedResponse {
    counter: Arc<Mutex<usize>>,
    responses: Vec<(String, u16)>,
}

impl VariedResponse {
    pub fn new(responses: Vec<(String, u16)>) -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
            responses,
        }
    }
}

impl Respond for VariedResponse {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let mut counter = self.counter.lock().unwrap();
        if *counter < self.responses.len() {
            let rsp_def = &self.responses[*counter];
            *counter += 1;
            ResponseTemplate::new(rsp_def.1).set_body_string(rsp_def.0.clone())
        } else {
            panic!("{} responses already exhausted", self.responses.len());
        }
    }
}

#[tokio::test]
async fn all_counter_types_including_tags() {
    use pathfinder_common::test_utils::metrics::FakeRecorder;

    let method_name = "get_block";
    let method_call = |client: Client, x| async move {
        let _ = client.block_header(x).await;
    };

    let recorder = FakeRecorder::new_for(&["get_block"]);
    let handle = recorder.handle();

    // Automatically deregister the recorder
    let _guard = metrics::set_default_local_recorder(&recorder);

    let responses = vec![
        // Any valid fixture
        (r#"{"block_hash": "0x7d328a71faf48c5c3857e99f20a77b18522480956d1cd5bff1ff2df3c8b427b", "block_number": 0}"#.to_owned(), 200),
        // 1 Starknet error
        test_response_from(KnownStarknetErrorCode::BlockNotFound, ""),
        // 2 decode errors
        (r#"{"not":"valid"}"#.to_owned(), 200),
        (r#"{"not":"valid, again"}"#.to_owned(), 200),
        // 3 of rate limiting
        ("you're being rate limited".to_owned(), 429),
        ("".to_owned(), 429),
        ("".to_owned(), 429),
    ];

    let server = MockServer::start().await;
    Mock::given(matchers::path("/feeder_gateway/get_block"))
        .and(matchers::query_param("blockNumber", "123"))
        .and(matchers::query_param("headerOnly", "true"))
        .respond_with(VariedResponse::new(responses.clone()))
        .mount(&server)
        .await;
    Mock::given(matchers::path("/feeder_gateway/get_block"))
        .and(matchers::query_param("blockNumber", "latest"))
        .and(matchers::query_param("headerOnly", "true"))
        .respond_with(VariedResponse::new(responses.clone()))
        .mount(&server)
        .await;
    Mock::given(matchers::path("/feeder_gateway/get_block"))
        .and(matchers::query_param("blockNumber", "pending"))
        .and(matchers::query_param("headerOnly", "true"))
        .respond_with(VariedResponse::new(responses))
        .mount(&server)
        .await;
    let client = Client::for_test(server.uri().parse().unwrap())
        .unwrap()
        .disable_retry_for_tests();

    [BlockId::Number(BlockNumber::new_or_panic(123)); 7]
        .into_iter()
        .chain([BlockId::Latest; 7].into_iter())
        .chain([BlockId::Pending; 7].into_iter())
        .map(|x| method_call(client.clone(), x))
        .collect::<futures::stream::FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    // IMPORTANT
    //
    // We're not using any crate::sequencer::metrics consts here, because this is
    // public API and we'd like to catch if/when it changed (apparently due to a
    // bug)
    [
        ("gateway_requests_total", None, None, 21),
        ("gateway_requests_total", Some("latest"), None, 7),
        ("gateway_requests_total", Some("pending"), None, 7),
        ("gateway_requests_failed_total", None, None, 18),
        ("gateway_requests_failed_total", Some("latest"), None, 6),
        ("gateway_requests_failed_total", Some("pending"), None, 6),
        ("gateway_requests_failed_total", None, Some("starknet"), 3),
        (
            "gateway_requests_failed_total",
            Some("latest"),
            Some("starknet"),
            1,
        ),
        (
            "gateway_requests_failed_total",
            Some("pending"),
            Some("starknet"),
            1,
        ),
        ("gateway_requests_failed_total", None, Some("decode"), 6),
        (
            "gateway_requests_failed_total",
            Some("latest"),
            Some("decode"),
            2,
        ),
        (
            "gateway_requests_failed_total",
            Some("pending"),
            Some("decode"),
            2,
        ),
        (
            "gateway_requests_failed_total",
            None,
            Some("rate_limiting"),
            9,
        ),
        (
            "gateway_requests_failed_total",
            Some("latest"),
            Some("rate_limiting"),
            3,
        ),
        (
            "gateway_requests_failed_total",
            Some("pending"),
            Some("rate_limiting"),
            3,
        ),
    ]
    .into_iter()
    .for_each(
        |(counter_name, tag, failure_reason, expected_count)| match (tag, failure_reason) {
            (None, None) => assert_eq!(
                handle.get_counter_value(counter_name, method_name),
                expected_count,
                "counter: {counter_name}, method: {method_name}"
            ),
            (None, Some(reason)) => assert_eq!(
                handle.get_counter_value_by_label(
                    counter_name,
                    [("method", method_name), ("reason", reason)]
                ),
                expected_count,
                "counter: {counter_name}, method: {method_name}, reason: {reason}"
            ),
            (Some(tag), None) => assert_eq!(
                handle.get_counter_value_by_label(
                    counter_name,
                    [("method", method_name), ("tag", tag)]
                ),
                expected_count,
                "counter: {counter_name}, method: {method_name}, tag: {tag}"
            ),
            (Some(tag), Some(reason)) => assert_eq!(
                handle.get_counter_value_by_label(
                    counter_name,
                    [("method", method_name), ("tag", tag), ("reason", reason)]
                ),
                expected_count,
                "counter: {counter_name}, method: {method_name}, tag: {tag}, reason: {reason}"
            ),
        },
    );
}
