//! Starknet L2 sequencer client.
use std::fmt::Debug;
use std::result::Result;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use pathfinder_common::class_definition::{
    SerializedCasmDefinition,
    SerializedOpaqueClassDefinition,
};
use pathfinder_common::prelude::*;
use reqwest::Url;
use starknet_gateway_types::error::SequencerError;
use starknet_gateway_types::reply::{
    PreConfirmedPollResponse,
    PreConfirmedPollResponseWire,
    PreLatestBlock,
};
use starknet_gateway_types::trace::{BlockTrace, TransactionTrace};
use starknet_gateway_types::{reply, request};

mod builder;
mod metrics;

/// A way of identifying a specific block.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BlockId {
    Number(BlockNumber),
    Hash(BlockHash),
    Latest,
    Pending,
}

impl From<BlockNumber> for BlockId {
    fn from(block_number: BlockNumber) -> Self {
        BlockId::Number(block_number)
    }
}

impl From<BlockHash> for BlockId {
    fn from(block_hash: BlockHash) -> Self {
        BlockId::Hash(block_hash)
    }
}

impl From<pathfinder_common::BlockId> for BlockId {
    fn from(block_id: pathfinder_common::BlockId) -> Self {
        match block_id {
            pathfinder_common::BlockId::Number(block_number) => BlockId::Number(block_number),
            pathfinder_common::BlockId::Hash(block_hash) => BlockId::Hash(block_hash),
            pathfinder_common::BlockId::Latest => BlockId::Latest,
        }
    }
}

#[allow(unused_variables)]
#[mockall::automock]
#[async_trait::async_trait]
pub trait GatewayApi: Sync {
    async fn pending_block(&self) -> Result<(PreLatestBlock, StateUpdate), SequencerError> {
        unimplemented!();
    }

    async fn preconfirmed_block(
        &self,
        block: BlockId,
        known_block_identifier: Option<String>,
        known_transaction_count: u64,
    ) -> Result<PreConfirmedPollResponse, SequencerError> {
        unimplemented!();
    }

    async fn block_header(
        &self,
        block: BlockId,
    ) -> Result<(BlockNumber, BlockHash), SequencerError> {
        unimplemented!()
    }

    async fn class_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedOpaqueClassDefinition, SequencerError> {
        unimplemented!();
    }

    async fn casm_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedCasmDefinition, SequencerError> {
        unimplemented!();
    }

    async fn transaction_status(
        &self,
        transaction_hash: TransactionHash,
    ) -> Result<reply::TransactionStatus, SequencerError> {
        unimplemented!();
    }

    async fn state_update_with_block(
        &self,
        block: BlockNumber,
    ) -> Result<(reply::Block, StateUpdate, BlockCommitmentSignature), SequencerError> {
        unimplemented!();
    }

    async fn eth_contract_addresses(&self) -> Result<reply::EthContractAddresses, SequencerError> {
        unimplemented!();
    }

    async fn add_invoke_transaction(
        &self,
        invoke: request::add_transaction::InvokeFunction,
    ) -> Result<reply::add_transaction::InvokeResponse, SequencerError> {
        unimplemented!();
    }

    async fn add_declare_transaction(
        &self,
        declare: request::add_transaction::Declare,
        token: Option<String>,
    ) -> Result<reply::add_transaction::DeclareResponse, SequencerError> {
        unimplemented!();
    }

    async fn add_deploy_account(
        &self,
        deploy: request::add_transaction::DeployAccount,
    ) -> Result<reply::add_transaction::DeployAccountResponse, SequencerError> {
        unimplemented!();
    }

    async fn head(&self) -> Result<(BlockNumber, BlockHash), SequencerError> {
        self.block_header(BlockId::Latest).await
    }

    async fn block_traces(&self, block: BlockId) -> Result<BlockTrace, SequencerError> {
        unimplemented!();
    }

    async fn transaction_trace(
        &self,
        transaction: TransactionHash,
    ) -> Result<TransactionTrace, SequencerError> {
        unimplemented!();
    }

    async fn public_key(&self) -> Result<PublicKey, SequencerError> {
        unimplemented!();
    }
}

#[async_trait::async_trait]
impl<T: GatewayApi + Sync + Send> GatewayApi for Arc<T> {
    async fn pending_block(&self) -> Result<(PreLatestBlock, StateUpdate), SequencerError> {
        self.as_ref().pending_block().await
    }

    async fn preconfirmed_block(
        &self,
        block: BlockId,
        known_block_identifier: Option<String>,
        known_transaction_count: u64,
    ) -> Result<PreConfirmedPollResponse, SequencerError> {
        self.as_ref()
            .preconfirmed_block(block, known_block_identifier, known_transaction_count)
            .await
    }

    async fn block_header(
        &self,
        block: BlockId,
    ) -> Result<(BlockNumber, BlockHash), SequencerError> {
        self.as_ref().block_header(block).await
    }

    async fn class_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedOpaqueClassDefinition, SequencerError> {
        self.as_ref().class_by_hash(class_hash, block).await
    }

    async fn casm_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedCasmDefinition, SequencerError> {
        self.as_ref().casm_by_hash(class_hash, block).await
    }

    async fn transaction_status(
        &self,
        transaction_hash: TransactionHash,
    ) -> Result<reply::TransactionStatus, SequencerError> {
        self.as_ref().transaction_status(transaction_hash).await
    }

    async fn state_update_with_block(
        &self,
        block: BlockNumber,
    ) -> Result<(reply::Block, StateUpdate, BlockCommitmentSignature), SequencerError> {
        self.as_ref().state_update_with_block(block).await
    }

    async fn eth_contract_addresses(&self) -> Result<reply::EthContractAddresses, SequencerError> {
        self.as_ref().eth_contract_addresses().await
    }

    async fn add_invoke_transaction(
        &self,
        invoke: request::add_transaction::InvokeFunction,
    ) -> Result<reply::add_transaction::InvokeResponse, SequencerError> {
        self.as_ref().add_invoke_transaction(invoke).await
    }

    async fn add_declare_transaction(
        &self,
        declare: request::add_transaction::Declare,
        token: Option<String>,
    ) -> Result<reply::add_transaction::DeclareResponse, SequencerError> {
        self.as_ref().add_declare_transaction(declare, token).await
    }

    async fn add_deploy_account(
        &self,
        deploy: request::add_transaction::DeployAccount,
    ) -> Result<reply::add_transaction::DeployAccountResponse, SequencerError> {
        self.as_ref().add_deploy_account(deploy).await
    }

    async fn block_traces(&self, block: BlockId) -> Result<BlockTrace, SequencerError> {
        self.as_ref().block_traces(block).await
    }

    async fn transaction_trace(
        &self,
        transaction: TransactionHash,
    ) -> Result<TransactionTrace, SequencerError> {
        self.as_ref().transaction_trace(transaction).await
    }

    async fn public_key(&self) -> Result<PublicKey, SequencerError> {
        self.as_ref().public_key().await
    }
}

/// Starknet sequencer client using REST API.
///
/// Retry is performed on __all__ types of errors __except for__
/// [Starknet specific errors](starknet_gateway_types::error::StarknetError).
///
/// Initial backoff time is 30 seconds and saturates at 10 minutes:
///
/// `backoff [secs] = min((2 ^ N) * 15, 600) [secs]`
///
/// where `N` is the consecutive retry iteration number `{1, 2, ...}`.
#[derive(Debug, Clone)]
pub struct Client {
    /// Shared, replaceable HTTP client. All [Clone]s of this [Client] share the
    /// same instance, so calling [Client::refresh] on any clone replaces the
    /// underlying connection pool for every holder.
    inner: Arc<RwLock<reqwest::Client>>,

    /// Timeout used when constructing the `reqwest` client. Stored so that
    /// [Client::refresh] can re-create the client with the same settings.
    timeout: Duration,

    /// Starknet gateway URL.
    gateway: Url,

    /// Starknet feeder gateway URL.
    feeder_gateway: Url,

    /// Whether __read only__ requests should be retried, defaults to __true__
    /// for production.
    /// Use [disable_retry_for_tests](Client::disable_retry_for_tests) to
    /// disable retry logic for all __read only__ requests when testing.
    retry: bool,

    /// Api key added to each request as a value for 'X-Throttling-Bypass'
    /// header.
    api_key: Option<String>,

    /// Compress requests to the gateway if they contain a non empty proof.
    /// Otherwise do not compress.
    compress_gateway_requests: bool,

    /// Resolved addresses for gateway URL.
    /// Used to detect DNS changes and refresh the HTTP client accordingly.
    resolved_gateway_addresses: Arc<RwLock<Vec<std::net::IpAddr>>>,

    /// Resolved addresses for feeder gateway URL.
    /// Used to detect DNS changes and refresh the HTTP client accordingly.
    resolved_feeder_gateway_addresses: Arc<RwLock<Vec<std::net::IpAddr>>>,
}

impl Client {
    /// Creates a [Client] for [pathfinder_common::Chain::Mainnet].
    pub fn mainnet(timeout: Duration) -> Self {
        Self::with_urls(
            Url::parse("https://alpha-mainnet.starknet.io/gateway").unwrap(),
            Url::parse("https://feeder.alpha-mainnet.starknet.io/feeder_gateway").unwrap(),
            timeout,
        )
        .unwrap()
    }

    /// Creates a [Client] for [pathfinder_common::Chain::SepoliaTestnet].
    pub fn sepolia_testnet(timeout: Duration) -> Self {
        Self::with_urls(
            Url::parse("https://alpha-sepolia.starknet.io/gateway").unwrap(),
            Url::parse("https://feeder.alpha-sepolia.starknet.io/feeder_gateway").unwrap(),
            timeout,
        )
        .unwrap()
    }

    /// Creates a [Client] for [pathfinder_common::Chain::SepoliaIntegration].
    pub fn sepolia_integration(timeout: Duration) -> Self {
        Self::with_urls(
            Url::parse("https://integration-sepolia.starknet.io/gateway").unwrap(),
            Url::parse("https://feeder.integration-sepolia.starknet.io/feeder_gateway").unwrap(),
            timeout,
        )
        .unwrap()
    }

    /// Creates a [Client] with a shared feeder gateway and gateway base url.
    pub fn for_test(base: Url) -> anyhow::Result<Self> {
        let gateway = base.join("gateway")?;
        let feeder_gateway = base.join("feeder_gateway")?;
        Self::with_urls(gateway, feeder_gateway, Duration::from_secs(5))
    }

    /// Create a Sequencer client for the given [Url]s.
    pub fn with_urls(gateway: Url, feeder_gateway: Url, timeout: Duration) -> anyhow::Result<Self> {
        metrics::register();

        let resolved_gateway_addresses =
            resolve_hosts(&gateway).context("Resolving gateway URL")?;
        let resolved_feeder_gateway_addresses =
            resolve_hosts(&feeder_gateway).context("Resolving feeder gateway URL")?;

        Ok(Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(
                reqwest::Client::builder()
                    .timeout(timeout)
                    .user_agent(pathfinder_version::USER_AGENT)
                    .gzip(true)
                    .deflate(true)
                    .build()?,
            )),
            timeout,
            gateway,
            feeder_gateway,
            retry: true,
            api_key: None,
            compress_gateway_requests: true,
            resolved_gateway_addresses: Arc::new(RwLock::new(resolved_gateway_addresses)),
            resolved_feeder_gateway_addresses: Arc::new(RwLock::new(
                resolved_feeder_gateway_addresses,
            )),
        })
    }

    /// Resolve the gateway and feeder gateway URLs and refresh the HTTP client
    /// if the resolved addresses have changed.
    ///
    /// Replaces the underlying `reqwest` HTTP client with a freshly built one,
    /// using the same timeout and settings as the original. Because all
    /// [Clone]s of this [Client] share the same [`std::sync::Arc`], the new
    /// connection pool becomes visible to every holder immediately.
    ///
    /// Intended to be called periodically to enforce reconnection to the
    /// gateway and feeder gateway if their IP addresses change due to DNS
    /// updates, without needing to restart the entire application.
    pub fn refresh(&self) -> anyhow::Result<()> {
        // Resolve the gateway URLs to detect any DNS changes. If the resolved addresses
        // have changed, we refresh the HTTP client to ensure that new connections are
        // made to the correct addresses.
        let new_resolved_gateway_addresses =
            resolve_hosts(&self.gateway).context("Resolving gateway URL")?;
        let new_resolved_feeder_gateway_addresses =
            resolve_hosts(&self.feeder_gateway).context("Resolving feeder gateway URL")?;

        tracing::trace!(
            ?new_resolved_feeder_gateway_addresses,
            ?new_resolved_gateway_addresses,
            "Resolved gateway addresses"
        );

        let mut resolved_addresses_changed = false;

        let mut old_resolved_gateway_addresses = self
            .resolved_gateway_addresses
            .write()
            .expect("gateway client resolved addresses lock is not poisoned");
        if old_resolved_gateway_addresses.as_slice() != new_resolved_gateway_addresses.as_slice() {
            tracing::debug!(old=?old_resolved_gateway_addresses, new=?new_resolved_gateway_addresses, "Gateway URL resolved to new addresses, refreshing HTTP client");
            *old_resolved_gateway_addresses = new_resolved_gateway_addresses;
            resolved_addresses_changed = true;
        }

        let mut old_resolved_feeder_gateway_addresses = self
            .resolved_feeder_gateway_addresses
            .write()
            .expect("gateway client resolved addresses lock is not poisoned");
        if old_resolved_feeder_gateway_addresses.as_slice()
            != new_resolved_feeder_gateway_addresses.as_slice()
        {
            tracing::debug!(old=?old_resolved_feeder_gateway_addresses, new=?new_resolved_feeder_gateway_addresses, "Feeder Gateway URL resolved to new addresses, refreshing HTTP client");
            *old_resolved_feeder_gateway_addresses = new_resolved_feeder_gateway_addresses;
            resolved_addresses_changed = true;
        }

        if resolved_addresses_changed {
            let new_inner = reqwest::Client::builder()
                .timeout(self.timeout)
                .user_agent(pathfinder_version::USER_AGENT)
                .gzip(true)
                .deflate(true)
                .build()?;
            *self
                .inner
                .write()
                .expect("gateway client inner lock is not poisoned") = new_inner;
        }

        Ok(())
    }

    /// Sets the api key to be used for each request as a value for
    /// 'X-Throttling-Bypass' header.
    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key;
        self
    }

    /// Sets whether to compress requests to the gateway if they contain a
    /// non-empty proof.
    pub fn with_compress_gateway_requests(mut self, compress: bool) -> Self {
        self.compress_gateway_requests = compress;
        self
    }

    /// Use this method to disable retry logic for all __non write__ requests
    /// when testing.
    pub fn disable_retry_for_tests(self) -> Self {
        Self {
            retry: false,
            ..self
        }
    }

    fn gateway_request(&self) -> builder::Request<builder::stage::Method> {
        let client = self
            .inner
            .read()
            .expect("gateway client inner lock is not poisoned")
            .clone();
        builder::Request::builder(client, self.gateway.clone(), self.api_key.clone())
    }

    fn feeder_gateway_request(&self) -> builder::Request<builder::stage::Method> {
        let client = self
            .inner
            .read()
            .expect("gateway client inner lock is not poisoned")
            .clone();
        builder::Request::builder(client, self.feeder_gateway.clone(), self.api_key.clone())
    }
}

/// Resolve the URL to addresses.
fn resolve_hosts(url: &Url) -> anyhow::Result<Vec<std::net::IpAddr>> {
    let addresses = url
        .socket_addrs(|| None)
        .context("Resolving host name in URL")?
        .into_iter()
        .map(|socket_addr| socket_addr.ip())
        .collect();
    Ok(addresses)
}

#[async_trait::async_trait]
impl GatewayApi for Client {
    #[tracing::instrument(skip(self))]
    async fn pending_block(&self) -> Result<(PreLatestBlock, StateUpdate), SequencerError> {
        #[derive(Clone, Debug, serde::Deserialize)]
        struct Dto {
            pub block: PreLatestBlock,
            pub state_update: starknet_gateway_types::reply::StateUpdate,
        }

        let result: Dto = self
            .feeder_gateway_request()
            .get_state_update()
            .block(BlockId::Pending)
            .param("includeBlock", "true")
            .retry(self.retry)
            .get()
            .await?;

        Ok((result.block, result.state_update.into()))
    }

    #[tracing::instrument(skip(self))]
    async fn preconfirmed_block(
        &self,
        block: BlockId,
        known_block_identifier: Option<String>,
        known_transaction_count: u64,
    ) -> Result<PreConfirmedPollResponse, SequencerError> {
        // Note that we don't do retries here.
        // The pre-confirmed block is polled continuously by the sync logic,
        // so retries are not needed.
        let id = known_block_identifier.as_deref().unwrap_or("");
        let wire: PreConfirmedPollResponseWire = self
            .feeder_gateway_request()
            .get_preconfirmed_block()
            .block(block)
            .param("blockIdentifier", id)
            .param(
                "knownTransactionCount",
                &known_transaction_count.to_string(),
            )
            .retry(false)
            .get()
            .await?;

        PreConfirmedPollResponse::try_from(wire).map_err(
            |e: starknet_gateway_types::reply::MalformedPreConfirmedResponse| {
                SequencerError::InvalidResponse(e.to_string())
            },
        )
    }

    #[tracing::instrument(skip(self))]
    async fn block_header(
        &self,
        block: BlockId,
    ) -> Result<(BlockNumber, BlockHash), SequencerError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct BlockHeader {
            pub block_hash: BlockHash,
            pub block_number: BlockNumber,
        }

        let header: BlockHeader = self
            .feeder_gateway_request()
            .get_block()
            .block(block)
            .param("headerOnly", "true")
            .retry(self.retry)
            .get()
            .await?;

        Ok((header.block_number, header.block_hash))
    }

    /// Gets class for a particular class hash.
    #[tracing::instrument(skip(self))]
    async fn class_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedOpaqueClassDefinition, SequencerError> {
        let bytes = self
            .feeder_gateway_request()
            .get_class_by_hash()
            .class_hash(class_hash)
            .block(block)
            .retry(self.retry)
            .get_as_bytes()
            .await?;
        Ok(SerializedOpaqueClassDefinition::from_vec(bytes.to_vec()))
    }

    /// Gets CASM for a particular class hash.
    #[tracing::instrument(skip(self))]
    async fn casm_by_hash(
        &self,
        class_hash: ClassHash,
        block: BlockId,
    ) -> Result<SerializedCasmDefinition, SequencerError> {
        let bytes = self
            .feeder_gateway_request()
            .get_compiled_class_by_class_hash()
            .class_hash(class_hash)
            .block(block)
            .retry(self.retry)
            .get_as_bytes()
            .await?;
        Ok(SerializedCasmDefinition::from_vec(bytes.to_vec()))
    }

    /// Gets transaction status by transaction hash.
    #[tracing::instrument(skip(self))]
    async fn transaction_status(
        &self,
        transaction_hash: TransactionHash,
    ) -> Result<reply::TransactionStatus, SequencerError> {
        self.feeder_gateway_request()
            .get_transaction_status()
            .transaction_hash(transaction_hash)
            .retry(self.retry)
            .get()
            .await
    }

    /// Gets a committed block, its state update, and the block commitment
    /// signature in a single request.

    #[tracing::instrument(skip(self))]
    async fn state_update_with_block(
        &self,
        block: BlockNumber,
    ) -> Result<(reply::Block, StateUpdate, BlockCommitmentSignature), SequencerError> {
        #[derive(serde::Deserialize)]
        struct Dto {
            block: reply::Block,
            state_update: reply::StateUpdate,
            signature: [BlockCommitmentSignatureElem; 2],
        }

        let result: Dto = self
            .feeder_gateway_request()
            .get_state_update()
            .block(block)
            .param("includeBlock", "true")
            .param("includeSignature", "true")
            .retry(self.retry)
            .get()
            .await?;

        let signature = BlockCommitmentSignature {
            r: result.signature[0],
            s: result.signature[1],
        };
        Ok((result.block, result.state_update.into(), signature))
    }

    /// Gets addresses of the Ethereum contracts crucial to Starknet operation.
    #[tracing::instrument(skip(self))]
    async fn eth_contract_addresses(&self) -> Result<reply::EthContractAddresses, SequencerError> {
        self.feeder_gateway_request()
            .get_contract_addresses()
            .retry(self.retry)
            .get()
            .await
    }

    /// Adds a transaction invoking a contract.
    #[tracing::instrument(skip(self))]
    async fn add_invoke_transaction(
        &self,
        invoke: request::add_transaction::InvokeFunction,
    ) -> Result<reply::add_transaction::InvokeResponse, SequencerError> {
        // Note that we don't do retries here.
        // This method is used to proxy an add transaction operation from the JSON-RPC
        // API to the sequencer. Retries should be implemented in the JSON-RPC
        // client instead.
        self.gateway_request()
            .add_transaction()
            .retry(false)
            // We only check the proof and ignore the proof_facts field because these proof_facts
            // are a summary derived from the proof itself anyway.
            .compress(self.compress_gateway_requests && !invoke.is_proof_empty())
            .post_with_json(
                &request::add_transaction::AddTransaction::Invoke(invoke),
                Some(Duration::MAX),
            )
            .await
    }

    /// Adds a transaction declaring a class.
    #[tracing::instrument(skip(self))]
    async fn add_declare_transaction(
        &self,
        declare: request::add_transaction::Declare,
        token: Option<String>,
    ) -> Result<reply::add_transaction::DeclareResponse, SequencerError> {
        // Note that we don't do retries here.
        // This method is used to proxy an add transaction operation from the JSON-RPC
        // API to the sequencer. Retries should be implemented in the JSON-RPC
        // client instead.
        self.gateway_request()
            .add_transaction()
            // mainnet requires a token (but testnet does not so its optional).
            .optional_token(token.as_deref())
            .retry(false)
            .post_with_json(
                &request::add_transaction::AddTransaction::Declare(declare),
                Some(Duration::MAX),
            )
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn add_deploy_account(
        &self,
        deploy: request::add_transaction::DeployAccount,
    ) -> Result<reply::add_transaction::DeployAccountResponse, SequencerError> {
        // Note that we don't do retries here.
        // This method is used to proxy an add transaction operation from the JSON-RPC
        // API to the sequencer. Retries should be implemented in the JSON-RPC
        // client instead.
        self.gateway_request()
            .add_transaction()
            .retry(false)
            .post_with_json(
                &request::add_transaction::AddTransaction::DeployAccount(deploy),
                Some(Duration::MAX),
            )
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn block_traces(&self, block: BlockId) -> Result<BlockTrace, SequencerError> {
        self.feeder_gateway_request()
            .get_block_traces()
            .block(block)
            .retry(self.retry)
            .get()
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn transaction_trace(
        &self,
        transaction: TransactionHash,
    ) -> Result<TransactionTrace, SequencerError> {
        self.feeder_gateway_request()
            .get_transaction_trace()
            .transaction_hash(transaction)
            .retry(self.retry)
            .get()
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn public_key(&self) -> Result<PublicKey, SequencerError> {
        self.feeder_gateway_request()
            .get_public_key()
            .retry(self.retry)
            .get()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert_matches::assert_matches;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;
    use pathfinder_crypto::Felt;
    use starknet_gateway_test_fixtures::testnet::*;
    use starknet_gateway_types::error::{test_response_from, KnownStarknetErrorCode};
    use starknet_gateway_types::request::add_transaction::ContractDefinition;
    use tokio::net::TcpListener;
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    use super::*;

    pub async fn bind_ephemeral() -> (SocketAddr, TcpListener) {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get local address");

        (addr, listener)
    }

    fn response_body_from(code: KnownStarknetErrorCode) -> serde_json::Value {
        let (s, _) = test_response_from(code);
        serde_json::from_str(&s).unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn client_user_agent() {
        use std::convert::Infallible;

        use warp::Filter;

        let filter = warp::header::optional("user-agent").and_then(
            |user_agent: Option<String>| async move {
                let user_agent = user_agent.expect("user-agent set");
                let (name, version) = user_agent.split_once('/').unwrap();

                assert_eq!(name, "starknet-pathfinder");
                assert_eq!(version, pathfinder_version::VERSION);

                Ok::<_, Infallible>(warp::reply::json(
                    &serde_json::json!({"block_hash": "0x0", "block_number": 0}),
                ))
            },
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (addr, listener) = bind_ephemeral().await;
        let server = warp::serve(filter).incoming(listener).graceful(async {
            shutdown_rx.await.ok();
        });
        let server_handle = tokio::spawn(server.run());

        let url = format!("http://{addr}");
        let url = Url::parse(&url).unwrap();
        let client = Client::for_test(url).unwrap();

        let _ = client.block_header(BlockId::Latest).await;
        shutdown_tx.send(()).unwrap();
        server_handle.await.unwrap();
    }

    mod transaction {
        use pretty_assertions_sorted::assert_eq;

        use super::reply::Status;
        use super::*;

        #[tokio::test]
        async fn invalid_hash() {
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_transaction_status"))
                .and(matchers::query_param(
                    "transactionHash",
                    INVALID_TX_HASH.0.to_hex_str(),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "tx_status": "NOT_RECEIVED",
                    "finality_status": "NOT_RECEIVED",
                    "execution_status": serde_json::Value::Null,
                })))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
            assert_eq!(
                client
                    .transaction_status(INVALID_TX_HASH)
                    .await
                    .unwrap()
                    .tx_status,
                Status::NotReceived,
            );
        }
    }

    #[tokio::test]
    async fn eth_contract_addresses() {
        let server = MockServer::start().await;
        Mock::given(matchers::path("/feeder_gateway/get_contract_addresses"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
		"FriStatementContract": "0x55d049b4C82807808E76e61a08C6764bbf2ffB55",
		"GpsStatementVerifier": "0x2046B966994Adcb88D83f467a41b75d64C2a619F",
		"MemoryPageFactRegistry": "0x5628E75245Cc69eCA0994F0449F4dDA9FbB5Ec6a",
		"MerkleStatementContract": "0xd414f8f535D4a96cB00fFC8E85160b353cb7809c",
		"Starknet": "0x4737c0c1B4D5b1A687B42610DdabEE781152359c",
		"strk_l2_token_address": "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
		"eth_l2_token_address": "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
            })))
            .mount(&server)
            .await;
        let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
        client.eth_contract_addresses().await.unwrap();
    }

    mod add_transaction {
        use std::collections::HashMap;

        use base64::prelude::*;
        use pathfinder_common::class_definition::{EntryPointType, SelectorAndOffset};
        use pathfinder_common::ContractAddress;
        use starknet_gateway_types::request::add_transaction::CairoContractDefinition;

        use super::*;

        mod invoke {
            use super::*;

            fn inputs() -> (
                TransactionVersion,
                Fee,
                Vec<TransactionSignatureElem>,
                TransactionNonce,
                ContractAddress,
                Vec<CallParam>,
            ) {
                (
                    TransactionVersion::ONE,
                    fee!("0x4F388496839"),
                    vec![
                        transaction_signature_elem!(
                            "0x07dd3a55d94a0de6f3d6c104d7e6c88ec719a82f4e2bbc12587c8c187584d3d5"
                        ),
                        transaction_signature_elem!(
                            "0x071456dded17015d1234779889d78f3e7c763ddcfd2662b19e7843c7542614f8"
                        ),
                    ],
                    transaction_nonce!("0x1"),
                    contract_address!(
                        "0x023371b227eaecd8e8920cd429357edddd2cd0f3fee6abaacca08d3ab82a7cdd"
                    ),
                    vec![
                        call_param!("0x1"),
                        call_param!(
                            "0x0677bb1cdc050e8d63855e8743ab6e09179138def390676cc03c484daf112ba1"
                        ),
                        call_param!(
                            "0x0362398bec32bc0ebb411203221a35a0301193a96f317ebe5e40be9f60d15320"
                        ),
                        CallParam(Felt::ZERO),
                        call_param!("0x1"),
                        call_param!("0x1"),
                        call_param!("0x2b"),
                        CallParam(Felt::ZERO),
                    ],
                )
            }

            #[tokio::test]
            async fn v0_is_deprecated() {
                use request::add_transaction::{InvokeFunction, InvokeFunctionV0V1};

                let server = MockServer::start().await;
                Mock::given(matchers::method("POST"))
                    .and(matchers::path("/gateway/add_transaction"))
                    .respond_with(ResponseTemplate::new(500).set_body_json(response_body_from(
                        KnownStarknetErrorCode::DeprecatedTransaction,
                    )))
                    .mount(&server)
                    .await;
                let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
                let (_, fee, sig, nonce, addr, call) = inputs();
                let invoke = InvokeFunction::V0(InvokeFunctionV0V1 {
                    max_fee: fee,
                    signature: sig,
                    nonce: Some(nonce),
                    sender_address: addr,
                    entry_point_selector: None,
                    calldata: call,
                });

                let error = client.add_invoke_transaction(invoke).await.unwrap_err();
                assert_matches!(
                    error,
                    SequencerError::StarknetError(e) => assert_eq!(e.code, KnownStarknetErrorCode::DeprecatedTransaction.into())
                );
            }

            #[tokio::test]
            async fn successful() {
                use request::add_transaction::{InvokeFunction, InvokeFunctionV0V1};

                let server = MockServer::start().await;
                Mock::given(matchers::method("POST"))
                    .and(matchers::path("/gateway/add_transaction"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "code": "TRANSACTION_RECEIVED",
                        "transaction_hash": "0x0389DD0629F42176CC8B6C43ACEFC0713D0064ECDFC0470E0FC179F53421A38B"
                    })))
                    .mount(&server)
                    .await;
                let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
                // test with values dumped from `starknet invoke` for a test contract
                let (_, fee, sig, nonce, addr, call) = inputs();
                let invoke = InvokeFunction::V1(InvokeFunctionV0V1 {
                    max_fee: fee,
                    signature: sig,
                    nonce: Some(nonce),
                    sender_address: addr,
                    entry_point_selector: None,
                    calldata: call,
                });
                client.add_invoke_transaction(invoke).await.unwrap();
            }
        }

        mod declare {
            use pathfinder_common::class_definition::SelectorAndFunctionIndex;
            use starknet_gateway_types::request::add_transaction::SierraContractDefinition;

            use super::*;

            #[tokio::test]
            async fn v0_is_deprecated() {
                use request::add_transaction::{Declare, DeclareV0V1V2};

                let server = MockServer::start().await;
                Mock::given(matchers::method("POST"))
                    .and(matchers::path("/gateway/add_transaction"))
                    .respond_with(ResponseTemplate::new(500).set_body_json(response_body_from(
                        KnownStarknetErrorCode::DeprecatedTransaction,
                    )))
                    .mount(&server)
                    .await;
                let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

                let declare = Declare::V0(DeclareV0V1V2 {
                    version: TransactionVersion::ZERO,
                    max_fee: Fee(Felt::ZERO),
                    signature: vec![],
                    contract_class: ContractDefinition::Cairo(cairo_contract_class_from_fixture()),
                    sender_address: contract_address!("0x1"),
                    nonce: TransactionNonce::ZERO,
                    compiled_class_hash: None,
                });
                let error = client
                    .add_declare_transaction(declare, None)
                    .await
                    .unwrap_err();
                assert_matches!(
                    error,
                    SequencerError::StarknetError(e) => assert_eq!(e.code, KnownStarknetErrorCode::DeprecatedTransaction.into())
                );
            }

            #[tokio::test]
            async fn successful_v1() {
                use request::add_transaction::{Declare, DeclareV0V1V2};

                let server = MockServer::start().await;
                Mock::given(matchers::method("POST"))
                    .and(matchers::path("/gateway/add_transaction"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "code": "TRANSACTION_RECEIVED",
                        "transaction_hash": "0x77ccba4df42cf0f74a8eb59a96d7880fae371edca5d000ca5f9985652c8a8ed",
                        "class_hash": "0x711941b11a8236b8cca42b664e19342ac7300abb1dc44957763cb65877c2708"
                    })))
                    .mount(&server)
                    .await;
                let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

                let declare = Declare::V1(DeclareV0V1V2 {
                    version: TransactionVersion::ONE,
                    max_fee: fee!("0xFFFF"),
                    signature: vec![],
                    contract_class: ContractDefinition::Cairo(cairo_contract_class_from_fixture()),
                    sender_address: contract_address!("0x1"),
                    nonce: TransactionNonce(Felt::ZERO),
                    compiled_class_hash: None,
                });

                client.add_declare_transaction(declare, None).await.unwrap();
            }

            fn sierra_contract_class_from_fixture() -> SierraContractDefinition {
                let sierra_class =
                    starknet_gateway_test_fixtures::class_definitions::CAIRO_1_0_0_ALPHA6_SIERRA;
                let mut sierra_class =
                    serde_json::from_slice::<serde_json::Value>(sierra_class).unwrap();
                let sierra_program = sierra_class.get_mut("sierra_program").unwrap().take();
                let sierra_program = serde_json::from_value::<Vec<Felt>>(sierra_program).unwrap();
                let mut gzip_encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                serde_json::to_writer(&mut gzip_encoder, &sierra_program).unwrap();
                let sierra_program = gzip_encoder.finish().unwrap();
                let sierra_program = BASE64_STANDARD.encode(sierra_program);

                let mut entry_points = sierra_class.get_mut("entry_points_by_type").unwrap().take();

                let mut entry_points_by_type: HashMap<
                    EntryPointType,
                    Vec<SelectorAndFunctionIndex>,
                > = Default::default();
                entry_points_by_type.insert(
                    EntryPointType::Constructor,
                    serde_json::from_value::<Vec<SelectorAndFunctionIndex>>(
                        entry_points.get_mut("CONSTRUCTOR").unwrap().take(),
                    )
                    .unwrap(),
                );
                entry_points_by_type.insert(
                    EntryPointType::External,
                    serde_json::from_value::<Vec<SelectorAndFunctionIndex>>(
                        entry_points.get_mut("EXTERNAL").unwrap().take(),
                    )
                    .unwrap(),
                );
                entry_points_by_type.insert(
                    EntryPointType::L1Handler,
                    serde_json::from_value::<Vec<SelectorAndFunctionIndex>>(
                        entry_points.get_mut("L1_HANDLER").unwrap().take(),
                    )
                    .unwrap(),
                );

                SierraContractDefinition {
                    sierra_program,
                    contract_class_version: "0.1.0".into(),
                    abi: "trust the contract developer".into(),
                    entry_points_by_type,
                }
            }

            #[tokio::test]
            async fn successful_v2() {
                use request::add_transaction::{Declare, DeclareV0V1V2};

                let server = MockServer::start().await;
                Mock::given(matchers::method("POST"))
                    .and(matchers::path("/gateway/add_transaction"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "code": "TRANSACTION_RECEIVED",
                        "transaction_hash": "0x77ccba4df42cf0f74a8eb59a96d7880fae371edca5d000ca5f9985652c8a8ed",
                        "class_hash": "0x711941b11a8236b8cca42b664e19342ac7300abb1dc44957763cb65877c2708"
                    })))
                    .mount(&server)
                    .await;
                let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

                let declare = Declare::V2(DeclareV0V1V2 {
                    version: TransactionVersion::TWO,
                    max_fee: fee!("0xffff"),
                    signature: vec![],
                    contract_class: ContractDefinition::Sierra(sierra_contract_class_from_fixture()),
                    sender_address: contract_address!("0x1"),
                    nonce: TransactionNonce::ZERO,
                    compiled_class_hash: Some(casm_hash!(
                        "0x5bcd45099caf3dca6c0c0f6697698c90eebf02851acbbaf911186b173472fcc"
                    )),
                });

                client.add_declare_transaction(declare, None).await.unwrap();
            }
        }

        /// Return a contract definition that was dumped from a `starknet
        /// deploy`.
        fn cairo_contract_class_from_fixture() -> CairoContractDefinition {
            let json = starknet_gateway_test_fixtures::class_definitions::CONTRACT_DEFINITION;
            let json: serde_json::Value = serde_json::from_slice(json).unwrap();
            let program = &json["program"];

            // Program is expected to be a gzip-compressed then base64 encoded
            // representation of the JSON.
            let mut gzip_encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            serde_json::to_writer(&mut gzip_encoder, program).unwrap();
            let compressed_program = gzip_encoder.finish().unwrap();
            let program = BASE64_STANDARD.encode(compressed_program);

            let entry_points_by_type: HashMap<EntryPointType, Vec<SelectorAndOffset>> =
                HashMap::from([
                    (EntryPointType::Constructor, vec![]),
                    (
                        EntryPointType::External,
                        vec![
                            SelectorAndOffset {
                                offset: byte_code_offset!("0x3a"),
                                selector: entry_point!("0x0362398bec32bc0ebb411203221a35a0301193a96f317ebe5e40be9f60d15320"),
                            },
                            SelectorAndOffset {
                                offset: byte_code_offset!("0x5b"),
                                selector: entry_point!("0x039e11d48192e4333233c7eb19d10ad67c362bb28580c604d67884c85da39695"),
                            },
                        ],
                    ),
                    (EntryPointType::L1Handler, vec![]),
                ]);
            CairoContractDefinition {
                program,
                entry_points_by_type,
                abi: Some(json["contract_definition"]["abi"].clone()),
            }
        }

        mod deploy_token {
            use std::collections::HashMap;

            use warp::http::{Response, StatusCode};
            use warp::Filter;

            use super::*;

            const EXPECTED_TOKEN: &str = "magic token value";
            const EXPECTED_ERROR_MESSAGE: &str = "error message";

            async fn test_server() -> (tokio::task::JoinHandle<()>, std::net::SocketAddr) {
                fn token_check(params: HashMap<String, String>) -> impl warp::Reply {
                    match params.get("token") {
                        Some(token) if token == EXPECTED_TOKEN => Response::builder().status(StatusCode::OK).body(serde_json::to_vec(&serde_json::json!({
                            "code": "TRANSACTION_ACCEPTED",
                            "transaction_hash": "0x57ed4b4c76a1ca0ba044a654dd3ee2d0d3e550343d739350a22aacdd524110d",
                            "class_hash":"0x3926aea98213ec34fe9783d803237d221c54c52344422e1f4942a5b340fa6ad"
                        })).unwrap()),
                        _ => Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(serde_json::to_vec(&serde_json::json!({
                            "code": "StarknetErrorCode.NON_PERMITTED_CONTRACT",
                            "message": EXPECTED_ERROR_MESSAGE,
                        })).unwrap())
                    }
                }

                let route = warp::any()
                    .and(warp::query::<HashMap<String, String>>())
                    .map(token_check);
                let (addr, listener) = bind_ephemeral().await;
                let server = warp::serve(route).incoming(listener);
                let server_handle = tokio::spawn(server.run());
                (server_handle, addr)
            }

            #[test_log::test(tokio::test)]
            async fn test_token_is_passed_to_sequencer_api() {
                use request::add_transaction::{Declare, DeclareV0V1V2};

                let (_jh, addr) = test_server().await;
                let mut url = reqwest::Url::parse("http://localhost/").unwrap();
                url.set_port(Some(addr.port())).unwrap();
                let client = Client::for_test(url).unwrap();

                let declare = Declare::V0(DeclareV0V1V2 {
                    version: TransactionVersion::ZERO,
                    max_fee: Fee::ZERO,
                    signature: vec![],
                    contract_class: ContractDefinition::Cairo(CairoContractDefinition {
                        program: "".to_owned(),
                        entry_points_by_type: HashMap::new(),
                        abi: None,
                    }),
                    sender_address: ContractAddress::ZERO,
                    nonce: TransactionNonce::ZERO,
                    compiled_class_hash: None,
                });

                client
                    .add_declare_transaction(declare, Some(EXPECTED_TOKEN.to_owned()))
                    .await
                    .unwrap();
            }

            #[test_log::test(tokio::test)]
            async fn test_declare_fails_with_no_token() {
                use request::add_transaction::{Declare, DeclareV0V1V2};

                let (_jh, addr) = test_server().await;
                let mut url = reqwest::Url::parse("http://localhost/").unwrap();
                url.set_port(Some(addr.port())).unwrap();
                let client = Client::for_test(url).unwrap();

                let declare = Declare::V0(DeclareV0V1V2 {
                    version: TransactionVersion::ZERO,
                    max_fee: Fee::ZERO,
                    signature: vec![],
                    contract_class: ContractDefinition::Cairo(CairoContractDefinition {
                        program: "".to_owned(),
                        entry_points_by_type: HashMap::new(),
                        abi: None,
                    }),
                    sender_address: ContractAddress::ZERO,
                    nonce: TransactionNonce::ZERO,
                    compiled_class_hash: None,
                });

                let err = client
                    .add_declare_transaction(declare, None)
                    .await
                    .unwrap_err();

                assert_matches!(err, SequencerError::StarknetError(se) => {
                        assert_eq!(se.code, KnownStarknetErrorCode::NotPermittedContract.into());
                        assert_eq!(se.message, EXPECTED_ERROR_MESSAGE);
                });
            }
        }
    }

    mod block_header {
        use super::*;

        #[test_log::test(tokio::test)]
        async fn success_by_number() {
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_block"))
                .and(matchers::query_param("blockNumber", "9703"))
                .and(matchers::query_param("headerOnly", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "block_hash": "0x6a2755817d86ade81ed0fea2eaf23d94264e2f25aff43ecb2e5000bf3ec28b7",
                    "block_number": 9703
                })))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client
                .block_header(BlockId::Number(BlockNumber::new_or_panic(9703)))
                .await
                .unwrap();
        }

        #[test_log::test(tokio::test)]
        async fn success_by_hash() {
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_block"))
                .and(matchers::query_param("blockHash", "0x6a2755817d86ade81ed0fea2eaf23d94264e2f25aff43ecb2e5000bf3ec28b7"))
                .and(matchers::query_param("headerOnly", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "block_hash": "0x6a2755817d86ade81ed0fea2eaf23d94264e2f25aff43ecb2e5000bf3ec28b7",
                    "block_number": 9703
                })))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client
                .block_header(
                    block_hash!(
                        "0x6a2755817d86ade81ed0fea2eaf23d94264e2f25aff43ecb2e5000bf3ec28b7"
                    )
                    .into(),
                )
                .await
                .unwrap();
        }

        #[test_log::test(tokio::test)]
        async fn block_not_found() {
            const BLOCK_NUMBER: u64 = 99999999;
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_block"))
                .and(matchers::query_param(
                    "blockNumber",
                    BLOCK_NUMBER.to_string(),
                ))
                .and(matchers::query_param("headerOnly", "true"))
                .respond_with(
                    ResponseTemplate::new(500)
                        .set_body_json(response_body_from(KnownStarknetErrorCode::BlockNotFound)),
                )
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
            let error = client
                .block_header(BlockNumber::new_or_panic(BLOCK_NUMBER).into())
                .await
                .unwrap_err();
            assert_matches!(
                error,
                SequencerError::StarknetError(e) => assert_eq!(e.code, KnownStarknetErrorCode::BlockNotFound.into())
            );
        }
    }

    mod pending_block {
        use super::*;

        #[test_log::test(tokio::test)]
        async fn success() {
            let body: serde_json::Value = serde_json::from_str(starknet_gateway_test_fixtures::v0_13_1::state_update_with_block::SEPOLIA_INTEGRATION_PENDING).unwrap();
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param("blockNumber", "pending"))
                .and(matchers::query_param("includeBlock", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client.pending_block().await.unwrap();
        }

        #[test_log::test(tokio::test)]
        async fn block_not_found() {
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param("blockNumber", "pending"))
                .and(matchers::query_param("includeBlock", "true"))
                .respond_with(
                    ResponseTemplate::new(500)
                        .set_body_json(response_body_from(KnownStarknetErrorCode::BlockNotFound)),
                )
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
            let error = client.pending_block().await.unwrap_err();
            assert_matches!(
                error,
                SequencerError::StarknetError(e) => assert_eq!(e.code, KnownStarknetErrorCode::BlockNotFound.into())
            );
        }
    }

    mod state_update_with_block {
        use super::*;

        #[test_log::test(tokio::test)]
        async fn success() {
            let body: serde_json::Value = serde_json::from_str(starknet_gateway_test_fixtures::v0_13_1::state_update_with_block::SEPOLIA_INTEGRATION_NUMBER_9703_WITH_SIGNATURE).unwrap();
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param("blockNumber", "9703"))
                .and(matchers::query_param("includeBlock", "true"))
                .and(matchers::query_param("includeSignature", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client
                .state_update_with_block(BlockNumber::new_or_panic(9703))
                .await
                .unwrap();
        }

        #[test_log::test(tokio::test)]
        async fn success_0_14_1_with_migrated_compiled_classes() {
            let body: serde_json::Value = serde_json::from_str(starknet_gateway_test_fixtures::v0_14_1::state_update_with_block::SEPOLIA_INTEGRATION_3077642_WITH_SIGNATURE).unwrap();
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param("blockNumber", "3077642"))
                .and(matchers::query_param("includeBlock", "true"))
                .and(matchers::query_param("includeSignature", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client
                .state_update_with_block(BlockNumber::new_or_panic(3077642))
                .await
                .unwrap();
        }

        // FIXME: add a proper fixture once `proof_facts` is available on a public
        // chain.
        #[test_log::test(tokio::test)]
        async fn success_0_14_3_with_invoke_proof_facts() {
            let body: serde_json::Value = serde_json::from_str(starknet_gateway_test_fixtures::v0_14_3::state_update_with_block::SEPOLIA_INTEGRATION_FAKE_WITH_SIGNATURE).unwrap();
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param("blockNumber", "3077642"))
                .and(matchers::query_param("includeBlock", "true"))
                .and(matchers::query_param("includeSignature", "true"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();

            client
                .state_update_with_block(BlockNumber::new_or_panic(3077642))
                .await
                .unwrap();
        }

        #[test_log::test(tokio::test)]
        async fn block_not_found() {
            const BLOCK_NUMBER: u64 = 99999999;
            let server = MockServer::start().await;
            Mock::given(matchers::path("/feeder_gateway/get_state_update"))
                .and(matchers::query_param(
                    "blockNumber",
                    BLOCK_NUMBER.to_string(),
                ))
                .and(matchers::query_param("includeBlock", "true"))
                .and(matchers::query_param("includeSignature", "true"))
                .respond_with(
                    ResponseTemplate::new(500)
                        .set_body_json(response_body_from(KnownStarknetErrorCode::BlockNotFound)),
                )
                .mount(&server)
                .await;
            let client = Client::for_test(server.uri().parse().unwrap()).unwrap();
            let error = client
                .state_update_with_block(BlockNumber::new_or_panic(BLOCK_NUMBER))
                .await
                .unwrap_err();
            assert_matches!(
                error,
                SequencerError::StarknetError(e) => assert_eq!(e.code, KnownStarknetErrorCode::BlockNotFound.into())
            );
        }
    }

    mod gzip_compression {
        use warp::Filter;

        use super::*;

        #[test_log::test(tokio::test)]
        async fn handles_gzip_compressed_responses() {
            // Expected response values
            const EXPECTED_BLOCK_NUMBER: u64 = 9703;
            const EXPECTED_BLOCK_HASH: &str =
                "0x6a2755817d86ade81ed0fea2eaf23d94264e2f25aff43ecb2e5000bf3ec28b7";

            // Create the JSON response that will be gzip-compressed by the server
            let response_json = serde_json::json!({
                "block_hash": EXPECTED_BLOCK_HASH,
                "block_number": EXPECTED_BLOCK_NUMBER
            });

            // Create a mock server that returns gzip-compressed responses
            // warp's gzip filter will automatically compress the response when the client
            // sends Accept-Encoding: gzip (which reqwest does by default)
            let server_filter = warp::path("feeder_gateway")
                .and(warp::path("get_block"))
                .and(warp::query::<std::collections::HashMap<String, String>>())
                .map(move |_| warp::reply::json(&response_json))
                .with(warp::filters::compression::gzip());

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let (server_addr, listener) = bind_ephemeral().await;
            let server = warp::serve(server_filter)
                .incoming(listener)
                .graceful(async {
                    shutdown_rx.await.ok();
                });
            let server_handle = tokio::spawn(server.run());

            let server_url = Url::parse(&format!("http://{server_addr}")).unwrap();
            let client = Client::for_test(server_url)
                .unwrap()
                .disable_retry_for_tests();

            // Make a request (reqwest should automatically decompress the gzip response)
            let (actual_block_number, actual_block_hash) = client
                .block_header(BlockId::Number(BlockNumber::new_or_panic(
                    EXPECTED_BLOCK_NUMBER,
                )))
                .await
                .unwrap();

            // Verify the response was correctly decompressed and parsed
            assert_eq!(
                actual_block_number,
                BlockNumber::new_or_panic(EXPECTED_BLOCK_NUMBER)
            );
            assert_eq!(actual_block_hash, block_hash!(EXPECTED_BLOCK_HASH));

            shutdown_tx.send(()).unwrap();
            server_handle.await.unwrap();
        }
    }
}
