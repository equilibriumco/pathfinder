#[cfg(feature = "p2p")]
pub(super) mod cli;
#[cfg(feature = "p2p")]
mod config;

#[cfg(feature = "p2p")]
pub use cli::P2PConsensusCli;
#[cfg(feature = "p2p")]
pub use config::{P2PConsensusConfig, P2PSyncConfig};

#[cfg(not(feature = "p2p"))]
#[derive(Clone)]
pub struct P2PSyncConfig;

#[cfg(not(feature = "p2p"))]
#[derive(Clone)]
pub struct P2PConsensusConfig;

#[cfg(not(feature = "p2p"))]
impl P2PSyncConfig {
    pub(super) fn parse_or_exit(_: ()) -> Self {
        Self
    }
}

#[cfg(not(feature = "p2p"))]
impl P2PConsensusConfig {
    pub(super) fn parse_or_exit(_: ()) -> Self {
        Self
    }
}
