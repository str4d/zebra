//! State storage code for Zebra. 🦓
//!
//! ## Organizational Structure
//!
//! zebra-state tracks `Blocks` using two key-value trees
//!
//! * BlockHeaderHash -> Block
//! * BlockHeight -> Block
//!
//! Inserting a block into the service will create a mapping in each tree for that block.

#![doc(html_favicon_url = "https://www.zfnd.org/images/zebra-favicon-128.png")]
#![doc(html_logo_url = "https://www.zfnd.org/images/zebra-icon.png")]
#![doc(html_root_url = "https://doc.zebra.zfnd.org/zebra_state")]
#![warn(missing_docs)]
#![allow(clippy::try_err)]

use color_eyre::eyre::{eyre, Report};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::{error, iter, sync::Arc};
use tower::{Service, ServiceExt};

use zebra_chain::{
    block::{Block, BlockHeaderHash},
    types::BlockHeight,
    Network,
    Network::*,
};

pub mod in_memory;
pub mod on_disk;

/// Configuration for the state service.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The root directory for storing cached data.
    ///
    /// Each network has a separate state, which is stored in "mainnet/state"
    /// and "testnet/state" subdirectories.
    pub cache_dir: Option<PathBuf>,
}

impl Config {
    /// Generate the appropriate `sled::Config` for `network`, based on the
    /// provided `zebra_state::Config`.
    ///
    /// # Details
    ///
    /// This function should panic if the user of `zebra-state` doesn't configure
    /// a directory to store the state.
    pub(crate) fn sled_config(&self, network: Network) -> sled::Config {
        let net_dir = match network {
            Mainnet => "mainnet",
            Testnet => "testnet",
        };
        let path = self
            .cache_dir
            .as_ref()
            .unwrap_or_else(|| {
                todo!("create a nice user facing error explaining how to set the cache directory in zebrad.toml:\n[state]\ncache_dir = '/path/to/cache-or-tmp'")
            })
            .join(net_dir)
            .join("state");

        sled::Config::default().path(path)
    }
}

impl Default for Config {
    fn default() -> Self {
        let cache_dir = std::env::var("ZEBRAD_CACHE_DIR")
            .map(PathBuf::from)
            .ok()
            .or_else(|| dirs::cache_dir().map(|dir| dir.join("zebra")));

        Self { cache_dir }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// A state request, used to manipulate the zebra-state on disk or in memory
pub enum Request {
    // TODO(jlusby): deprecate in the future based on our validation story
    /// Add a block to the zebra-state
    AddBlock {
        /// The block to be added to the state
        block: Arc<Block>,
    },
    /// Get a block from the zebra-state
    GetBlock {
        /// The hash used to identify the block
        hash: BlockHeaderHash,
    },
    /// Get a block locator list for the current best chain
    GetBlockLocator {
        /// The genesis block of the current best chain
        genesis: BlockHeaderHash,
    },
    /// Get the block that is the tip of the current chain
    GetTip,
    /// Ask the state if the given hash is part of the current best chain
    GetDepth {
        /// The hash to check against the current chain
        hash: BlockHeaderHash,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// A state response
pub enum Response {
    /// The response to a `AddBlock` request indicating a block was successfully
    /// added to the state
    Added {
        /// The hash of the block that was added
        hash: BlockHeaderHash,
    },
    /// The response to a `GetBlock` request by hash
    Block {
        /// The block that was requested
        block: Arc<Block>,
    },
    /// The response to a `GetBlockLocator` request
    BlockLocator {
        /// The set of blocks that make up the block locator
        block_locator: Vec<BlockHeaderHash>,
    },
    /// The response to a `GetTip` request
    Tip {
        /// The hash of the block at the tip of the current chain
        hash: BlockHeaderHash,
    },
    /// The response to a `Contains` request indicating that the given has is in
    /// the current best chain
    Depth(
        /// The number of blocks above the given block in the current best chain
        Option<u32>,
    ),
}

/// Get the heights of the blocks for constructing a block_locator list
fn block_locator_heights(tip_height: BlockHeight) -> impl Iterator<Item = BlockHeight> {
    iter::successors(Some(1u32), |h| h.checked_mul(2))
        .flat_map(move |step| tip_height.0.checked_sub(step))
        .map(BlockHeight)
        .chain(iter::once(BlockHeight(0)))
}

/// The error type for the State Service.
// TODO(jlusby): Error = Report ?
type Error = Box<dyn error::Error + Send + Sync + 'static>;

/// Get the tip block, using `state`.
///
/// If there is no tip, returns `Ok(None)`.
/// Returns an error if `state.poll_ready` errors.
pub async fn initial_tip<S>(state: S) -> Result<Option<Arc<Block>>, Report>
where
    S: Service<Request, Response = Response, Error = Error> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    let initial_tip_hash = state
        .clone()
        .ready_and()
        .await
        .map_err(|e| eyre!(e))?
        .call(Request::GetTip)
        .await
        .map(|response| match response {
            Response::Tip { hash } => hash,
            _ => unreachable!("GetTip request can only result in Response::Tip"),
        })
        .ok();

    let initial_tip_block = match initial_tip_hash {
        Some(hash) => state
            .clone()
            .ready_and()
            .await
            .map_err(|e| eyre!(e))?
            .call(Request::GetBlock { hash })
            .await
            .map(|response| match response {
                Response::Block { block } => block,
                _ => unreachable!("GetBlock request can only result in Response::Block"),
            })
            .ok(),
        None => None,
    };

    Ok(initial_tip_block)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::OsStr;

    #[test]
    fn test_path_mainnet() {
        test_path(Mainnet);
    }

    #[test]
    fn test_path_testnet() {
        test_path(Testnet);
    }

    /// Check the sled path for `network`.
    fn test_path(network: Network) {
        zebra_test::init();

        let config = Config::default();
        // we can't do many useful tests on this value, because it depends on the
        // local environment and OS.
        let sled_config = config.sled_config(network);
        let mut path = sled_config.get_path();
        assert_eq!(path.file_name(), Some(OsStr::new("state")));
        assert!(path.pop());
        match network {
            Mainnet => assert_eq!(path.file_name(), Some(OsStr::new("mainnet"))),
            Testnet => assert_eq!(path.file_name(), Some(OsStr::new("testnet"))),
        }
    }

    /// Check what happens when the config is invalid.
    #[test]
    #[should_panic]
    fn test_no_path() {
        // We don't call `zebra_test::init` here, to silence the expected panic log
        // TODO:
        //  - implement test log levels in #760
        //  - call `zebra_test::init`
        //  - disable all log output from this test
        let bad_config = Config { cache_dir: None };
        let _unreachable = bad_config.sled_config(Mainnet);
    }
}
