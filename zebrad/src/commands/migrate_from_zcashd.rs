//! `migrate-from-zcashd` subcommand - creates a new Zebra state from an existing `zcashd`
//! datadir
//!
//! ## Command Structure
//!
//! Migrating `zcashd` state uses the following services and tasks:
//!
//! Tasks:
//!  * `zcashd` to Zebra Migrate Task
//!    * reads blocks from the `zcashd` state,
//!      copies those blocks to the target state, then
//!      reads the copied blocks from the target state.
//!
//! Services:
//!  * Target New State Service
//!    * writes best finalized chain blocks to permanent storage,
//!      in the new format
//!    * only performs essential contextual verification of blocks,
//!      to make sure that block data hasn't been corrupted by
//!      receiving blocks in the new format
//!    * fetches blocks from the best finalized chain from permanent storage,
//!      in the new format

use std::{
    collections::BTreeMap,
    future::Future,
    io::SeekFrom,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use abscissa_core::{config, Command, FrameworkError};
use color_eyre::eyre::{eyre, Report};
use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::oneshot,
    time::{timeout, Duration, Instant},
};
use tower::{buffer::Buffer, util::BoxService, Service, ServiceBuilder, ServiceExt};

use zebra_chain::{
    block::{Block, Hash, Height},
    chain_tip::ChainTip,
    parameters::{
        checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP,
        {Magic, Network},
    },
    serialization::ZcashDeserialize,
};
use zebra_node_services::mempool;
use zebra_state::LatestChainTip;

use crate::{
    components::tokio::{RuntimeRun, TokioComponent},
    config::ZebradConfig,
    prelude::*,
    BoxError,
};

/// How often we log info-level progress messages
const PROGRESS_HEIGHT_INTERVAL: u32 = 5_000;

/// The maximum number of unprocessed messages to buffer for
/// the state service when migrating from zcashd.
const STATE_BUFFER_BOUND: usize = 100;

/// The maximum total size, in bytes, of blocks submitted to zebrad but not
/// yet verified and committed (including their response futures and
/// dedup-tracking entries).
///
/// `zcashd` writes blocks to its `blk*.dat` files in arrival order, so the
/// files contain re-downloaded stretches, orphaned fork candidates, and
/// exact duplicates. The migration submits every distinct block above the
/// committed tip to the verifier (which queues candidates per height and
/// picks the ones on the checkpoint chain), and tracks each block's size
/// until its response future resolves.
///
/// This bound is the migration's primary RAM limit: when it is exceeded,
/// the migration waits for verification to make progress before reading
/// more blocks. Verification always makes progress while the files contain
/// the main chain, so waiting can't deadlock; if the files are missing a
/// needed main-chain block, the wait ends with a "missing block" error at
/// [`VERIFY_PROGRESS_TIMEOUT`] instead of hanging or OOMing.
const MAX_INFLIGHT_BYTES: u64 = 512 * 1024 * 1024;

/// How long to wait for the verifier to make progress when the in-flight
/// byte budget is exhausted, before failing with a "missing block" error.
///
/// Only the last incomplete checkpoint range's futures stay unresolved for
/// a while, so a healthy migration always sees progress well within this
/// timeout when it is reading data the verifier can use.
const VERIFY_PROGRESS_TIMEOUT: Duration = Duration::from_secs(60);

/// How long to wait for the last pending block verification to resolve,
/// after the last block has been submitted and all other futures are done.
///
/// If no future resolves for this long, the remaining blocks are in a
/// checkpoint range that never verified (e.g. the migration was stopped
/// below the next checkpoint). Their futures legitimately never resolve,
/// so we give up on them.
const LAST_BLOCK_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The block verifier router service type returned by
/// [`zebra_consensus::router::init`], for annotating helper functions.
type BlockVerifierRouterService = Buffer<
    BoxService<zebra_consensus::Request, Hash, zebra_consensus::RouterError>,
    zebra_consensus::Request,
>;

/// Creates a new Zebra state from an existing `zcashd` datadir
#[derive(Command, Debug, clap::Parser)]
pub struct MigrateFromZcashdCmd {
    /// `zcashd` datadir from which to migrate state.
    #[clap(long)]
    datadir: PathBuf,

    /// Source height that the migration finishes at.
    #[clap(long, short, help = "stop copying at this source height")]
    max_source_height: Option<u32>,

    /// Filter strings which override the config file and defaults
    #[clap(help = "tracing filters which override the zebrad.toml config")]
    filters: Vec<String>,
}

impl MigrateFromZcashdCmd {
    /// Configure and launch the migrate command
    async fn start(&self) -> Result<(), Report> {
        let app_config = APPLICATION.config();

        self.migrate(
            app_config.network.network.clone(),
            app_config.state.clone(),
            app_config.consensus.clone(),
        )
        .await
        .map_err(|e| eyre!(e))
    }

    /// Initialize the target state, then copy from the `zcashd` datadir to the target
    /// state.
    async fn migrate(
        &self,
        network: Network,
        target_state_config: zebra_state::Config,
        target_consensus_config: zebra_consensus::Config,
    ) -> Result<(), BoxError> {
        info!(
            ?target_state_config,
            ?target_consensus_config,
            "initializing target state service"
        );

        let target_start_time = Instant::now();
        // We're not verifying UTXOs here, so we don't need the maximum checkpoint height.
        //
        // TODO: call Options::PrepareForBulkLoad()
        // See "What's the fastest way to load data into RocksDB?" in
        // https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
        let (
            target_state_service,
            _target_read_only_state_service,
            target_latest_chain_tip,
            _target_chain_tip_change,
        ) = zebra_state::init(target_state_config.clone(), &network, Height::MAX, 0).await;

        let target_state = ServiceBuilder::new()
            .buffer(STATE_BUFFER_BOUND)
            .service(target_state_service);
        let (
            mut block_verifier_router,
            _tx_verifier,
            _consensus_task_handles,
            _max_checkpoint_height,
        ) = zebra_consensus::router::init(
            target_consensus_config,
            &network,
            target_state,
            oneshot::channel::<
                Buffer<BoxService<mempool::Request, mempool::Response, BoxError>, mempool::Request>,
            >()
            .1,
        )
        .await;

        let elapsed = target_start_time.elapsed();
        info!(?elapsed, "finished initializing target state service");

        info!("fetching Zebra tip height");

        let initial_target_tip = target_latest_chain_tip.best_tip_height();
        let min_target_height = initial_target_tip
            .map(|Height(target_tip)| target_tip + 1)
            .unwrap_or(0);

        let max_copy_height = self.max_source_height;

        let mut zcashd_blocks = ZcashdBlocks::open_at_start(network, self.datadir.clone()).await?;

        info!(
            ?min_target_height,
            ?max_copy_height,
            max_source_height = ?self.max_source_height,
            ?initial_target_tip,
            "starting migration from zcashd to Zebra"
        );

        let copy_start_time = Instant::now();

        // Every distinct block read from the zcashd files is submitted to the
        // verifier immediately. The verifier decides which blocks are on the
        // main chain: it queues candidate blocks at each height (up to a few
        // per height) and picks the ones that match the checkpoint list.
        //
        // Submitting everything is necessary because zcashd block files are
        // in arrival order, which is not height order. In particular, the
        // main-chain block at a height can appear *after* orphaned fork
        // blocks at that same height (e.g. after a netsplit heals), so no
        // first-observed block at a height can be assumed to be final.
        //
        // Blocks are only skipped when they are *certain* to be useless:
        //  - below the committed tip (already finalized or permanently
        //    rejected), or
        //  - an exact duplicate of an already-submitted block.
        let mut pending = FuturesUnordered::<PendingBlockCommit>::new();

        // Hashes of already-submitted blocks, for dropping exact duplicates
        // (zcashd re-appends blocks during re-downloads). Entries are pruned
        // once the committed tip passes their height.
        let mut submitted_hashes: BTreeMap<Height, Vec<Hash>> = BTreeMap::new();

        // Total size of blocks submitted but not yet verified and committed.
        // Decremented as their futures resolve.
        let mut inflight_bytes: u64 = 0;

        while let Some((source_block, source_block_size)) = zcashd_blocks.read_next_block().await? {
            let source_block_hash = source_block.hash();
            let height = source_block.coinbase_height().ok_or_else(|| {
                eyre!("zcashd stored invalid block {source_block_hash} with no coinbase height")
            })?;
            trace!(?height, %source_block, "read zcashd block");

            if let Some(max_height) = max_copy_height {
                if height.0 > max_height {
                    break;
                }
            }

            // Blocks at or below the committed tip were already finalized or permanently
            // rejected, so they can't affect the chain. This could occur in zcashd if a
            // competing historic chain were received and then orphaned.
            //
            // If no block has been committed yet (the tip is `None`), nothing can be
            // skipped: the genesis block must be submitted for the checkpoint verifier
            // to make progress (its first checkpoint range starts at genesis).
            let committed_tip_height = target_latest_chain_tip.best_tip_height().map(|Height(h)| h);
            if committed_tip_height.is_some_and(|tip| height.0 <= tip) {
                trace!(
                    ?height,
                    ?source_block_hash,
                    "skipping committed zcashd block"
                );
                continue;
            }

            // Drop exact duplicates; distinct blocks at the same height (fork candidates)
            // are all submitted for verification.
            let hashes_at_height = submitted_hashes.entry(height).or_default();
            if hashes_at_height.contains(&source_block_hash) {
                trace!(
                    ?height,
                    ?source_block_hash,
                    "skipping duplicate zcashd block"
                );
                continue;
            }
            hashes_at_height.push(source_block_hash);

            // Prune dedup entries that the committed tip has moved past, so the map stays
            // bounded. (The tip only moves upward.)
            if let Some(tip) = committed_tip_height {
                while let Some(&lowest_height) = submitted_hashes.keys().next() {
                    if lowest_height.0 <= tip {
                        submitted_hashes.remove(&lowest_height);
                    } else {
                        break;
                    }
                }
            }

            // Provide backpressure: wait until verification has made enough progress that
            // the in-flight data fits the budget.
            //
            // If verification makes no progress for `VERIFY_PROGRESS_TIMEOUT`, the
            // checkpoint verifier is stuck waiting for a block that is *further ahead*
            // in the zcashd files (they are in arrival order). Lookahead-scan the rest
            // of the files, submitting only blocks in the stuck window; the reader is
            // then rewound to resume the migration normally. Only if the scan reaches
            // the end of the files without unsticking verification is the block proven
            // to be missing.
            while inflight_bytes > MAX_INFLIGHT_BYTES {
                match timeout(VERIFY_PROGRESS_TIMEOUT, pending.next()).await {
                    Ok(Some((_height, size, result))) => {
                        inflight_bytes = inflight_bytes.saturating_sub(size);
                        warn_if_block_commit_failed(result);
                    }
                    Ok(None) => unreachable!("pending is not empty when inflight_bytes > 0"),
                    Err(_elapsed) => {
                        let stuck_tip_height =
                            target_latest_chain_tip.best_tip_height().map(|Height(h)| h);
                        info!(
                            ?stuck_tip_height,
                            ?inflight_bytes,
                            "verification stalled: looking ahead for the missing zcashd blocks"
                        );

                        // Rewind point: just after the block that triggered the wait.
                        let rewind_pos = zcashd_blocks.save_position();

                        let scanned_past_tip = lookahead_scan(
                            &mut zcashd_blocks,
                            &mut block_verifier_router,
                            &mut pending,
                            &mut submitted_hashes,
                            &mut inflight_bytes,
                            &target_latest_chain_tip,
                            stuck_tip_height,
                            max_copy_height,
                        )
                        .await?;

                        if scanned_past_tip {
                            // The lookahead scan reached EOF without unsticking
                            // verification: the needed blocks are genuinely absent
                            // from the datadir.
                            return Err(eyre!(
                                "no verification progress for {VERIFY_PROGRESS_TIMEOUT:?}: \
                                 verification is stuck waiting for a block that continues the \
                                 chain at the committed tip {stuck_tip_height:?}. The lookahead \
                                 scan reached the end of the zcashd block files without \
                                 finding it, so the datadir is missing that block (or the \
                                 checkpoint verifier rejected every candidate for it). Run \
                                 zcashd to complete its block index, or migrate the missing \
                                 blocks from a peer-synced zebrad"
                            )
                            .into());
                        }

                        // The scan unstuck verification: rewind and resume
                        // migrating from where this wait started. Blocks that
                        // the scan skipped are re-read and dedup-skipped
                        // (cheap), or committed-skipped once the tip passes
                        // them.
                        info!("lookahead unstuck verification: resuming normal migration");
                        zcashd_blocks.rewind_to(rewind_pos).await?;
                    }
                }
            }

            // Submit the block to zebrad.
            //
            // # Correctness
            //
            // The response future must not be dropped until it resolves. Dropping it
            // cancels the request while it is queued in the router's `Buffer`, causing
            // the block to never reach the checkpoint verifier's block queue and creating
            // a permanent gap. The checkpoint verifier can't verify past a gap, so the
            // migration would stall, and the queued blocks would accumulate until zebrad
            // crashes on OOM.
            //
            // Each future resolves to (height, block size, result) so the in-flight
            // byte count can be decremented accurately, and unresolved blocks
            // can be reported by height at the end of the migration.
            let size = source_block_size as u64;
            inflight_bytes += size;
            let rsp = block_verifier_router
                .ready()
                .await?
                .call(zebra_consensus::Request::Commit(source_block));
            pending.push(Box::pin(async move { (height, size, rsp.await) }));

            // Opportunistically drain resolved futures, to keep `inflight_bytes` accurate
            // and surface errors early. This is best-effort: the futures resolve in
            // verifier tasks, so more may complete at any time.
            while let Some(Some((_height, size, result))) = pending.next().now_or_never() {
                inflight_bytes = inflight_bytes.saturating_sub(size);
                warn_if_block_commit_failed(result);
            }

            // Log progress
            if height.0 % PROGRESS_HEIGHT_INTERVAL == 0 {
                let elapsed = copy_start_time.elapsed();
                info!(
                    ?height,
                    ?max_copy_height,
                    ?elapsed,
                    "copied block from zcashd to Zebra"
                );
            }
        }

        // Wait for the last submitted blocks to finish verifying and committing.
        //
        // The last checkpoint range is usually incomplete, so its futures
        // legitimately never resolve. Wait for progress, but give up when
        // nothing has resolved for `LAST_BLOCK_COMMIT_TIMEOUT`.
        info!(
            ?max_copy_height,
            "waiting for pending block commits to finish"
        );
        let drain_start_time = Instant::now();
        let mut abandoned_blocks: BTreeMap<Height, u64> = BTreeMap::new();
        while !pending.is_empty() {
            // `BlockVerifierRouter` requires a timeout because zcashd might have recorded
            // out-of-order or invalid blocks, and `zebra_consensus` will leave their
            // responses pending.
            match timeout(LAST_BLOCK_COMMIT_TIMEOUT, pending.next()).await {
                Ok(Some((_height, _size, result))) => {
                    warn_if_block_commit_failed(result);
                }
                Ok(None) => unreachable!("pending is not empty"),
                Err(_elapsed) => {
                    // Count the remaining futures by height: they are the
                    // blocks in the final incomplete checkpoint range (plus
                    // any fork candidates at those heights).
                    while let Some(Some((height, _size, _result))) = pending.next().now_or_never() {
                        *abandoned_blocks.entry(height).or_default() += 1;
                    }
                    if let (Some(first), Some(last)) = (
                        abandoned_blocks.keys().next(),
                        abandoned_blocks.keys().next_back(),
                    ) {
                        let abandoned_count: u64 = abandoned_blocks.values().sum();
                        info!(
                            ?first,
                            ?last,
                            ?abandoned_count,
                            idle_timeout = ?LAST_BLOCK_COMMIT_TIMEOUT,
                            "giving up waiting for the last block commits: \
                             the final checkpoint range is incomplete, \
                             so these blocks will never verify"
                        );
                    }
                    break;
                }
            }
        }
        info!(
            drain_elapsed = ?drain_start_time.elapsed(),
            "finished waiting for block commits"
        );

        let elapsed = copy_start_time.elapsed();
        let final_tip = target_latest_chain_tip.best_tip_height();
        let initial_tip = initial_target_tip;
        let migrated_height = match (initial_tip, final_tip) {
            (Some(Height(start)), Some(Height(end))) => end - start,
            (None, Some(Height(end))) => end + 1,
            _ => 0,
        };
        info!(
            ?initial_tip,
            ?final_tip,
            migrated_height,
            ?max_copy_height,
            ?elapsed,
            "finished migrating blocks"
        );

        Ok(())
    }
}

/// Log a warning if a block verification or commit failed.
///
/// `zcashd` stores side-chain and stale blocks in its `blk*.dat` files,
/// alongside the best chain. Those blocks fail checkpoint verification or
/// contextual validation in Zebra, which is expected during a migration,
/// so their errors are logged and skipped.
///
/// More serious failures (e.g. corrupt chain data) surface as the migration
/// making no progress, so they are also logged rather than fatal here.
fn warn_if_block_commit_failed(result: Result<Hash, BoxError>) {
    match result {
        Ok(_hash) => {}
        Err(error) => warn!(?error, "zcashd block failed verification"),
    }
}

/// The type of the futures in the migration's `pending` set. Each future
/// resolves to the block's height, serialized size, and the verifier's result,
/// so the in-flight byte count can be decremented accurately and unresolved
/// blocks can be reported by height.
type PendingBlockCommit =
    Pin<Box<dyn Future<Output = (Height, u64, Result<Hash, BoxError>)> + Send>>;

/// Scan ahead in the zcashd block files while verification is stuck, submitting
/// only blocks that verification can use: distinct blocks in the window
/// (stuck tip, stuck tip + `MAX_CHECKPOINT_HEIGHT_GAP`]. All other blocks are
/// read and dropped, because the normal migration re-reads them after the
/// rewind.
///
/// Returns `Ok(true)` if the scan reached EOF without verification becoming
/// unstuck, i.e. the needed blocks are proven missing. Returns `Ok(false)`
/// when the committed tip advanced past the stuck tip, in which case the
/// caller rewinds the reader and resumes normal migration.
///
/// # Correctness
///
/// - The scan must not buffer skipped blocks: the whole point is to look far
///   ahead without RAM cost. Only submitted blocks count against the caller's
///   `inflight_bytes` budget, and each submitted block's future is kept alive
///   until it resolves, so no request is ever cancelled mid-flight.
/// - Rescued blocks are recorded in `submitted_hashes`, so after the rewind
///   the normal migration dedup-skips them instead of re-submitting.
/// - The scan is bounded by `max_copy_height` like the main loop, so a bounded
///   migration still ends at the requested height.
// The scan shares the main loop's state (reader, verifier, pending futures,
// dedup map, and byte budget), so its parameters mirror the loop's locals.
#[allow(clippy::too_many_arguments)]
async fn lookahead_scan(
    zcashd_blocks: &mut ZcashdBlocks,
    block_verifier_router: &mut BlockVerifierRouterService,
    pending: &mut FuturesUnordered<PendingBlockCommit>,
    submitted_hashes: &mut BTreeMap<Height, Vec<Hash>>,
    inflight_bytes: &mut u64,
    latest_chain_tip: &LatestChainTip,
    stuck_tip_height: Option<u32>,
    max_copy_height: Option<u32>,
) -> Result<bool, BoxError> {
    // The stuck tip is where verification waits for the next block. If
    // nothing is committed yet, verification is stuck at genesis: rescue the
    // whole first checkpoint window.
    let stuck_tip = stuck_tip_height.map_or(0, |tip| tip + 1);
    let rescue_end = stuck_tip + u32::try_from(MAX_CHECKPOINT_HEIGHT_GAP).expect("fits in u32");

    let mut rescued_block_count = 0u64;

    while let Some((source_block, source_block_size)) = zcashd_blocks.read_next_block().await? {
        // Check whether verification unstuck while we were reading: the
        // committed tip advances as the stuck range completes and commits.
        while let Some(Some((_height, size, result))) = pending.next().now_or_never() {
            *inflight_bytes = inflight_bytes.saturating_sub(size);
            warn_if_block_commit_failed(result);
        }
        let is_unstuck = match stuck_tip_height {
            // Any committed block unsticks a tip that was absent (the
            // verifier was stuck waiting for genesis).
            None => latest_chain_tip.best_tip_height().is_some(),
            Some(stuck_tip) => latest_chain_tip
                .best_tip_height()
                .is_some_and(|Height(tip)| tip > stuck_tip),
        };
        if is_unstuck {
            info!(?rescued_block_count, "lookahead unstuck verification");
            return Ok(false);
        }

        let height = match source_block.coinbase_height() {
            Some(height) => height,
            None => continue,
        };

        if let Some(max_height) = max_copy_height {
            if height.0 > max_height {
                // The migration's configured end is before EOF: not "missing",
                // but the scan can't find rescue blocks past it either.
                return Ok(true);
            }
        }

        // Only rescue blocks in the stuck window; skip everything else.
        if height.0 < stuck_tip || height.0 > rescue_end {
            continue;
        }

        let source_block_hash = source_block.hash();
        let hashes_at_height = submitted_hashes.entry(height).or_default();
        if hashes_at_height.contains(&source_block_hash) {
            continue;
        }
        hashes_at_height.push(source_block_hash);

        let size = source_block_size as u64;
        *inflight_bytes += size;
        let rsp = block_verifier_router
            .ready()
            .await?
            .call(zebra_consensus::Request::Commit(source_block));
        pending.push(Box::pin(async move { (height, size, rsp.await) }));
        rescued_block_count += 1;
    }

    // Reached EOF (or max_copy_height) without unsticking verification.
    Ok(true)
}

impl Runnable for MigrateFromZcashdCmd {
    /// Start the application.
    fn run(&self) {
        info!(
            max_source_height = ?self.max_source_height,
            "starting zcashd data migration"
        );
        let rt = APPLICATION
            .state()
            .components_mut()
            .get_downcast_mut::<TokioComponent>()
            .expect("TokioComponent should be available")
            .rt
            .take();

        rt.expect("runtime should not already be taken")
            .run(self.start());

        info!("finished zcashd data migration");
    }
}

impl config::Override<ZebradConfig> for MigrateFromZcashdCmd {
    // Process the given command line options, overriding settings from
    // a configuration file using explicit flags taken from command-line
    // arguments.
    fn override_config(&self, mut config: ZebradConfig) -> Result<ZebradConfig, FrameworkError> {
        if !self.filters.is_empty() {
            config.tracing.filter = Some(self.filters.join(","));
        }

        Ok(config)
    }
}

struct ZcashdBlocks {
    network: Network,
    datadir: PathBuf,
    block_file: ZcashdBlockFile,
}

/// A saved reader position, for rewinding after a lookahead scan.
///
/// This is the (file number, byte offset) of the next unread record, tracked
/// by the reader itself (each record consumes exactly `8 + size` bytes, and
/// skipped zero-size records consume 8). It is deliberately not derived from
/// `BufReader`'s stream position, whose buffered-byte accounting must not be
/// relied on across a rewind.
#[derive(Clone, Debug)]
struct ZcashdBlocksPosition {
    file_number: usize,
    offset: u64,
}

impl ZcashdBlocks {
    async fn open_at_start(network: Network, datadir: PathBuf) -> Result<Self, BoxError> {
        let block_file = ZcashdBlockFile::open(&datadir, 0).await?;

        Ok(Self {
            network,
            datadir,
            block_file,
        })
    }

    /// Returns the current read position as a rewind point.
    ///
    /// Only valid at a record boundary: i.e. just after a successful
    /// `read_next_block` returned a block.
    fn save_position(&self) -> ZcashdBlocksPosition {
        ZcashdBlocksPosition {
            file_number: self.block_file.file_number,
            offset: self.block_file.offset,
        }
    }

    /// Rewinds to a previously saved position.
    async fn rewind_to(&mut self, pos: ZcashdBlocksPosition) -> Result<(), BoxError> {
        if self.block_file.file_number != pos.file_number {
            self.block_file = ZcashdBlockFile::open(&self.datadir, pos.file_number).await?;
        }
        self.block_file.rewind_to_offset(pos.offset).await
    }

    async fn read_next_block(&mut self) -> Result<Option<(Arc<Block>, usize)>, BoxError> {
        loop {
            match self.block_file.read_next_block().await? {
                Some((magic, block, size)) => {
                    break if magic == self.network.magic() {
                        Ok(Some((Arc::new(block), size)))
                    } else {
                        Err(eyre!("zcashd block is for a different network ({magic:?})").into())
                    }
                }
                None => {
                    match ZcashdBlockFile::open(&self.datadir, self.block_file.file_number + 1)
                        .await
                    {
                        // If the next file does not exist, we are done reading blocks.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => break Ok(None),
                        Err(e) => break Err(e.into()),
                        Ok(block_file) => self.block_file = block_file,
                    };
                }
            }
        }
    }
}

struct ZcashdBlockFile {
    file_number: usize,
    reader: BufReader<File>,
    block_buf: Vec<u8>,
    /// Byte offset of the next unread record in this file.
    ///
    /// Tracked by the reader rather than derived from the stream position, so
    /// that a saved position survives the buffered reader's internal state.
    offset: u64,
    /// Number of consecutive zero-size junk records skipped without reading
    /// a valid block. Used to log one summary line per junk stretch instead
    /// of one warn per record.
    consecutive_zero_records: usize,
}

impl ZcashdBlockFile {
    async fn open(datadir: &Path, file_number: usize) -> Result<Self, std::io::Error> {
        let path = datadir
            .join("blocks")
            .join(format!("blk{:05}.dat", file_number));
        info!(?path, "opening zcashd block file");
        let reader = BufReader::new(File::open(path).await?);

        Ok(Self {
            file_number,
            reader,
            block_buf: vec![],
            offset: 0,
            consecutive_zero_records: 0,
        })
    }

    /// Rewinds this file so the next record read starts at `offset`.
    async fn rewind_to_offset(&mut self, offset: u64) -> Result<(), BoxError> {
        self.reader.seek(SeekFrom::Start(offset)).await?;
        self.offset = offset;
        Ok(())
    }

    /// Reads the next block record from this file.
    ///
    /// Returns `Ok(None)` when the file ends: either at a clean EOF, at a
    /// partial trailing record, or after skipping junk records.
    ///
    /// # Junk records
    ///
    /// `zcashd` records each block's position in its block index database,
    /// not in these files. If it was interrupted while writing a block, the
    /// file can contain junk where a record should be, without any marker of
    /// where the valid records resume. The reader must therefore be robust:
    ///
    ///  - a **zero-size record** (8 bytes of zeros) is skipped, and reading
    ///    continues from the next byte. This happens when a crash left an
    ///    all-zeros gap in the file, as observed in real datadirs.
    ///  - a **partial trailing record** (fewer bytes than the record header
    ///    promises, including a truncated data section) ends the file.
    ///    `zcashd`'s index would not have recorded a block for it, so the
    ///    remaining bytes are junk.
    ///  - a **record larger than the protocol maximum block size** is
    ///    treated as junk that ends the file, rather than attempting to
    ///    buffer it.
    async fn read_next_block(&mut self) -> Result<Option<(Magic, Block, usize)>, BoxError> {
        loop {
            let mut magic = Magic([0; 4]);
            match self.reader.read_exact(&mut magic.0).await {
                // If there aren't enough bytes to read the magic, assume we reached the end
                // of this block file.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.log_skipped_zero_records();
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
                Ok(_) => {
                    // Read the rest of the encoded block from the file; if it does not error
                    // then the reader is left in a consistent state for subsequent reads.
                    let size = self.reader.read_u32_le().await?;

                    // A zero-size record is junk left by an interrupted
                    // write: skip it and read the next record. (The loop
                    // avoids unbounded recursion through junk stretches.)
                    if size == 0 {
                        self.consecutive_zero_records += 1;
                        self.offset += 8;
                        continue;
                    }

                    // A record bigger than the protocol maximum is junk:
                    // assume the valid records end here, so the reader stays
                    // in sync.
                    if size as u64 > zebra_chain::serialization::MAX_PROTOCOL_MESSAGE_LEN as u64 {
                        self.log_skipped_zero_records();
                        warn!(
                            ?size,
                            "zcashd block record is larger than the maximum block size, \
                             ending this file"
                        );
                        return Ok(None);
                    }

                    self.block_buf.resize(size as usize, 0);
                    // If the record is truncated, the valid records end here.
                    match self.reader.read_exact(&mut self.block_buf).await {
                        Ok(_bytes_read) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            self.log_skipped_zero_records();
                            warn!(?size, "zcashd block record is truncated, ending this file");
                            return Ok(None);
                        }
                        Err(e) => return Err(e.into()),
                    }

                    // Track the offset of the next record, for rewinding.
                    self.offset += 8 + size as u64;

                    // Now attempt to parse the block bytes.
                    let block = match Block::zcash_deserialize(self.block_buf.as_slice()) {
                        Ok(block) => block,
                        Err(e) => {
                            self.log_skipped_zero_records();
                            return Err(e.into());
                        }
                    };
                    self.log_skipped_zero_records();
                    return Ok(Some((magic, block, size as usize)));
                }
            }
        }
    }

    /// Log a summary of any skipped zero-size junk records, then reset the count.
    fn log_skipped_zero_records(&mut self) {
        if self.consecutive_zero_records > 0 {
            warn!(
                zero_records = self.consecutive_zero_records,
                file_number = ?self.file_number,
                "skipped zero-size zcashd block records left by an interrupted write"
            );
            self.consecutive_zero_records = 0;
        }
    }
}
