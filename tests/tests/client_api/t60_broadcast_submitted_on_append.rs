use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft_memstore::BlockOperation;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::RaftRouter;
use crate::fixtures::ut_harness;

/// With `broadcast_submitted_on_append`, the leader replicates an entry as soon as its own
/// `append` returns, even while `RaftCore` is still appending later writes in the same loop
/// iteration.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn entry_is_replicated_before_the_loop_iteration_ends() -> Result<()> {
    let replicated_early = first_write_reaches_follower_early(Some(true)).await?;
    assert!(
        replicated_early,
        "the follower should receive the first write while the leader is still appending the second"
    );
    Ok(())
}

/// By default the submitted watermark is published once per loop iteration, so the first write
/// is not replicated until the leader has also appended the second. This is the baseline that
/// shows the case above measures a real difference.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn entry_waits_for_the_loop_iteration_by_default() -> Result<()> {
    let replicated_early = first_write_reaches_follower_early(None).await?;
    assert!(
        !replicated_early,
        "by default the first write should not be replicated until the second is appended"
    );
    Ok(())
}

/// How long each leader append takes. Two appends run back to back, so the first write is
/// replicated after one `APPEND_DELAY` with the eager broadcast and after two without it.
const APPEND_DELAY: Duration = Duration::from_millis(500);

/// Queues two writes behind a slow leader append and reports whether a follower received the
/// first write before the leader finished appending the second.
async fn first_write_reaches_follower_early(broadcast_submitted_on_append: Option<bool>) -> Result<bool> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            // The leader lease equals `election_timeout_max`. Keep it well above the two slow
            // appends so the leader still accepts the second write without heartbeats.
            election_timeout_min: 5_000,
            election_timeout_max: 6_000,
            // Keep each client write its own RaftMsg, so the two writes are appended separately
            // within one loop iteration.
            api_batch_capacity: 1,
            broadcast_submitted_on_append,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- bring up a 3-node cluster");
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;

    tracing::info!(log_index, "--- slow down appends on the leader only");
    {
        let (_sto, sm) = router.get_storage_handle(&0)?;
        sm.block.set_blocking(BlockOperation::DelayAppend, APPEND_DELAY);
    }

    tracing::info!(
        log_index,
        "--- queue two writes, then watch when the first reaches node-1"
    );
    let replicated_early = {
        n0.client_write_ff(ClientRequest::make_request("first", 1), None).await?;
        n0.client_write_ff(ClientRequest::make_request("second", 2), None).await?;
        let first = log_index + 1;
        log_index += 2;

        // The first append finishes at about 1 * APPEND_DELAY and the second at 2 * APPEND_DELAY.
        // Waiting 1.6 * APPEND_DELAY separates "replicated after the first append" from
        // "replicated after both".
        let early = APPEND_DELAY.mul_f64(1.6);
        let res = router.wait(&1, Some(early)).log_index_at_least(Some(first), "first write on node-1").await;

        for id in [0, 1, 2] {
            router.wait(&id, timeout()).applied_index(Some(log_index), "both writes applied").await?;
        }

        res.is_ok()
    };

    Ok(replicated_early)
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
