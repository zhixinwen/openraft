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

/// A small RaftMsg run lets RaftCore observe a quorum acknowledgement before it finishes
/// appending every queued client write.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn replication_progress_is_processed_between_raft_msg_runs() -> Result<()> {
    let committed_early = first_write_commits_early(Some(1)).await?;
    assert!(
        committed_early,
        "the first write should commit before every queued write has been appended"
    );
    Ok(())
}

/// Without a cap, RaftCore drains all queued client messages before it handles the follower's
/// replication-progress notification.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn replication_progress_waits_for_the_raft_msg_budget_by_default() -> Result<()> {
    let committed_early = first_write_commits_early(None).await?;
    assert!(
        !committed_early,
        "by default the first write should wait for every queued write to be appended"
    );
    Ok(())
}

const APPEND_DELAY: Duration = Duration::from_millis(500);
const WRITES: u64 = 5;

/// Queue five writes behind slow leader appends and report whether the first commits well before
/// the final append can finish.
async fn first_write_commits_early(max_raft_msg_per_run: Option<u64>) -> Result<bool> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            election_timeout_min: 5_000,
            election_timeout_max: 6_000,
            api_batch_capacity: 1,
            broadcast_submitted_on_append: Some(true),
            max_raft_msg_per_run,
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

    tracing::info!(log_index, "--- queue writes and observe the first commit");
    let committed_early = {
        for i in 0..WRITES {
            n0.client_write_ff(ClientRequest::make_request("fairness", i), None).await?;
        }

        let first = log_index + 1;
        log_index += WRITES;

        // The first acknowledgement arrives while a later append is delayed. With a one-message
        // run it is processed near the front of the queue; without the cap it waits for all five
        // appends. Four append delays leave a full-delay margin between those outcomes.
        let early = APPEND_DELAY.mul_f64(4.0);
        let res = router.wait(&0, Some(early)).committed_index(Some(first), "first write committed").await;

        for id in [0, 1, 2] {
            router.wait(&id, timeout()).applied_index(Some(log_index), "all writes applied").await?;
        }

        res.is_ok()
    };

    Ok(committed_early)
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
