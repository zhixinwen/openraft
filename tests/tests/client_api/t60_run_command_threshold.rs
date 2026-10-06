use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::RaftLogReader;
use openraft_memstore::BlockOperation;
use openraft_memstore::ClientRequest;
use openraft_memstore::IntoMemClientRequest;

use crate::fixtures::RaftRouter;
use crate::fixtures::ut_harness;

/// With `run_command_threshold` set, client writes that queue up behind a slow `append` are
/// merged into a single `append` instead of being appended one by one.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn queued_writes_are_appended_in_one_batch() -> Result<()> {
    let appends = write_behind_slow_append(Some(64)).await?;

    // The first write may be appended on its own before the rest arrive; everything queued
    // behind it must go out in one more append.
    assert!(
        appends <= 2,
        "{} queued writes should take at most 2 appends with batching, got {}",
        WRITES,
        appends
    );

    Ok(())
}

/// Without `run_command_threshold`, every queued client write is appended on its own. This is
/// the baseline that shows the batching case above measures a real difference.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn queued_writes_are_appended_one_by_one_by_default() -> Result<()> {
    let appends = write_behind_slow_append(None).await?;

    assert_eq!(
        WRITES, appends,
        "each queued write should be its own append when batching is disabled"
    );

    Ok(())
}

const WRITES: u64 = 10;

/// Sends `WRITES` client writes to a single-node cluster whose log store delays every append,
/// waits for all of them to be applied, and returns how many `append` calls they took.
async fn write_behind_slow_append(run_command_threshold: Option<u64>) -> Result<u64> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            // Keep every client write its own RaftMsg, so only `run_command_threshold` can merge
            // their appends.
            api_batch_capacity: 1,
            run_command_threshold,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- bring up a single-node cluster");
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    let (mut sto, sm) = router.get_storage_handle(&0)?;
    let n0 = router.get_raft_handle(&0)?;

    tracing::info!(
        log_index,
        "--- slow down appends so writes queue up behind the first one"
    );
    let appends_before = {
        sm.block.set_blocking(BlockOperation::DelayAppend, Duration::from_millis(100));
        sto.append_calls.load(Ordering::Relaxed)
    };

    tracing::info!(
        log_index,
        "--- send {} writes without waiting for each to finish",
        WRITES
    );
    {
        for i in 0..WRITES {
            n0.client_write_ff(ClientRequest::make_request("batch", i), None).await?;
        }
        log_index += WRITES;

        router.wait(&0, timeout()).applied_index(Some(log_index), "all writes applied").await?;
    }

    tracing::info!(log_index, "--- every write is in the log, in order");
    {
        let entries = sto.try_get_log_entries(log_index + 1 - WRITES..=log_index).await?;
        assert_eq!(WRITES as usize, entries.len());
    }

    Ok(sto.append_calls.load(Ordering::Relaxed) - appends_before)
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
