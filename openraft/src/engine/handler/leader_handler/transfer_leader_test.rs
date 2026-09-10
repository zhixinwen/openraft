use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::MembershipState;
use crate::Vote;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::raft::TransferLeaderRequest;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;

fn m23() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {2,3}], btreeset! {1,2,3})
}

fn m123() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1,2,3}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(3, 1),
    );
    eng.state.log_ids.append(log_id(1, 1, 1));
    eng.state.log_ids.append(log_id(2, 1, 3));
    eng.state.membership_state = MembershipState::new(
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m23())),
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23())),
    );
    eng.testing_new_leader();
    eng.state.server_state = eng.calc_server_state();

    eng
}

fn voter_eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 1;
    eng.state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(3, 1),
    );
    eng.state.log_ids.append(log_id(1, 1, 1));
    eng.state.log_ids.append(log_id(2, 1, 3));
    eng.state.membership_state = MembershipState::new(
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m123())),
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m123())),
    );
    eng.testing_new_leader();
    eng.state.server_state = eng.calc_server_state();

    eng
}

#[test]
fn test_leader_send_heartbeat() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.output.take_commands();

    let mut lh = eng.try_leader_handler()?;

    lh.transfer_leader(2);

    assert_eq!(lh.leader.transfer_to, Some(2));

    let lease_info = lh.state.vote.lease_info();
    assert_eq!(lease_info.1, Duration::default());
    assert_eq!(lease_info.2, false);

    assert_eq!(
        vec![
            //
            Command::BroadcastTransferLeader {
                req: TransferLeaderRequest::new(Vote::new_committed(3, 1), 2, Some(log_id(2, 1, 3))),
            },
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_transfer_timeout_starts_fenced_recovery_election() -> anyhow::Result<()> {
    let mut eng = voter_eng();
    eng.output.take_commands();

    eng.try_leader_handler()?.transfer_leader(2);
    eng.output.take_commands();

    eng.recover_from_transfer_timeout(&Vote::new_committed(3, 1), &2);

    assert!(eng.leader.is_none());
    assert_eq!(Vote::new(5, 1), *eng.state.vote_ref());
    assert_eq!(Vote::new(5, 1), *eng.candidate_ref().unwrap().vote_ref());
    assert_eq!(crate::ServerState::Candidate, eng.state.server_state);

    assert_eq!(
        vec![
            Command::FailPendingReads,
            Command::CloseReplicationStreams,
            Command::SaveVote { vote: Vote::new(5, 1) },
            Command::SendVote {
                vote_req: crate::raft::VoteRequest {
                    vote: Vote::new(5, 1),
                    last_log_id: Some(log_id(2, 1, 3)),
                    leadership_transfer: true,
                },
            },
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_stale_transfer_timeout_is_ignored() -> anyhow::Result<()> {
    let mut eng = voter_eng();
    eng.output.take_commands();

    eng.try_leader_handler()?.transfer_leader(2);
    eng.output.take_commands();

    eng.recover_from_transfer_timeout(&Vote::new_committed(3, 1), &3);

    assert_eq!(Some(&2), eng.leader_ref().unwrap().get_transfer_to());
    assert_eq!(Vote::new_committed(3, 1), *eng.state.vote_ref());
    assert_eq!(Vec::<Command<UTConfig>>::new(), eng.output.take_commands());

    Ok(())
}
