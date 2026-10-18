use super::*;
use crate::app::THREAD_EVENT_CHANNEL_CAPACITY;
use crate::app::Turn;
use crate::app::TurnStatus;
use crate::test_support::PathBufExt;
use crate::test_support::test_path_buf;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::ReasoningSummaryTextDeltaNotification;
use codex_app_server_protocol::RequestId as AppServerRequestId;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::TurnDiffUpdatedNotification;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartedNotification;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

fn turn_started_notification(thread_id: ThreadId, turn_id: &str) -> ServerNotification {
    ServerNotification::TurnStarted(TurnStartedNotification {
        thread_id: thread_id.to_string(),
        turn: Turn {
            id: turn_id.to_string(),
            root_turn_id: None,
            items_view: TurnItemsView::Full,
            items: Vec::new(),
            status: TurnStatus::InProgress,
            error: None,
            started_at: Some(0),
            completed_at: None,
            duration_ms: None,
        },
    })
}

fn exec_approval_request(
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    approval_id: Option<&str>,
) -> ServerRequest {
    ServerRequest::CommandExecutionRequestApproval {
        request_id: AppServerRequestId::Integer(1),
        params: CommandExecutionRequestApprovalParams {
            kind: Default::default(),
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            item_id: item_id.to_string(),
            started_at_ms: 0,
            approval_id: approval_id.map(str::to_string),
            environment_id: None,
            reason: Some("needs approval".to_string()),
            network_approval_context: None,
            command: Some("echo hello".to_string()),
            cwd: Some(test_path_buf("/tmp/project").abs().into()),
            command_actions: None,
            additional_permissions: None,
            proposed_execpolicy_amendment: None,
            proposed_network_policy_amendments: None,
            available_decisions: None,
        },
    }
}

#[test]
fn thread_event_store_coalesces_only_adjacent_matching_agent_message_deltas() {
    let thread_id = ThreadId::new();
    let mut store = ThreadEventStore::new(/*capacity*/ 6);
    let delta = |turn_id: &str, item_id: &str, text: &str| {
        ServerNotification::AgentMessageDelta(AgentMessageDeltaNotification {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            item_id: item_id.to_string(),
            delta: text.to_string(),
        })
    };
    let approval =
        exec_approval_request(thread_id, "turn-2", "approval-1", /*approval_id*/ None);
    let other_thread_delta = ServerNotification::AgentMessageDelta(AgentMessageDeltaNotification {
        thread_id: ThreadId::new().to_string(),
        turn_id: "turn-2".to_string(),
        item_id: "item-2".to_string(),
        delta: "other thread".to_string(),
    });
    let snapshot_events = |store: &ThreadEventStore| {
        store
            .snapshot()
            .events
            .into_iter()
            .map(|event| match event {
                ThreadBufferedEvent::Notification(notification) => {
                    serde_json::to_value(notification).expect("notification should serialize")
                }
                ThreadBufferedEvent::Request(request) => {
                    serde_json::to_value(request).expect("request should serialize")
                }
                other => panic!("unexpected buffered event: {other:?}"),
            })
            .collect::<Vec<_>>()
    };

    store.push_notification(delta("turn-1", "item-1", "hello"));
    store.push_notification_ref(&delta("turn-1", "item-1", " world"));
    store.push_notification(delta("turn-1", "item-2", "another"));
    store.push_notification(delta("turn-2", "item-2", "next"));
    store.push_notification(other_thread_delta.clone());
    store.push_request(approval.clone());
    store.push_notification(delta("turn-2", "item-2", "after approval"));

    assert_eq!(
        snapshot_events(&store),
        vec![
            serde_json::to_value(delta("turn-1", "item-1", "hello world"))
                .expect("delta should serialize"),
            serde_json::to_value(delta("turn-1", "item-2", "another"))
                .expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", "next"))
                .expect("delta should serialize"),
            serde_json::to_value(other_thread_delta).expect("delta should serialize"),
            serde_json::to_value(approval).expect("request should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", "after approval"))
                .expect("delta should serialize"),
        ]
    );
    assert!(store.has_pending_thread_approvals());

    let remaining = MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES - "after approval".len();
    let exact_limit = "é".repeat(remaining / "é".len());
    let full_chunk = "é".repeat(MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES / "é".len());
    store.push_notification(delta("turn-2", "item-2", &exact_limit));
    store.push_notification(delta("turn-2", "item-2", "🙂"));
    store.push_notification(delta("turn-2", "item-2", &full_chunk));
    store.push_notification(delta("turn-2", "item-2", "🙂"));
    store.push_notification(delta("turn-2", "item-2", &full_chunk));
    store.push_notification(delta("turn-2", "item-2", "🙂"));

    assert_eq!(
        snapshot_events(&store),
        vec![
            serde_json::to_value(delta(
                "turn-2",
                "item-2",
                &format!("after approval{exact_limit}"),
            ))
            .expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", "🙂")).expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", &full_chunk))
                .expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", "🙂")).expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", &full_chunk))
                .expect("delta should serialize"),
            serde_json::to_value(delta("turn-2", "item-2", "🙂")).expect("delta should serialize"),
        ]
    );
    assert!(!store.has_pending_thread_approvals());

    let mut store = ThreadEventStore::new(/*capacity*/ THREAD_EVENT_CHANNEL_CAPACITY);
    store.push_request(exec_approval_request(
        thread_id,
        "turn-budget",
        "approval-budget",
        /*approval_id*/ None,
    ));
    for chunk in 0..65 {
        let fragment = format!("{chunk:08}");
        let notification = delta("turn-budget", "item-budget", &fragment);
        for _ in 0..MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES / fragment.len() {
            store.push_notification_ref(&notification);
        }
    }

    assert_eq!(
        snapshot_events(&store),
        (1..65)
            .map(|chunk| {
                serde_json::to_value(delta(
                    "turn-budget",
                    "item-budget",
                    &format!("{chunk:08}").repeat(MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES / 8),
                ))
                .expect("delta should serialize")
            })
            .collect::<Vec<_>>()
    );
    assert!(!store.has_pending_thread_approvals());

    let mut store = ThreadEventStore::new(/*capacity*/ 4);
    store.push_notification(turn_started_notification(thread_id, "turn-oversized"));
    store.push_request(exec_approval_request(
        thread_id,
        "turn-oversized",
        "approval-oversized",
        /*approval_id*/ None,
    ));
    store.push_notification(delta("turn-oversized", "item-oversized", "retained"));
    let expected_events = snapshot_events(&store);
    let oversized = "🙂".repeat(MAX_BUFFERED_AGENT_MESSAGE_DELTA_BYTES / "🙂".len() + 1);

    store.push_notification(delta("turn-oversized", "item-oversized", &oversized));
    store.push_notification_ref(&delta("turn-oversized", "item-oversized", &oversized));

    assert_eq!(snapshot_events(&store), expected_events);
    assert!(store.has_pending_thread_approvals());
    assert_eq!(store.buffered_agent_message_delta_bytes, "retained".len());

    store.push_notification(delta("turn-oversized", "item-oversized", " text"));
    assert_eq!(
        snapshot_events(&store),
        vec![
            serde_json::to_value(turn_started_notification(thread_id, "turn-oversized"))
                .expect("turn notification should serialize"),
            serde_json::to_value(exec_approval_request(
                thread_id,
                "turn-oversized",
                "approval-oversized",
                /*approval_id*/ None,
            ))
            .expect("approval should serialize"),
            serde_json::to_value(delta("turn-oversized", "item-oversized", "retained text"))
                .expect("delta should serialize"),
        ]
    );
    assert!(store.has_pending_thread_approvals());
}

fn reasoning_delta(thread_id: ThreadId, item_id: &str, text: String) -> ServerNotification {
    ServerNotification::ReasoningSummaryTextDelta(ReasoningSummaryTextDeltaNotification {
        thread_id: thread_id.to_string(),
        turn_id: "turn-1".to_string(),
        item_id: item_id.to_string(),
        delta: text,
        summary_index: 0,
    })
}

fn bounded_store() -> ThreadEventStore {
    let mut store = ThreadEventStore::new(THREAD_EVENT_CHANNEL_CAPACITY);
    store.byte_budget = Some(MAX_BUFFERED_EVENT_BYTES);
    store
}

fn turn_diff(thread_id: ThreadId) -> ServerNotification {
    ServerNotification::TurnDiffUpdated(TurnDiffUpdatedNotification {
        thread_id: thread_id.to_string(),
        turn_id: "turn-1".to_string(),
        diff: "diff --git a/file b/file".repeat(1024),
    })
}

#[test]
fn thread_event_store_bounds_buffered_bytes_and_marks_replay_truncated() {
    let thread_id = ThreadId::new();
    let mut store = bounded_store();
    let chunk = "x".repeat(1024 * 1024);
    for index in 0..40 {
        store.push_notification(reasoning_delta(
            thread_id,
            &format!("item-{index}"),
            chunk.clone(),
        ));
    }

    assert!(store.replay_truncated);
    assert!(store.buffered_event_bytes <= MAX_BUFFERED_EVENT_BYTES);
    assert!(store.buffer.len() < 40);
    let retained = store.buffered_event_bytes;
    store.recount_buffered_bytes();
    assert_eq!(store.buffered_event_bytes, retained);
    let Some(ThreadBufferedEvent::Notification(newest)) = store.buffer.back() else {
        panic!("expected the newest notification to stay buffered");
    };
    assert_eq!(
        serde_json::to_value(newest).expect("notification should serialize"),
        serde_json::to_value(reasoning_delta(thread_id, "item-39", chunk))
            .expect("notification should serialize")
    );
}

#[test]
fn thread_event_store_without_byte_budget_keeps_count_only_bound() {
    let thread_id = ThreadId::new();
    let mut store = ThreadEventStore::new(THREAD_EVENT_CHANNEL_CAPACITY);
    let chunk = "x".repeat(1024 * 1024);
    for index in 0..20 {
        store.push_notification(reasoning_delta(
            thread_id,
            &format!("item-{index}"),
            chunk.clone(),
        ));
    }
    store.push_notification(turn_diff(thread_id));

    assert_eq!(store.buffer.len(), 21);
    assert_eq!(store.buffered_event_bytes, 0);
    assert!(!store.replay_truncated);
}

#[test]
fn thread_event_store_with_byte_budget_skips_turn_diffs_that_replay_ignores() {
    let thread_id = ThreadId::new();
    let mut store = bounded_store();
    store.push_notification(turn_diff(thread_id));

    assert!(store.buffer.is_empty());
    assert_eq!(store.buffered_event_bytes, 0);
}

#[test]
fn thread_event_store_compaction_keeps_pending_requests_until_history_refresh() {
    let thread_id = ThreadId::new();
    let mut store = bounded_store();
    let approval =
        exec_approval_request(thread_id, "turn-1", "approval-1", /*approval_id*/ None);
    store.push_notification(turn_started_notification(thread_id, "turn-1"));
    store.push_notification(reasoning_delta(thread_id, "item-1", "thinking".to_string()));
    store.push_request(approval.clone());

    store.compact_replay_buffer();

    assert!(store.replay_truncated);
    assert_eq!(store.buffer.len(), 1);
    let Some(ThreadBufferedEvent::Request(request)) = store.buffer.front() else {
        panic!("expected the pending approval to stay buffered");
    };
    assert_eq!(
        serde_json::to_value(request).expect("request should serialize"),
        serde_json::to_value(approval).expect("request should serialize")
    );
    assert!(store.has_pending_thread_approvals());
    assert_eq!(store.active_turn_id(), Some("turn-1"));

    store.rebase_buffer_after_session_refresh();
    assert!(!store.replay_truncated);
}
