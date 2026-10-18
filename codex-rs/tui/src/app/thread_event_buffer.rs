//! Bounded replay-buffer policy for per-thread app-server events.

use super::ServerNotification;
use super::ThreadBufferedEvent;
use super::ThreadEventStore;
use std::borrow::Cow;

// Keep merged text finite so continued streaming still reaches bounded replay eviction.
const MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES: usize = 4 * 1024;
const MAX_BUFFERED_AGENT_MESSAGE_DELTA_BYTES: usize = 256 * 1024;
// With `bounded_thread_replay`, bound every thread's replay payload, not only its event count: a
// long-lived session can hold hundreds of subagent threads, and completed items carry full command
// output and reasoning.
pub(super) const MAX_BUFFERED_EVENT_BYTES: usize = 16 * 1024 * 1024;

/// Approximate retained size of a buffered event: its serialized JSON length. Agent message
/// deltas count only their text so in-place coalescing keeps the running total exact.
fn estimated_event_bytes(event: &ThreadBufferedEvent) -> usize {
    match event {
        ThreadBufferedEvent::Notification(notification) => match notification.as_ref() {
            ServerNotification::AgentMessageDelta(delta) => delta.delta.len(),
            notification => serialized_len(notification),
        },
        ThreadBufferedEvent::Request(request) => serialized_len(request.as_ref()),
        ThreadBufferedEvent::HistoryEntryResponse(_)
        | ThreadBufferedEvent::FeedbackSubmission(_) => 0,
    }
}

fn serialized_len(value: &impl serde::Serialize) -> usize {
    struct ByteCounter(usize);
    impl std::io::Write for ByteCounter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = ByteCounter(0);
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

impl ThreadEventStore {
    pub(super) fn push_replay_notification(&mut self, notification: Cow<'_, ServerNotification>) {
        if let ServerNotification::AgentMessageDelta(delta) = notification.as_ref()
            && delta.delta.len() > MAX_BUFFERED_AGENT_MESSAGE_DELTA_BYTES
        {
            return;
        }

        if let ServerNotification::AgentMessageDelta(delta) = notification.as_ref()
            && let Some(ThreadBufferedEvent::Notification(previous)) = self.buffer.back_mut()
            && let ServerNotification::AgentMessageDelta(previous) = previous.as_mut()
            && previous.thread_id == delta.thread_id
            && previous.turn_id == delta.turn_id
            && previous.item_id == delta.item_id
            && previous.delta.len().saturating_add(delta.delta.len())
                <= MAX_COALESCED_AGENT_MESSAGE_DELTA_BYTES
        {
            previous.delta.push_str(&delta.delta);
            self.buffered_agent_message_delta_bytes = self
                .buffered_agent_message_delta_bytes
                .saturating_add(delta.delta.len());
            if self.byte_budget.is_some() {
                self.buffered_event_bytes =
                    self.buffered_event_bytes.saturating_add(delta.delta.len());
            }
            self.evict_overflowing_events();
            return;
        }

        self.push_buffered_event(ThreadBufferedEvent::Notification(Box::new(
            notification.into_owned(),
        )));
    }

    pub(super) fn push_buffered_event(&mut self, event: ThreadBufferedEvent) {
        if let ThreadBufferedEvent::Notification(notification) = &event
            && let ServerNotification::AgentMessageDelta(delta) = notification.as_ref()
        {
            self.buffered_agent_message_delta_bytes = self
                .buffered_agent_message_delta_bytes
                .saturating_add(delta.delta.len());
        }
        if self.byte_budget.is_some() {
            self.buffered_event_bytes = self
                .buffered_event_bytes
                .saturating_add(estimated_event_bytes(&event));
        }
        self.buffer.push_back(event);
        self.evict_overflowing_events();
    }

    /// Recounts retained bytes after the buffer was filtered or edited in place.
    pub(super) fn recount_buffered_bytes(&mut self) {
        self.buffered_event_bytes = if self.byte_budget.is_some() {
            self.buffer.iter().map(estimated_event_bytes).sum()
        } else {
            0
        };
        self.buffered_agent_message_delta_bytes = self
            .buffer
            .iter()
            .map(|event| match event {
                ThreadBufferedEvent::Notification(notification) => match notification.as_ref() {
                    ServerNotification::AgentMessageDelta(delta) => delta.delta.len(),
                    _ => 0,
                },
                _ => 0,
            })
            .sum();
    }

    /// Drops buffered events that thread history can rebuild, keeping pending requests and the
    /// few notifications history does not carry. The next replay refreshes from the server.
    pub(super) fn compact_replay_buffer(&mut self) {
        let before = self.buffer.len();
        self.buffer.retain_mut(Self::event_survives_session_refresh);
        if self.buffer.len() != before {
            self.replay_truncated = true;
        }
        self.recount_buffered_bytes();
    }

    fn evict_overflowing_events(&mut self) {
        while self.buffer.len() > self.capacity
            || self.buffered_agent_message_delta_bytes > MAX_BUFFERED_AGENT_MESSAGE_DELTA_BYTES
            || self
                .byte_budget
                .is_some_and(|budget| self.buffered_event_bytes > budget)
        {
            let Some(removed) = self.buffer.pop_front() else {
                break;
            };
            self.replay_truncated = true;
            if self.byte_budget.is_some() {
                self.buffered_event_bytes = self
                    .buffered_event_bytes
                    .saturating_sub(estimated_event_bytes(&removed));
            }
            match removed {
                ThreadBufferedEvent::Notification(notification) => {
                    if let ServerNotification::AgentMessageDelta(delta) = notification.as_ref() {
                        self.buffered_agent_message_delta_bytes = self
                            .buffered_agent_message_delta_bytes
                            .saturating_sub(delta.delta.len());
                    }
                }
                ThreadBufferedEvent::Request(request) => self
                    .pending_interactive_replay
                    .note_evicted_server_request(request.as_ref()),
                ThreadBufferedEvent::HistoryEntryResponse(_)
                | ThreadBufferedEvent::FeedbackSubmission(_) => {}
            }
        }
    }
}

#[cfg(test)]
#[path = "thread_event_buffer_tests.rs"]
mod tests;
