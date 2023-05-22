use std::time::Duration;

/// Each event message is nearly of size 0.3 KB. So the stream of this size(3 MB) can hold nearly 10 K messages
pub const STREAM_SIZE: i64 = 3 * 1024 * 1024;

/// Stream name for the events
pub const STREAM_NAME: &str = "events-stream";

/// Stats consumer name for message bus
pub const CONSUMER_NAME: &str = "stats-events-consumer";

/// Subjects for events stream
pub const SUBJECTS: &str = "events.*";

/// 3 mins 0 secs - Duplicates are tracked within this window (to publish messages atmost once)
pub const DUPLICATE_WINDOW: i64 = 180000000000;

/// Timeout for jetstream publish
pub const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);