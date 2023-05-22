use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::NatsMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct EventMessage {
    pub category: String,
    pub action: String,
    pub target: String,
    pub metadata: EventMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventMeta {
    pub id: String,
    pub source: EventSource,
    pub event_timestamp: String,
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventSource {
    pub component: String,
    pub node: String,
}

impl NatsMessage for EventMessage {
    fn subject(&self) -> String {
        format!("events.{}", self.category.to_string()) // if category is volume, then the subject for the message is 'events.volume'
    }
    fn payload(&self) -> bytes::Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap())
    }
    fn headers(&self) -> async_nats::header::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, new_random().as_ref());
        headers
    }
    fn msg(&self) -> String {
        format!("event: {:?}", self)
    }
}

fn new_random() -> String {
    let id = Uuid::new_v4();
    id.to_string()
}

