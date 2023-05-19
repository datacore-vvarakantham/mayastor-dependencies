use bytes::Bytes;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Error};

use crate::mbus_nats::NatsMessage;

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
        format!("stats.events.{}", self.category.to_string())
    }
    fn payload(&self) -> bytes::Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap())
    }
    fn msg(&self) -> String {
        format!("event: {:?}", self)
    }
}
