// use bytes::Bytes;
use serde::{Deserialize, Serialize};
// use uuid::Uuid;
// use crate::NatsMessage;


// General structure of message bus event
#[derive(Serialize, Deserialize, Debug)]
pub struct EventMessage {
    // Event Category
    pub category: String,
    // Event Action
    pub action: String,
    // target id for the category against which action is performed
    pub target: String,
    // Event meta data
    pub metadata: EventMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventMeta {
    // Something that uniquely identifies events
    // UUIDv4
    // GUID
    pub id: String,
    pub source: EventSource,
    // Timestamp
    pub event_timestamp: String,
    // Version of the event message
    pub version: String,
}

// Source
#[derive(Serialize, Deserialize, Debug)]
pub struct EventSource {
    // io-engine or core-agent
    pub component: String,
    //node name
    pub node: String,
}

// impl NatsMessage for EventMessage {
//     fn subject(&self) -> String {
//         format!("events.{}", self.category.to_string()) // if category is volume, then the subject for the message is 'events.volume'
//     }
//     fn payload(&self) -> bytes::Bytes {
//         Bytes::from(serde_json::to_vec(self).unwrap())
//     }
//     fn headers(&self) -> async_nats::header::HeaderMap {
//         let mut headers = async_nats::HeaderMap::new();
//         headers.insert(async_nats::header::NATS_MESSAGE_ID, new_random().as_ref());
//         headers
//     }
//     fn msg(&self) -> String {
//         format!("event: {:?}", self)
//     }
// }


// /// Generates random id for the nats message (useful for checking for duplicate messages though the duplicate_window of the nats stream)
// fn new_random() -> String {
//     let id = Uuid::new_v4();
//     id.to_string()
// }

