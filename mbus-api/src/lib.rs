use async_trait::async_trait;
// use bytes::Bytes;
use mbus_nats::{BusSubscription, BusResult};
use message::EventMessage;
// use messaged::EventMessage;
use serde::{Serialize, de::DeserializeOwned};

pub mod mbus_nats;
pub mod message;
mod common;
// pub mod messaged;

// pub trait NatsMessage: Send + Sync {
//     /// Returns the subject for the message
//     fn subject(&self) -> String;
//     ///Returns the payload to publish to the message bus
//     fn payload(&self) -> Bytes;
//     ///Returns the headers with the message id to publish to the message bus.
//     /// The message id is required to publish the message atmost once
//     fn headers(&self) -> async_nats::header::HeaderMap;
//     ///Returns the message for logging
//     fn msg(&self) -> String;
// }

#[async_trait]
pub trait Bus {
    /// publish a message to message bus.
    async fn publish(&mut self,
        message: &EventMessage) -> BusResult<u64>;
    /// Create a subscription which can be
    /// polled for messages until the bus is closed.
    async fn subscribe<T: Serialize + DeserializeOwned>(&mut self) -> BusResult<BusSubscription<T>>;
}
