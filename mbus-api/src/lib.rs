use async_trait::async_trait;
use bytes::Bytes;
use mbus_nats::{BusSubscription, BusResult};
use serde::{Serialize, de::DeserializeOwned};

mod mbus_nats;
mod message;
mod common;

pub trait NatsMessage: Send + Sync {
    /// Returns the subject for the message
    fn subject(&self) -> String;
    ///Returns the payload to publish to the message bus
    fn payload(&self) -> Bytes;
    ///Returns the headers with the message id to publish to the message bus.
    /// The message id is required to publish the message atmost once
    fn headers(&self) -> async_nats::header::HeaderMap;
    ///Returns the message for logging
    fn msg(&self) -> String;
}

#[async_trait]
pub trait Bus {
    /// publish a message to message bus
    async fn publish<T: NatsMessage>(&mut self,
        message: &T) -> BusResult<u64>;
    /// Create a subscription which can be
    /// polled for messages until it is either explicitly closed or
    /// when the bus is closed
    async fn subscribe<T: Serialize + DeserializeOwned>(&mut self) -> BusResult<BusSubscription<T>>;
}
