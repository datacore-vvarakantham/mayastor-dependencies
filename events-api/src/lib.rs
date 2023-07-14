use crate::event::EventMessage;
use async_trait::async_trait;
use mbus_nats::{BusResult, BusSubscription};
use serde::{de::DeserializeOwned, Serialize};

mod common;
pub mod event_traits;
pub mod mbus_nats;

#[async_trait]
pub trait Bus {
    /// Publish a message to message bus.
    async fn publish(&mut self, message: &EventMessage) -> BusResult<u64>;
    /// Create a subscription which can be
    /// polled for messages until the bus is closed.
    async fn subscribe<T: Serialize + DeserializeOwned>(&mut self)
        -> BusResult<BusSubscription<T>>;
}

/// Event module for the autogenerated event code.
pub mod event {
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(clippy::large_enum_variant)]
    tonic::include_proto!("v1.event");
}
