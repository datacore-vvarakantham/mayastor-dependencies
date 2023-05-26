use snafu::Snafu;

/// All Errors that can be returns from mbus
#[derive(Debug, Snafu)]
#[snafu(visibility(pub), context(suffix(false)))]
pub enum Error {
    /// Failed to publish message to the mbus.
    #[snafu(display("Jetstream Publish Error. Retried '{}' times. Error: {}. Message : {}", retries, error, payload))]
    PublishError {
        retries: u32,
        payload: String,
        error: String
    },
    /// Failed to get consumer messages.
    #[snafu(display("Jetstream Error while getting consumer messages from consumer '{}': {}", consumer, error))]
    ConsumerError {
        consumer: String,
        error: String,
    },
    /// Failed to get/create stream.
    #[snafu(display("Jetstream Error while getting/creating stream '{}': {}", stream, error))]
    StreamError {
        stream: String,
        error: String,
    },
    /// Failed to serialise value.
    #[snafu(display("Failed to serialise value. Error {}", source))]
    SerdeSerializeError { source: serde_json::Error },
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Self::SerdeSerializeError { source }
    }
}
