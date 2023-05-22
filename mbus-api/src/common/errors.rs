use snafu::Snafu;

/// Contains Errors that occur while execution of NATS client
#[derive(Debug, Snafu)]
#[snafu(visibility(pub), context(suffix(false)))]
pub enum JetstreamError {
    #[snafu(display("Jetstream Publish Error. Retried '{}' times. Error: {}. Dropping the message : {}", retries, error, payload))]
    PublishError {
        retries: u32,
        payload: String,
        error: String
    },
    #[snafu(display("Jetstream Error while getting consumer messages from consumer '{}': {}", consumer, error))]
    ConsumerError {
        consumer: String,
        error: String,
    },
    #[snafu(display("Jetstream Error while getting/creating stream '{}': {}", stream, error))]
    StreamError {
        stream: String,
        error: String,
    }
}