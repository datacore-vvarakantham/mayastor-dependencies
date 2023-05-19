use futures::StreamExt;
use std::{marker::PhantomData, io::ErrorKind};
use async_nats::{jetstream::{consumer::{push::{Messages, Config}, DeliverPolicy}, stream::Stream, Context, self}, Client};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
// use tracing::{info, warn};

// Each event message is nearly of size 0.3 KB. So the stream of this size(0.3 MB) can hold nearly 1 K messages
const STREAM_SIZE: i64 = 314573;

// Stream name
const STREAM_NAME: &str = "stats-stream";

// Stats consumer name for Nats mbus
const CONSUMER_NAME: &str = "stats-events-consumer";

/// Result wrapper for send/receive
pub type BusResult<T> = Result<T, Error>;

pub trait NatsMessage: Send +Sync {
    /// Returns the subject for the message
    fn subject(&self) -> String;
    ///Returns the payload to publish to the message bus
    fn payload(&self) -> Bytes;
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

#[derive(Debug)]
pub enum Error {
    PublishError {
        msg: String,
        payload: String,
        error: String
    },
    ConsumerError {
        msg: String,
        error: String,
    },
    StreamError {
        msg: String,
        error: String,
    }
}

/// Initialise the Nats Message Bus
pub async fn message_bus_init(server: &str) -> NatsMessageBus {
    NatsMessageBus::new(&server).await
}

/// Nats implementation of the Bus
#[derive(Clone)]
pub struct NatsMessageBus {
    client: Client,
    jetstream: Context,
    stream_created: bool,
}
impl NatsMessageBus {
    pub async fn connect(server: &str) -> Client {
        println!("Connecting to the nats server {}...", server);
        // We retry in a loop until successful. Once connected the nats library will handle reconnections for us.
        let options = BackoffOptions::new()
                        .with_init_delay(Duration::from_secs(5))
                        .with_cutoff(4)
                        .with_delay_step(Duration::from_secs(2))
                        .with_max_delay(Duration::from_secs(10));
        let mut tries = 0;
        let interval = std::time::Duration::from_secs(5);
        let mut log_error = true;
        loop {
            match async_nats::ConnectOptions::new().event_callback(|event| async move {
                match event {
                    async_nats::Event::Disconnected => println!("NATS connection has been lost"),
                    async_nats::Event::Connected => println!("NATS connection has been reestablished"),
                    async_nats::Event::ClientError(err) => println!("Client error occurred: {}", err),
                    other => println!("Other event happened: {}", other),
            }
            }).connect(server).await {
                Ok(client) => {
                    println!("Successfully connected to the nats server {}", server);
                    return client;
                },
                Err(error) => {
                    // if error.
                    if log_error {
                        println!("Nats connection error: {}. Retrying...", error);
                        log_error = false;
                    }
                    backoff_with_options(&mut tries, options.clone()).await;
                },
            }
        }

    }

    pub async fn new(
        server: &str
    ) -> Self {
        let client = Self::connect(server).await;
        Self {
            client: client.clone(),
            jetstream: jetstream::new(client.clone()),
            stream_created: false,
        }
    }

    pub async fn ensure_jetstream_exists(&mut self) -> BusResult<()> {
        if !self.stream_created {
            self.get_or_create_stream().await?;

            self.stream_created = true;
        }
        Ok(())
    }

    async fn create_consumer_and_get_messages(&mut self, stream: Stream) -> BusResult<Messages> {
        println!("Creating consumer {}", CONSUMER_NAME);
        let options = BackoffOptions::new();
        let mut tries = 0;
        let mut log_error = true;

        let consumer_config = Config {
            durable_name: Some(CONSUMER_NAME.to_string()),
            deliver_policy: DeliverPolicy::All,
            deliver_subject: self.client.new_inbox(),
            max_ack_pending: 1, // NOTE: when this value is 1, we receive and ack each message before moving on to the next ordered message 
            ..Default::default()
        };

        loop {
            let err = match stream.get_or_create_consumer(CONSUMER_NAME, consumer_config.clone()).await {
                Ok(consumer) => {
                    match consumer.messages().await {
                        Ok(messages) => {
                            return Ok(messages);
                        }
                        Err(error) => error
                    }
                },
                Err(error) => error
            };
            if log_error {
                println!("Nats error while getting consumer {} messages : {}. Retrying...", CONSUMER_NAME, err);
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(Error::ConsumerError {
                    msg: format!("Nats error while getting consumer messages {}", CONSUMER_NAME),
                    error: err.to_string()
                });
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }

    async fn get_or_create_stream(&self) -> BusResult<Stream> {
        println!("Getting/creating stream {}", STREAM_NAME);
        let options = BackoffOptions::new();
        let mut tries = 0;
        let mut log_error = true;
        let config = async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: vec!["stats.events.*".into()],
            duplicate_window: 120000000000, // 2 mins 0 secs - Duplicates are tracked within this window (to publish messages exactly once)
            max_bytes: STREAM_SIZE, 

            ..async_nats::jetstream::stream::Config::default()
        };

        loop {
            let err = match self.jetstream
            .get_or_create_stream(config.clone())
            .await {
                Ok(stream) => {
                    println!("Getting/creating stream {} successful", STREAM_NAME);
                    return Ok(stream);
                },
                Err(error) => error
            };
            if log_error {
                println!("Error while getting/creating stream {}: {}. Retrying...", STREAM_NAME, err);
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(Error::StreamError {
                    msg: format!("Error while getting/creating stream {}", STREAM_NAME),
                    error: err.to_string()
                });
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {

    async fn publish<T: NatsMessage>(&mut self, message: &T) -> BusResult<u64> {
        let options = BackoffOptions::publish_backoff_options();
        let mut tries = 0;
        let mut log_error = true;
        let subject = message.subject().clone();
        let payload = message.payload().clone();
        loop {
            let publish_request = self.jetstream.publish(subject.clone(), payload.clone());
            let err = match publish_request.await {
                Ok(x) => {
                    match x.await {
                        Ok(y) => {
                            return Ok(y.sequence);
                        },
                        Err(error) => error
                    }
                },
                Err(error) => error
            };
            
            if log_error {
                println!("Error publishing message to jetstream: {}. Retrying...", err);
                let _stream = self.ensure_jetstream_exists().await;
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(Error::PublishError {
                    msg: format!("Error publishing message to jetstream. Retried {} times. Dropping the message.", options.max_tries),
                    payload: message.msg(),
                    error: err.to_string()
                });
            }
            if let Some(error) = err.downcast_ref::<std::io::Error>() {
                if error.kind() == ErrorKind::TimedOut {
                    tries += 1;
                    continue;
                } 
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }

    

    async fn subscribe<T: Serialize + DeserializeOwned>(&mut self) -> BusResult<BusSubscription<T>> {
        println!("Subscribing to message bus");
        let stream = self.get_or_create_stream().await?;

        let messages = self.create_consumer_and_get_messages(stream).await?;
        println!("Subscribed to message bus successfully");

        return Ok(BusSubscription {
            messages,
            _phantom: PhantomData::default(),
        });
    }
}

pub struct BusSubscription<T> {
    pub messages: Messages,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> BusSubscription<T> {
    pub async fn next(&mut self) -> Option<T> {
        loop {
            if let Some(message) = self.messages.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        println!("Error accessing jetstream message: {}.", error);
                        continue;
                    }
                };
                let _ack = message
                    .ack()
                    .await.map_err(|error| {
                        println!("Error acking jetstream message: {:?}", error)
                    });
                let value: Result<T, _> = serde_json::from_slice(&message.payload);
                match value {
                    Ok(value) => return Some(value),
                    Err(_error) => {
                        println!("Error parsing jetstream message: {:?}; message ignored.", message);
                    }
                }
            } else {
                return None;
            }
        }
    }
}

/// Backoff delay options for the retries to the NATS jetstream message bus
/// Max number of retries until it gives up.
#[derive(Clone, Debug)]
struct BackoffOptions {
    // initial delay
    init_delay: Duration,
    // the number of attempts with initial delay
    cutoff: u32,
    // increase in delay with each retry after cutoff is reached
    step: Duration,
    // maximum delay
    max_delay: Duration,
    // maximum tries
    max_tries: u32,
}

impl Default for BackoffOptions {
    fn default() -> Self {
        Self {
            init_delay: Duration::from_secs(5),
            cutoff: 4,
            step: Duration::from_secs(2),
            max_delay: Duration::from_secs(10),
            max_tries: 10
        }
    }
}

impl BackoffOptions {

    /// New options with default values.
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// Initial delay before the first retry
    #[must_use]
    pub fn with_init_delay(mut self, init_delay: Duration) -> Self {
        self.init_delay = init_delay;
        self
    }

    /// Delay multiplied at each iteration.
    #[must_use]
    pub fn with_delay_step(mut self, step: Duration) -> Self {
        self.step = step;
        self
    }

    /// Number of tries with the initial delay
    #[must_use]
    pub fn with_cutoff(mut self, cutoff: u32) -> Self {
        self.cutoff = cutoff;
        self
    }

    /// Maximum delay
    #[must_use]
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Specify a max number of tries before giving up.
    pub fn _with_max_tries(mut self, max_tries: u32) -> Self {
        self.max_tries = max_tries;
        self
    }

    fn publish_backoff_options() -> Self {
        Self {
            init_delay: Duration::from_secs(2),
            cutoff: 4,
            step: Duration::from_secs(2),
            max_delay: Duration::from_secs(10),
            max_tries: 10
        }
    }
}
