use futures::StreamExt;
use std::{marker::PhantomData, io::ErrorKind};
use async_nats::{jetstream::{consumer::{push::{Messages, Config}, DeliverPolicy}, stream::Stream, Context, self}, Client};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
// use tracing::{info, warn};

// Each event message is nearly of size 0.3 KB. So the stream of this size(3MB) can hold nearly 10K messages
const STREAM_SIZE: i64 = 3 * 1024 * 1024;

// Stream name
const STREAM_NAME: &str = "stats-stream";

// Stats consumer name
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
    PublishTimeout {
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
                    tokio::time::sleep(interval).await;
                    continue;
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

    fn get_subjects() -> Vec<String> {
        vec![
            "stats.events.volume".to_string(),
            "stats.events.nexus".to_string(),
            "stats.events.pool".to_string(),
            "stats.events.replica".to_string(),
            "stats.events.node".to_string(),
            "stats.events.ha".to_string(),
        ]
    }

    async fn create_consumer_and_get_messages(&mut self, stream: Stream) -> BusResult<Messages> {
        println!("Creating consumer {}", CONSUMER_NAME);
        // we retry if there is a server error
        let mut retries = 0;
        let max_retries = 6;
        let interval = std::time::Duration::from_secs(5);
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

            if retries == max_retries {
                return Err(Error::ConsumerError {
                    msg: format!("Nats error while getting consumer messages {}", CONSUMER_NAME),
                    error: err.to_string()
                });
            }
            retries += 1;
            tokio::time::sleep(interval).await;
        }
    }

    async fn get_or_create_stream(&self) -> BusResult<Stream> {
        println!("Creating stream {}", STREAM_NAME);
        // we retry if there is a server error
        let mut retries = 0;
        let max_retries = 6;
        let interval = std::time::Duration::from_secs(5);
        let mut log_error = true;
        let config = async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: NatsMessageBus::get_subjects(),
            max_bytes: STREAM_SIZE, 
            ..async_nats::jetstream::stream::Config::default()
        };

        loop {
            let err = match self.jetstream
            .get_or_create_stream(config.clone())
            .await {
                Ok(stream) => {
                    println!("Created stream {}", STREAM_NAME);
                    return Ok(stream);
                },
                Err(error) => error
            };
            if log_error {
                println!("Error while creating stream {}: {}. Retrying...", STREAM_NAME, err);
                log_error = false;
            }

            if retries == max_retries {
                return Err(Error::StreamError {
                    msg: format!("Error while creating stream {}", STREAM_NAME),
                    error: err.to_string()
                });
            }
            retries += 1;
            tokio::time::sleep(interval).await;
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {

    async fn publish<T: NatsMessage>(&mut self, message: &T) -> BusResult<u64> {
        let mut retries = 0;
        let max_retries = 6;
        let mut log_error = true;
        let interval = std::time::Duration::from_secs(5);
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

            if retries == max_retries {
                return Err(Error::PublishTimeout {
                    msg: format!("Error publishing message to jetstream. Retried {} times. Dropping the message.", max_retries),
                    payload: message.msg(),
                    error: err.to_string()
                });
            }
            retries += 1;
            if let Some(error) = err.downcast_ref::<std::io::Error>() {
                if error.kind() == ErrorKind::TimedOut {
                    continue;
                } 
            }
            tokio::time::sleep(interval).await;
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
