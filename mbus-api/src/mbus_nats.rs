use futures::StreamExt;
use std::{marker::PhantomData, io::ErrorKind, time::Duration};
use async_nats::{jetstream::{consumer::{push::{Messages, Config}, DeliverPolicy}, stream::Stream, Context, self}, Client};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use crate::{common::{errors::JetstreamError, constants::*, retry::{BackoffOptions, backoff_with_options}}, NatsMessage, Bus};


/// Result wrapper for Jetstream requests
pub type BusResult<T> = Result<T, JetstreamError>;

/// Initialise the Nats Message Bus
pub async fn message_bus_init(server: &str) -> NatsMessageBus {
    NatsMessageBus::new(&server).await
}

/// Nats implementation of the Bus
#[derive(Clone)]
pub struct NatsMessageBus {
    client: Client,
    jetstream: Context,
}

impl NatsMessageBus {

    /// Connect to nats server
    pub async fn connect(server: &str) -> Client {
        tracing::info!("Connecting to the nats server {}...", server);
        // We retry in a loop until successful. Once connected the nats library will handle reconnections for us.
        let options = BackoffOptions::new()
                            .with_init_delay(Duration::from_secs(5))
                            .with_cutoff(4)
                            .with_delay_step(Duration::from_secs(2))
                            .with_max_delay(Duration::from_secs(10));
        let mut tries = 0;
        let mut log_error = true;
        loop {
            match async_nats::ConnectOptions::new().event_callback(|event| async move {
                match event {
                    async_nats::Event::Disconnected => tracing::warn!("NATS connection has been lost"),
                    async_nats::Event::Connected => tracing::info!("NATS connection has been reestablished"),
                    _ => ()
            }
            }).connect(server).await {
                Ok(client) => {
                    tracing::info!("Connected to the nats server {}", server);
                    return client;
                },
                Err(error) => {
                    if log_error {
                        tracing::warn!("Nats connection error: {:?}. Retrying...", error);
                        log_error = false;
                    }
                    backoff_with_options(&mut tries, options.clone()).await;
                },
            }
        }

    }

    /// Creates a new nats message bus connection
    pub async fn new(
        server: &str
    ) -> Self {
        let client = Self::connect(server).await;
        Self {
            client: client.clone(),
            jetstream: {
                let mut js = jetstream::new(client.clone());
                js.set_timeout(PUBLISH_TIMEOUT);
                js
            }
        }
    }

    /// Ensures that the stream is created on jetstream
    pub async fn verify_stream_exists(&mut self) -> BusResult<()> {
        let options = BackoffOptions::new().with_max_retries(0);
        if let Err(err) = self.get_or_create_stream(Some(options)).await {
            tracing::warn!("Error while getting/creating stream '{}': {:?}", STREAM_NAME, err);
        }        
        Ok(())
    }

    /// Creates consumer and returns an iterator for the messages on the stream
    async fn create_consumer_and_get_messages(&mut self, stream: Stream) -> BusResult<Messages> {
        tracing::info!("Getting/creating consumer for stats '{}'", CONSUMER_NAME);
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
                            tracing::info!("Getting/creating consumer for stats '{}' successful", CONSUMER_NAME);
                            return Ok(messages);
                        }
                        Err(error) => error
                    }
                },
                Err(error) => error
            };

            if tries == options.clone().max_retries {
                return Err(JetstreamError::ConsumerError {
                    consumer: CONSUMER_NAME.to_string(),
                    error: err.to_string()
                });
            }
            if log_error {
                tracing::warn!("Nats error while getting consumer '{}' messages: {:?}. Retrying...", CONSUMER_NAME, err);
                log_error = false;
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }

    /// Creates a stream if not exists on message bus. Returns a handle to the stream
    pub async fn get_or_create_stream(&self, retry_options: Option<BackoffOptions>) -> BusResult<Stream> {
        tracing::info!("Getting/creating stream '{}'", STREAM_NAME);
        let options = retry_options.unwrap_or_else(|| BackoffOptions::new());
        let mut tries = 0;
        let mut log_error = true;
        let stream_config = async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: vec![SUBJECTS.into()],
            duplicate_window: DUPLICATE_WINDOW, 
            max_bytes: STREAM_SIZE, 
            ..async_nats::jetstream::stream::Config::default()
        };

        loop {
            let err = match self.jetstream
            .get_or_create_stream(stream_config.clone())
            .await {
                Ok(stream) => {
                    tracing::info!("Getting/creating stream '{}' successful", STREAM_NAME);
                    return Ok(stream);
                },
                Err(error) => error
            };

            if tries == options.clone().max_retries {
                return Err(JetstreamError::StreamError {
                    stream: STREAM_NAME.to_string(),
                    error: err.to_string()
                });
            }
            if log_error {
                tracing::warn!("Error while getting/creating stream '{}': {:?}. Retrying...", STREAM_NAME, err);
                log_error = false;
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {

    /// Publish messages to the message bus atmost once. The message is discarded if there is any connection error after some retries
    async fn publish<T: NatsMessage>(&mut self, message: &T) -> BusResult<u64> {
        let options = BackoffOptions::publish_backoff_options();
        let mut tries = 0;
        let mut log_error = true;

        let subject = message.subject().clone();
        let payload = message.payload().clone();
        let headers = message.headers().clone();

        use std::time::Instant;
        let now = Instant::now();

        loop {
            let publish_request = self.jetstream.publish_with_headers(subject.clone(), headers.clone(), payload.clone());
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
                tracing::warn!("Error publishing message to jetstream: {:?}. Retrying...", err);
                let _stream = self.verify_stream_exists().await;
                log_error = false;
            }

            if tries == options.clone().max_retries {

                let elapsed = now.elapsed();
                tracing::info!("Elapsed: {:.2?}", elapsed);

                return Err(JetstreamError::PublishError {
                    retries: options.max_retries,
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

    /// Subscribes to the stream on messages though a consumer. Returns an ordered stream of messages published to the stream
    async fn subscribe<T: Serialize + DeserializeOwned>(&mut self) -> BusResult<BusSubscription<T>> {
        tracing::info!("Subscribing to Nats message bus");
        let stream = self.get_or_create_stream(None).await?;

        let messages = self.create_consumer_and_get_messages(stream).await?;
        tracing::info!("Subscribed to Nats message bus successfully");

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
                        tracing::warn!("Error accessing jetstream message: {:?}.", error);
                        continue;
                    }
                };
                let _ack = message
                    .ack()
                    .await.map_err(|error| {
                        tracing::warn!("Error acking jetstream message: {:?}", error)
                    });
                let value: Result<T, _> = serde_json::from_slice(&message.payload);
                match value {
                    Ok(value) => return Some(value),
                    Err(_error) => {
                        tracing::warn!("Error parsing jetstream message: {:?}; message ignored.", message);
                    }
                }
            } else {
                return None;
            }
        }
    }
}