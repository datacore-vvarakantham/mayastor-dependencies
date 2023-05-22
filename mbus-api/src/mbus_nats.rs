use futures::StreamExt;
use std::{marker::PhantomData, io::ErrorKind, time::Duration};
use async_nats::{jetstream::{consumer::{push::{Messages, Config}, DeliverPolicy}, stream::Stream, Context, self}, Client};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use crate::{common::{errors::JetstreamError, constants::*, retry::{BackoffOptions, backoff_with_options}}, NatsMessage, Bus};
use tracing::info;


/// Result wrapper for send/receive
pub type BusResult<T> = Result<T, JetstreamError>;

/// Initialise the Nats Message Bus
pub async fn message_bus_init(server: &str) -> NatsMessageBus {
    let mut mbus = NatsMessageBus::new(&server).await;
    mbus.jetstream.set_timeout(PUBLISH_TIMEOUT); // timeout for jetstream publish
    mbus
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
        info!("Connecting to the nats server {}...", server);
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
                    async_nats::Event::Disconnected => info!("NATS connection has been lost"),
                    async_nats::Event::Connected => info!("NATS connection has been reestablished"),
                    async_nats::Event::ClientError(err) => info!("Client error occurred: {}", err),
                    other => info!("Other event happened: {}", other),
            }
            }).connect(server).await {
                Ok(client) => {
                    info!("Successfully connected to the nats server {}", server);
                    return client;
                },
                Err(error) => {
                    if log_error {
                        info!("Nats connection error: {}. Retrying...", error);
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

    pub async fn verify_stream_exists(&mut self) -> BusResult<()> {
        if !self.stream_created {
            self.get_or_create_stream().await?;

            self.stream_created = true;
        }
        Ok(())
    }

    async fn create_consumer_and_get_messages(&mut self, stream: Stream) -> BusResult<Messages> {
        info!("Getting/Creating consumer for stats '{}'", CONSUMER_NAME);
        let options = BackoffOptions::new();
        let mut tries = 0;
        // let max_retries = 6;
        // let interval = std::time::Duration::from_secs(5);
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
                            info!("Getting/creating consumer for stats '{}' successful", CONSUMER_NAME);
                            return Ok(messages);
                        }
                        Err(error) => error
                    }
                },
                Err(error) => error
            };
            if log_error {
                info!("Nats error while getting consumer '{}' messages: {}. Retrying...", CONSUMER_NAME, err);
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(JetstreamError::ConsumerError {
                    consumer: CONSUMER_NAME.to_string(),
                    error: err.to_string()
                });
            }
            backoff_with_options(&mut tries, options.clone()).await;
        }
    }

    async fn get_or_create_stream(&self) -> BusResult<Stream> {
        info!("Getting/creating stream '{}'", STREAM_NAME);
        let options = BackoffOptions::new();
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
                    info!("Getting/creating stream '{}' successful", STREAM_NAME);
                    return Ok(stream);
                },
                Err(error) => error
            };
            if log_error {
                info!("Error while getting/creating stream '{}': {}. Retrying...", STREAM_NAME, err);
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(JetstreamError::StreamError {
                    stream: STREAM_NAME.to_string(),
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
                info!("Error publishing message to jetstream: {}. Retrying...", err);
                let _stream = self.verify_stream_exists().await;
                log_error = false;
            }

            if tries == options.clone().max_tries {
                return Err(JetstreamError::PublishError {
                    retries: options.max_tries,
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
        info!("Subscribing to Nats message bus");
        let stream = self.get_or_create_stream().await?;

        let messages = self.create_consumer_and_get_messages(stream).await?;
        info!("Subscribed to Nats message bus successfully");

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
                        info!("Error accessing jetstream message: {}.", error);
                        continue;
                    }
                };
                let _ack = message
                    .ack()
                    .await.map_err(|error| {
                        info!("Error acking jetstream message: {:?}", error)
                    });
                let value: Result<T, _> = serde_json::from_slice(&message.payload);
                match value {
                    Ok(value) => return Some(value),
                    Err(_error) => {
                        info!("Error parsing jetstream message: {:?}; message ignored.", message);
                    }
                }
            } else {
                return None;
            }
        }
    }
}