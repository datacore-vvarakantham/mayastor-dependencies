use std::{collections::BTreeMap, str::FromStr};
use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use bytes::Bytes;
use crate::NatsMessage;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{transport::{CreatePool, DestroyPool, CreateReplica, DestroyReplica, CreateNexus, DestroyNexus, Volume}, store::pool::PoolSpec},
    // types::v0::store::pool::PoolSpec,
    
};
// use crate::{
//     types::v0::store::pool::PoolSpec
// };
// use grpc::{
//     operations::{
//         volume::traits::{
//             PublishVolumeInfo, RepublishVolumeInfo, UnpublishVolumeInfo,
//         },
//         nexus::traits::{
//             DestroyNexusInfo,
//         },
//     },
// };
// use derivative::Derivative;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCategory {
    Volume,
    Nexus,
    Pool, 
    Replica,
}

impl FromStr for EventCategory {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "volume" => Ok(EventCategory::Volume),
            "nexus" => Ok(EventCategory::Nexus),
            "pool" => Ok(EventCategory::Pool),
            "replica" => Ok(EventCategory::Replica),
            _ => Err(anyhow!(
                "The string {:?} does not describe a valid category.",
                s
            )),
        }
    }
}

impl ToString for EventCategory {
    fn to_string(&self) -> String {
        match self {
            EventCategory::Volume => "volume".to_string(),
            EventCategory::Nexus => "nexus".to_string(),
            EventCategory::Pool => "pool".to_string(),
            EventCategory::Replica => "replica".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventAction {
    Created,
    Deleted,
}

impl FromStr for EventAction {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "created" => Ok(EventAction::Created),
            "deleted" => Ok(EventAction::Deleted),
            _ => Err(anyhow!(
                "The string {:?} does not describe a valid category.",
                s
            )),
        }
    }
}

impl ToString for EventAction {
    fn to_string(&self) -> String {
        match self {
            EventAction::Created => "created".to_string(),
            EventAction::Deleted => "deleted".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventMessage {
    pub category: String,
    pub action: String,
    pub target: String,
    pub metadata: EventMeta,
}

// #[derive(Derivative)]
#[derive(Serialize, Deserialize, Debug)]
pub struct EventMeta {
    pub id: String,
    pub source: EventSource,
    pub event_timestamp: String,
    pub version: String,
}

// #[derive(Derivative)]
#[derive(Serialize, Deserialize, Debug)]
pub struct EventSource {
    pub component: String,
    pub node: String,
}

impl NatsMessage for EventMessage {
    fn subject(&self) -> String {
        format!("events.{}", self.category.to_string()) // if category is volume, then the subject for the message is 'events.volume'
    }
    fn payload(&self) -> bytes::Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap())
    }
    fn headers(&self) -> async_nats::header::HeaderMap {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, new_random().as_ref());
        headers
    }
    fn msg(&self) -> String {
        format!("event: {:?}", self)
    }
}

fn new_random() -> String {
    let id = Uuid::new_v4();
    id.to_string()
}

impl EventMessage {
    pub fn from_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
        match event.get("event") {
            Some(event_msg) => match event_msg.as_str() {
                "VolumeCreated" => get_volume_created_event(event),
                "VolumeDeleted" => get_volume_deleted_event(event),
                "NexusCreated" => get_nexus_created_event(event),
                "NexusDeleted" => get_nexus_deleted_event(event),
                "PoolCreated" => get_pool_created_event(event),
                "PoolDeleted" => get_pool_deleted_event(event),
                "ReplicaCreated" => get_replica_created_event(event),
                "ReplicaDeleted" => get_replica_deleted_event(event),
                _ => {
                    println!("Unexpected event message");
                    return None;
                }
            },
            None => return None,
        }
    }
}

fn get_volume_created_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &req.uuid().to_string(), node = ""
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: Volume = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Volume.to_string(),
        action: EventAction::Created.to_string(),
        target: p.spec().uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.state().target_node().unwrap()?.to_string(),  
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_volume_deleted_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = req.uuid().to_string(), node = ""
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: Volume = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Volume.to_string(),
        action: EventAction::Deleted.to_string(),
        target: p.spec().uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.state().target_node().unwrap()?.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_pool_created_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &pool.spec().unwrap().id.to_string(), node = &pool.spec().unwrap().node.to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: PoolSpec = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Pool.to_string(),
        action: EventAction::Created.to_string(),
        target: p.id.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_pool_deleted_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &req_clone.id.to_string(), node = &req_clone.node.to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: DestroyPool = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        category: EventCategory::Pool.to_string(),
        action: EventAction::Deleted.to_string(),
        target: p.id.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_nexus_created_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &nexus.uuid.to_string(), node = &nexus.node.to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: CreateNexus = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Nexus.to_string(),
        action: EventAction::Created.to_string(),
        target: p.uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_nexus_deleted_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &req.uuid().to_string(), node = &req.node().to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: DestroyNexus = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Nexus.to_string(),
        action: EventAction::Deleted.to_string(),
        target: p.uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_replica_created_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &request.uuid.to_string(), node = &request.node.to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: CreateReplica = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Replica.to_string(),
        action: EventAction::Created.to_string(),
        target: p.uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}

fn get_replica_deleted_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
    // target = &request.uuid.to_string(), node = &request.node.to_string()
    let data = event.get("event_data").unwrap().to_string();
    println!("{:?}", data);
    let p: DestroyReplica = serde_json::from_str(&data).unwrap();
    Some(EventMessage {
        
        category: EventCategory::Replica.to_string(),
        action: EventAction::Deleted.to_string(),
        target: p.uuid.to_string(),
        metadata: EventMeta {
            id: new_random(),
            source: EventSource {
                node: p.node.to_string(), 
                component: "core-agent".to_string(),
            },
            event_timestamp: event.get("timestamp").unwrap().to_string(),
            version: "v0".to_string(),
        },
    })
    //let p: Person = serde_json::from_str(data)?;
}


