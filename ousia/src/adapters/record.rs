use std::borrow::Cow;

use crate::{Object, Union, edge::Edge, error::Error};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A row's worth of object data flowing between the adapter and the
/// engine. `data` is MessagePack bytes (the `BYTEA data` column);
/// `index_meta` stays as JSONB so Postgres can GIN-index it.
#[derive(Debug)]
pub struct ObjectRecord {
    pub id: Uuid,
    pub type_name: Cow<'static, str>,
    pub owner: Uuid,
    pub data: Vec<u8>,
    pub index_meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ObjectRecord {
    pub fn to_object<T: Object>(self) -> Result<T, Error> {
        let mut val: T = rmp_serde::from_slice(&self.data)
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let meta = val.meta_mut();
        meta.id = self.id;
        meta.owner = self.owner;
        meta.created_at = self.created_at;
        meta.updated_at = self.updated_at;
        Ok(val)
    }

    pub fn from_object<'a, T: Object>(obj: &'a T) -> Self {
        let meta = obj.meta();
        Self {
            id: meta.id,
            type_name: Cow::Borrowed(obj.type_name()),
            index_meta: serde_json::to_value(obj.index_meta())
                .expect("Failed to serialize index_meta"),
            data: obj.__serialize_internal(),
            owner: meta.owner,
            created_at: meta.created_at,
            updated_at: meta.updated_at,
        }
    }
}

impl<A: Object, B: Object> Into<Union<A, B>> for ObjectRecord {
    fn into(self) -> Union<A, B> {
        match self.type_name.as_ref() {
            t if t == A::TYPE => ObjectRecord::to_object::<A>(self)
                .map(Union::First)
                .unwrap_or_else(|err| {
                    panic!("Error: {:?}", err);
                }),
            t if t == B::TYPE => ObjectRecord::to_object::<B>(self)
                .map(Union::Second)
                .unwrap_or_else(|err| {
                    panic!("Error: {:?}", err);
                }),
            _ => panic!("Invalid type name"),
        }
    }
}

#[derive(Debug)]
pub struct EdgeRecord {
    pub type_name: Cow<'static, str>,
    pub from: Uuid,
    pub to: Uuid,
    pub data: Vec<u8>,
    pub index_meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl EdgeRecord {
    pub fn to_edge<E: Edge>(self) -> Result<E, Error> {
        let mut val: E = rmp_serde::from_slice(&self.data)
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let meta = val.meta_mut();
        meta.to = self.to;
        meta.from = self.from;
        meta.created_at = self.created_at;
        meta.updated_at = self.updated_at;
        Ok(val)
    }

    pub fn from_edge<'a, E: Edge>(edge: &'a E) -> Self {
        let meta = edge.meta();
        Self {
            to: meta.to,
            from: meta.from,
            type_name: Cow::Borrowed(edge.type_name()),
            data: rmp_serde::to_vec_named(edge)
                .expect("ousia: msgpack serialization of edge data failed"),
            index_meta: serde_json::to_value(edge.index_meta())
                .expect("Failed to serialize index meta"),
            created_at: meta.created_at,
            updated_at: meta.updated_at,
        }
    }
}
