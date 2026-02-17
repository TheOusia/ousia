use crate::{Object, Union, edge::Edge, error::Error};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub id: Uuid,
    pub type_name: String,
    pub owner: Uuid,
    pub data: serde_json::Value,
    pub index_meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ObjectRecord {
    pub fn to_object<T: Object>(self) -> Result<T, Error> {
        if self.type_name != T::TYPE {
            return Err(Error::TypeMismatch);
        }

        let mut val = serde_json::from_value::<T>(self.data)
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
            type_name: obj.type_name().to_string(),
            owner: meta.owner,
            index_meta: serde_json::to_value(obj.index_meta())
                .expect("Failed to serialize index_meta"),
            data: obj.__serialize_internal(),
            created_at: meta.created_at,
            updated_at: meta.updated_at,
        }
    }
}

impl<A: Object, B: Object> Into<Union<A, B>> for ObjectRecord {
    fn into(self) -> Union<A, B> {
        match self.type_name.as_str() {
            _ if self.type_name == A::TYPE => ObjectRecord::to_object::<A>(self)
                .map(Union::First)
                .unwrap_or_else(|err| {
                    panic!("Error: {:?}", err);
                }),
            _ if self.type_name == B::TYPE => ObjectRecord::to_object::<B>(self)
                .map(Union::Second)
                .unwrap_or_else(|err| {
                    panic!("Error: {:?}", err);
                }),
            _ => panic!("Invalid type name"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EdgeRecord {
    pub type_name: String,
    pub from: Uuid,
    pub to: Uuid,
    pub data: serde_json::Value,
    pub index_meta: serde_json::Value,
}

impl EdgeRecord {
    pub fn to_edge<E: Edge>(self) -> Result<E, Error> {
        if self.type_name != E::TYPE {
            return Err(Error::TypeMismatch);
        }

        let mut val = serde_json::from_value::<E>(self.data)
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let meta = val.meta_mut();
        meta.to = self.to;
        meta.from = self.from;
        Ok(val)
    }

    pub fn from_edge<'a, E: Edge>(edge: &'a E) -> Self {
        let meta = edge.meta();
        Self {
            to: meta.to,
            from: meta.from,
            type_name: edge.type_name().to_string(),
            data: serde_json::to_value(edge).expect("Failed to serialize edge"),
            index_meta: serde_json::to_value(edge.index_meta())
                .expect("Failed to serialize index meta"),
        }
    }
}
