use serde::{Deserialize, Serialize};

use crate::{Object, edges::meta::EdgeMeta, query::IndexMeta};

pub trait Edge: Serialize + for<'de> Deserialize<'de> + Sized + Send + Sync + 'static {
    /// Source object type (compile-time only)
    type From: Object;

    /// Target object type (compile-time only)
    type To: Object;

    /// Edge logical type (e.g. "Follow", "Member", "Like")
    const TYPE: &'static str;

    /// Object type name helper
    fn type_name(&self) -> &'static str {
        Self::TYPE
    }

    fn meta(&self) -> &EdgeMeta;

    fn meta_mut(&mut self) -> &mut EdgeMeta;

    /// Indexable fields
    fn index_meta(&self) -> IndexMeta;
}
