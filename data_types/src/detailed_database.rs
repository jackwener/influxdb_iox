use crate::DatabaseName;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Detailed metadata about a database.
#[derive(Debug, Clone, PartialEq)]
pub struct DetailedDatabase {
    /// The name of the database
    pub name: DatabaseName<'static>,
    /// The UUID of the database
    pub uuid: Uuid,
    /// The UTC datetime at which this database was deleted, if applicable
    pub deleted_at: Option<DateTime<Utc>>,
}
