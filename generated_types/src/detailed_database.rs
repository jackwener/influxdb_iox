use crate::{
    google::{FieldViolation, FieldViolationExt},
    influxdata::iox::management::v1 as management,
};
use data_types::{detailed_database::DetailedDatabase, DatabaseName};
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

impl From<DetailedDatabase> for management::DetailedDatabase {
    fn from(database: DetailedDatabase) -> Self {
        let DetailedDatabase {
            name,
            uuid,
            deleted_at,
        } = database;

        Self {
            db_name: name.to_string(),
            uuid: uuid.as_bytes().to_vec(),
            deleted_at: deleted_at.map(Into::into),
        }
    }
}

impl TryFrom<management::DetailedDatabase> for DetailedDatabase {
    type Error = FieldViolation;

    fn try_from(proto: management::DetailedDatabase) -> Result<Self, Self::Error> {
        let name = DatabaseName::new(proto.db_name.clone()).field("name")?;

        let uuid = Uuid::from_slice(&proto.uuid).map_err(|e| FieldViolation {
            field: "uuid".to_string(),
            description: e.to_string(),
        })?;

        let deleted_at = proto
            .deleted_at
            .map(|da| {
                da.try_into().map_err(|_| FieldViolation {
                    field: "deleted_at".to_string(),
                    description: "Timestamp must be positive".to_string(),
                })
            })
            .transpose()?;

        Ok(Self {
            name,
            uuid,
            deleted_at,
        })
    }
}
