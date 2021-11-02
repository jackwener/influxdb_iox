use crate::{
    google::{FieldViolation, FromFieldOpt},
    influxdata::iox::write_buffer::v1 as write_buffer,
};
use data_types::write_buffer::{
    WriteBufferConnection, WriteBufferCreationConfig, WriteBufferDirection, DEFAULT_N_SEQUENCERS,
};
use std::{
    convert::{TryFrom, TryInto},
    num::NonZeroU32,
};

impl From<WriteBufferConnection> for write_buffer::WriteBufferConnection {
    fn from(v: WriteBufferConnection) -> Self {
        let direction: write_buffer::write_buffer_connection::Direction = v.direction.into();
        Self {
            direction: direction.into(),
            r#type: v.type_,
            connection: v.connection,
            connection_config: v.connection_config,
            creation_config: v.creation_config.map(|x| x.into()),
        }
    }
}

impl From<WriteBufferDirection> for write_buffer::write_buffer_connection::Direction {
    fn from(v: WriteBufferDirection) -> Self {
        match v {
            WriteBufferDirection::Read => Self::Read,
            WriteBufferDirection::Write => Self::Write,
        }
    }
}

impl From<WriteBufferCreationConfig> for write_buffer::WriteBufferCreationConfig {
    fn from(v: WriteBufferCreationConfig) -> Self {
        Self {
            n_sequencers: v.n_sequencers.get(),
            options: v.options,
        }
    }
}

impl TryFrom<write_buffer::WriteBufferConnection> for WriteBufferConnection {
    type Error = FieldViolation;

    fn try_from(proto: write_buffer::WriteBufferConnection) -> Result<Self, Self::Error> {
        use write_buffer::write_buffer_connection::Direction;

        let direction: Direction =
            Direction::from_i32(proto.direction).ok_or_else(|| FieldViolation {
                field: "direction".to_string(),
                description: "Cannot decode enum variant from i32".to_string(),
            })?;

        Ok(Self {
            direction: direction.try_into()?,
            type_: proto.r#type,
            connection: proto.connection,
            connection_config: proto.connection_config,
            creation_config: proto.creation_config.optional("creation_config")?,
        })
    }
}

impl TryFrom<write_buffer::write_buffer_connection::Direction> for WriteBufferDirection {
    type Error = FieldViolation;

    fn try_from(
        proto: write_buffer::write_buffer_connection::Direction,
    ) -> Result<Self, Self::Error> {
        use write_buffer::write_buffer_connection::Direction;

        match proto {
            Direction::Unspecified => Err(FieldViolation::required("direction")),
            Direction::Write => Ok(Self::Write),
            Direction::Read => Ok(Self::Read),
        }
    }
}

impl TryFrom<write_buffer::WriteBufferCreationConfig> for WriteBufferCreationConfig {
    type Error = FieldViolation;

    fn try_from(proto: write_buffer::WriteBufferCreationConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            n_sequencers: NonZeroU32::try_from(proto.n_sequencers)
                .unwrap_or_else(|_| NonZeroU32::try_from(DEFAULT_N_SEQUENCERS).unwrap()),
            options: proto.options,
        })
    }
}