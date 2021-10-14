#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! A mutable data structure for a collection of writes
//!
//! Currently supports:
//! - `[TableBatch`] writes
//! - [`RecordBatch`] conversion

use crate::column::Column;
use arrow::record_batch::RecordBatch;
use entry::TableBatch;
use hashbrown::HashMap;
use influxdb_line_protocol::{FieldValue, ParsedLine};
use schema::selection::Selection;
use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use generated_types::influxdata::pbdata::v1::{
    column::{SemanticType, Values as PbValues},
    Column as PbColumn, TableBatch as PbTableBatch,
};

pub mod column;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display("Column {} had {} rows, expected {}", column, expected, actual))]
    IncorrectRowCount {
        column: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema { source: schema::builder::Error },

    #[snafu(display("Column not found: {}", column))]
    ColumnNotFound { column: String },

    #[snafu(display("Mask had {} rows, expected {}", expected, actual))]
    IncorrectMaskLength { expected: usize, actual: usize },
}

/// A specialized `Error` for [`MutableBatch`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a Chunk of data (a horizontal subset of a table) in
/// the mutable store.
#[derive(Debug, Default)]
pub struct MutableBatch {
    /// Map of column id from the chunk dictionary to the column
    columns: HashMap<String, Column>,

    /// Number of rows in this batch
    row_count: usize,
}

impl MutableBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            columns: Default::default(),
            row_count: 0,
        }
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        let schema = match selection {
            Selection::All => {
                for (column_name, column) in self.columns.iter() {
                    schema_builder.influx_column(column_name, column.influx_type());
                }

                schema_builder
                    .build()
                    .context(InternalSchema)?
                    .sort_fields_by_name()
            }
            Selection::Some(cols) => {
                for col in cols {
                    let column = self.column(col)?;
                    schema_builder.influx_column(col, column.influx_type());
                }
                schema_builder.build().context(InternalSchema)?
            }
        };

        Ok(schema)
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn to_arrow(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        let schema = self.schema(selection)?;
        let columns = schema
            .iter()
            .map(|(_, field)| {
                let column = self
                    .columns
                    .get(field.name())
                    .expect("schema contains non-existent column");

                column.to_arrow().context(ColumnError {
                    column: field.name(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema.into(), columns).context(ArrowError {})
    }

    /// Returns an iterator over the columns in this batch in no particular order
    pub fn columns(&self) -> impl Iterator<Item = (&String, &Column)> + '_ {
        self.columns.iter()
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.row_count
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column: &str) -> Result<&Column> {
        self.columns.get(column).context(ColumnNotFound { column })
    }

    /// Write the contents of a [`TableBatch`] into this MutableBatch.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        batch: TableBatch<'_>,
        mask: Option<&[bool]>,
    ) -> Result<()> {
        self.write_columns(batch.columns(), mask)
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    fn write_columns(
        &mut self,
        columns: Vec<entry::Column<'_>>,
        mask: Option<&[bool]>,
    ) -> Result<()> {
        let row_count_before_insert = self.rows();
        let additional_rows = columns.first().map(|x| x.row_count).unwrap_or_default();
        let masked_values = if let Some(mask) = mask {
            ensure!(
                additional_rows == mask.len(),
                IncorrectMaskLength {
                    expected: additional_rows,
                    actual: mask.len(),
                }
            );
            mask.iter().filter(|x| !*x).count()
        } else {
            0
        };
        let final_row_count = row_count_before_insert + additional_rows - masked_values;

        // get the column ids and validate schema for those that already exist
        columns.iter().try_for_each(|column| {
            ensure!(
                column.row_count == additional_rows,
                IncorrectRowCount {
                    column: column.name(),
                    expected: additional_rows,
                    actual: column.row_count,
                }
            );

            if let Some(c) = self.columns.get(column.name()) {
                c.validate_schema(column.influx_type())
                    .context(ColumnError {
                        column: column.name(),
                    })?;
            }

            Ok(())
        })?;

        for fb_column in columns {
            let column = self.column_or_insert(fb_column.name(), fb_column.influx_type());

            column.append_entry(&fb_column, mask).context(ColumnError {
                column: fb_column.name(),
            })?;

            assert_eq!(column.len(), final_row_count);
        }

        // Pad any columns that did not have values in this batch with NULLs
        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        self.row_count = final_row_count;

        Ok(())
    }

    fn validate_column_schema(&self, name: &str, influx_type: InfluxColumnType) -> Result<()> {
        if let Some(column) = self.columns.get(name) {
            column
                .validate_schema(influx_type)
                .context(ColumnError { column: name })?;
        }
        Ok(())
    }

    /// Writes the provided [`ParsedLine`] into this
    pub fn write_line(&mut self, line: ParsedLine<'_>, default_time: i64) -> Result<()> {
        let final_row_count = self.row_count + 1;

        self.validate_column_schema(TIME_COLUMN_NAME, InfluxColumnType::Timestamp)?;

        for (tag, _) in line.series.tag_set.iter().flatten() {
            self.validate_column_schema(tag, InfluxColumnType::Tag)?;
        }

        for (field, value) in &line.field_set {
            self.validate_column_schema(field, InfluxColumnType::Field(field_value_type(value)))?;
        }

        for (tag, val) in line.series.tag_set.iter().flatten() {
            self.column_or_insert(tag, InfluxColumnType::Tag)
                .append_tag(val);
        }

        for (field, value) in line.field_set {
            self.column_or_insert(&field, InfluxColumnType::Field(field_value_type(&value)))
                .append_field(value);
        }

        self.column_or_insert(TIME_COLUMN_NAME, InfluxColumnType::Timestamp)
            .append_time(line.timestamp.unwrap_or(default_time));

        // Pad any columns that did not have values in this batch with NULLs
        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        self.row_count = final_row_count;
        Ok(())
    }

    fn column_or_insert(&mut self, name: &str, influx_type: InfluxColumnType) -> &mut Column {
        let row_count = self.row_count;
        self.columns
            .raw_entry_mut()
            .from_key(name)
            .or_insert_with(|| (name.to_string(), Column::new(row_count, influx_type)))
            .1
    }

    pub fn write_pb(&mut self, batch: PbTableBatch) -> Result<()> {
        let to_add = batch.row_count;
        let final_row_count = self.row_count + batch.row_count as usize;

        // TODO: Schema validation

        for col in batch.columns {
            // TODO: error handling
            let influx_type = pb_column_type(&col).unwrap();
            let mut column = self.column_or_insert(col.column_name.as_str(), influx_type);
            column.append_pb(to_add as usize, col)
        }

        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        self.row_count = final_row_count;

        Ok(())
    }

    pub fn to_pb(&self, table_name: String) -> PbTableBatch {
        PbTableBatch {
            table_name,
            columns: self
                .columns
                .iter()
                .map(|(name, value)| value.to_pb(name.clone()))
                .collect(),
            row_count: self.row_count as u32,
        }
    }
}

fn field_value_type(value: &FieldValue<'_>) -> InfluxFieldType {
    match value {
        FieldValue::I64(_) => InfluxFieldType::Integer,
        FieldValue::U64(_) => InfluxFieldType::UInteger,
        FieldValue::F64(_) => InfluxFieldType::Float,
        FieldValue::String(_) => InfluxFieldType::String,
        FieldValue::Boolean(_) => InfluxFieldType::Boolean,
    }
}

fn pb_column_type(col: &PbColumn) -> Option<InfluxColumnType> {
    let value_type = pb_value_type(col.values.as_ref()?)?;
    let semantic_type = SemanticType::from_i32(col.semantic_type)?;

    match (semantic_type, value_type) {
        (SemanticType::Tag, InfluxFieldType::String) => Some(InfluxColumnType::Tag),
        (SemanticType::Field, field) => Some(InfluxColumnType::Field(field)),
        (SemanticType::Time, InfluxFieldType::Integer) => Some(InfluxColumnType::Timestamp),
        _ => None,
    }
}

fn pb_value_type(values: &PbValues) -> Option<InfluxFieldType> {
    if !values.string_values.is_empty() {
        return Some(InfluxFieldType::String);
    }
    if !values.i64_values.is_empty() {
        return Some(InfluxFieldType::Integer);
    }
    if !values.u64_values.is_empty() {
        return Some(InfluxFieldType::UInteger);
    }
    if !values.f64_values.is_empty() {
        return Some(InfluxFieldType::Float);
    }
    if !values.bool_values.is_empty() {
        return Some(InfluxFieldType::Boolean);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use entry::lines_to_sharded_entries;
    use entry::test_helpers::lp_to_entry;
    use influxdb_line_protocol::parse_lines;
    use schema::{InfluxColumnType, InfluxFieldType};

    #[test]
    fn write_columns_validates_schema() {
        let mut table = MutableBatch::new();
        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(lp);
        table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Tag,
                        inserted: InfluxColumnType::Field(InfluxFieldType::String)
                    }
                } if column == "t1"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo iv=1u 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        inserted: InfluxColumnType::Field(InfluxFieldType::UInteger),
                        existing: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "iv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo fv=1i 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Float),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "fv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo bv=1 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Boolean),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Float)
                    }
                } if column == "bv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo sv=true 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Boolean),
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo,sv=\"bar\" f=3i 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Tag,
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );
    }

    #[test]
    fn test_lp() {
        use data_types::database_rules::{PartitionTemplate, TemplatePart};
        use entry::test_helpers::sharder;
        use std::io::Read;

        let raw = include_bytes!("../../tests/fixtures/lineproto/read_filter.lp.gz");
        let mut gz = flate2::read::GzDecoder::new(&raw[..]);
        let mut lp = String::new();
        gz.read_to_string(&mut lp).unwrap();

        let parsed_lines = parse_lines(lp.as_str())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let entries = lines_to_sharded_entries(
            &parsed_lines,
            0,
            sharder(1).as_ref(),
            &PartitionTemplate {
                parts: vec![TemplatePart::Table],
            },
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        let entry = entries.into_iter().next().unwrap().entry;

        let mut a = MutableBatch::new();

        for (_, table) in entry.table_batches() {
            a.write_table_batch(table, None).unwrap()
        }

        let mut b = MutableBatch::new();

        for maybe_line in parsed_lines {
            b.write_line(maybe_line, 0).unwrap();
        }

        let a_schema = a.schema(Selection::All).unwrap();
        let b_schema = b.schema(Selection::All).unwrap();
        assert_eq!(a_schema, b_schema);

        let a_formatted =
            arrow_util::display::pretty_format_batches(&[a.to_arrow(Selection::All).unwrap()])
                .unwrap();

        let b_formatted =
            arrow_util::display::pretty_format_batches(&[b.to_arrow(Selection::All).unwrap()])
                .unwrap();

        assert_eq!(a_formatted, b_formatted);

        let pb = a.to_pb("my table".to_string());

        let mut c = MutableBatch::new();

        c.write_pb(pb).unwrap();

        assert_eq!(c.schema(Selection::All).unwrap(), a_schema);

        let c_formatted =
            arrow_util::display::pretty_format_batches(&[c.to_arrow(Selection::All).unwrap()])
                .unwrap();

        assert_eq!(a_formatted, c_formatted);
    }
}
