#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::{
    arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch},
    logical_plan::{binary_expr, col, lit, Expr, Operator},
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
    scalar::ScalarValue,
};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;

/// Traits to help creating DataFusion [`Expr`]s
pub trait AsExpr {
    /// Creates a DataFusion expr
    fn as_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn as_sort_expr(&self) -> Expr {
        Expr::Sort {
            expr: Box::new(self.as_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        }
    }
}

impl AsExpr for Arc<str> {
    fn as_expr(&self) -> Expr {
        col(self.as_ref())
    }
}

impl AsExpr for str {
    fn as_expr(&self) -> Expr {
        col(self)
    }
}

impl AsExpr for Expr {
    fn as_expr(&self) -> Expr {
        self.clone()
    }
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    // We need to cast the start and end values to timestamps
    // the equivalent of:
    let ts_start = ScalarValue::TimestampNanosecond(Some(start), None);
    let ts_end = ScalarValue::TimestampNanosecond(Some(end), None);

    let ts_low = lit(ts_start).lt_eq(col(time.as_ref()));
    let ts_high = col(time.as_ref()).lt(lit(ts_end));

    ts_low.and(ts_high)
}

/// Creates a single expression representing the conjunction (aka
/// AND'ing) together of a set of expressions
#[derive(Debug, Default)]
pub struct AndExprBuilder {
    cur_expr: Option<Expr>,
}

impl AndExprBuilder {
    /// append `new_expr` to the expression chain being built
    pub fn append_opt_ref(self, new_expr: Option<&Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr.clone()),
        }
    }

    /// append `new_expr` to the expression chain being built
    pub fn append_opt(self, new_expr: Option<Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr),
        }
    }

    /// Append `new_expr` to the expression chain being built
    pub fn append_expr(self, new_expr: Expr) -> Self {
        let Self { cur_expr } = self;

        let cur_expr = if let Some(cur_expr) = cur_expr {
            binary_expr(cur_expr, Operator::And, new_expr)
        } else {
            new_expr
        };

        let cur_expr = Some(cur_expr);

        Self { cur_expr }
    }

    /// Creates the new filter expression, consuming Self
    pub fn build(self) -> Option<Expr> {
        self.cur_expr
    }
}

/// A RecordBatchStream created from in-memory RecordBatches.
#[derive(Debug)]
pub struct MemoryStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemoryStream {
    /// Create new stream.
    ///
    /// Must at least pass one record batch!
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        assert!(!batches.is_empty(), "must at least pass one record batch");
        Self {
            schema: batches[0].schema(),
            batches,
        }
    }

    /// Create new stream with provided schema.
    pub fn new_with_schema(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self { schema, batches }
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl futures::Stream for MemoryStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(self.batches.remove(0))))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.batches.len(), Some(self.batches.len()))
    }
}

#[derive(Debug)]
/// Implements a [`SendableRecordBatchStream`] to help create DataFusion outputs from tokio channels.
///
/// It sends streams of RecordBatches from a tokio channel *and*
/// crucially knows up front the schema each batch will have be used.
pub struct AdapterStream {
    /// Schema
    schema: SchemaRef,
    /// channel for getting deduplicated batches
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl AdapterStream {
    /// Create a new stream which wraps the `inner` channel which
    /// produces [`RecordBatch`]es that each have the specified schema
    ///
    /// Not called `new` because it returns a pinned reference rather than the object itself
    pub fn adapt(
        schema: SchemaRef,
        rx: Receiver<ArrowResult<RecordBatch>>,
    ) -> SendableRecordBatchStream {
        let inner = ReceiverStream::new(rx);
        Box::pin(Self { schema, inner })
    }
}

impl Stream for AdapterStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for AdapterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Create a SendableRecordBatchStream a RecordBatch
pub fn stream_from_batch(batch: RecordBatch) -> SendableRecordBatchStream {
    stream_from_batches(vec![Arc::new(batch)])
}

/// Create a SendableRecordBatchStream from Vec of RecordBatches with the same schema
pub fn stream_from_batches(batches: Vec<Arc<RecordBatch>>) -> SendableRecordBatchStream {
    let dummy_metrics = ExecutionPlanMetricsSet::new();
    let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
    let stream = SizedRecordBatchStream::new(batches[0].schema(), batches, mem_metrics);
    Box::pin(stream)
}

/// Create a SendableRecordBatchStream that sends back no RecordBatches with a specific schema
pub fn stream_from_schema(schema: SchemaRef) -> SendableRecordBatchStream {
    let dummy_metrics = ExecutionPlanMetricsSet::new();
    let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
    let stream = SizedRecordBatchStream::new(schema, vec![], mem_metrics);
    Box::pin(stream)
}

/// Execute the [ExecutionPlan] with a default [RuntimeEnv] and
/// collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap());
    collect(plan, runtime).await.unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [RuntimeEnv] returning the resulting stream.
///
/// # Panics
/// If an an error occurs
pub async fn test_execute_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> SendableRecordBatchStream {
    let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap());
    plan.execute(partition, runtime).await.unwrap()
}

/// Execute the specified partition of the [ExecutionPlan] with a
/// default [RuntimeEnv] and collect the results in memory.
///
/// # Panics
/// If an an error occurs
pub async fn test_collect_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Vec<RecordBatch> {
    let stream = test_execute_partition(plan, partition).await;
    datafusion::physical_plan::common::collect(stream)
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let ts_predicate_expr = make_range_expr(101, 202, "time");
        let expected_string =
            "TimestampNanosecond(101, None) <= #time AND #time < TimestampNanosecond(202, None)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }
}
