use std::sync::Arc;

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};
use metric::{Metric, U64Histogram, U64HistogramOptions};
use time::{SystemProvider, TimeProvider};
use trace::ctx::SpanContext;

use super::DmlHandler;

/// An instrumentation decorator recording call latencies for [`DmlHandler`]
/// implementations.
///
/// Metrics are broken down by operation (write/delete) and result
/// (success/error) with call latency reported in milliseconds.
///
/// # Chained / Nested Handlers
///
/// Because [`DmlHandler`] implementations are constructed as a chain of
/// decorators to build up a full request handling pipeline, the reported call
/// latency of a given handler is a cumulative measure of the execution time for
/// handler and all of its children.
#[derive(Debug)]
pub struct InstrumentationDecorator<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    write_success: U64Histogram,
    write_error: U64Histogram,

    delete_success: U64Histogram,
    delete_error: U64Histogram,
}

impl<T> InstrumentationDecorator<T> {
    /// Wrap a new [`InstrumentationDecorator`] over `T` exposing metrics
    /// labelled with `handler=name`.
    pub fn new(name: &'static str, registry: Arc<metric::Registry>, inner: T) -> Self {
        let buckets = || {
            U64HistogramOptions::new([5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, u64::MAX])
        };

        let write: Metric<U64Histogram> = registry.register_metric_with_options(
            "dml_handler_write_duration_ms",
            "write handler call duration in milliseconds",
            buckets,
        );
        let delete: Metric<U64Histogram> = registry.register_metric_with_options(
            "dml_handler_delete_duration_ms",
            "delete handler call duration in milliseconds",
            buckets,
        );

        let write_success = write.recorder(&[("handler", name), ("result", "success")]);
        let write_error = write.recorder(&[("handler", name), ("result", "error")]);

        let delete_success = delete.recorder(&[("handler", name), ("result", "success")]);
        let delete_error = delete.recorder(&[("handler", name), ("result", "error")]);

        Self {
            inner,
            time_provider: Default::default(),
            write_success,
            write_error,
            delete_success,
            delete_error,
        }
    }
}

#[async_trait]
impl<T> DmlHandler for InstrumentationDecorator<T>
where
    T: DmlHandler,
{
    type WriteInput = T::WriteInput;
    type WriteError = T::WriteError;
    type DeleteError = T::DeleteError;

    /// Call the inner `write` method and record the call latency.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        let t = self.time_provider.now();
        let res = self.inner.write(namespace, input, span_ctx).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.write_success.record(delta.as_millis() as _),
                Err(_) => self.write_error.record(delta.as_millis() as _),
            };
        }

        res
    }

    /// Call the inner `delete` method and record the call latency.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        let t = self.time_provider.now();
        let res = self
            .inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.delete_success.record(delta.as_millis() as _),
                Err(_) => self.delete_error.record(delta.as_millis() as _),
            };
        }

        res
    }
}
