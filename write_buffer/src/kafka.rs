use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::{TryFrom, TryInto},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use data_types::{
    database_rules::WriteBufferCreationConfig, sequence::Sequence, server_id::ServerId,
};
use entry::{Entry, SequencedEntry};
use futures::{FutureExt, StreamExt};
use http::{HeaderMap, HeaderValue};
use observability_deps::tracing::{debug, info};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    error::KafkaError,
    message::{Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
    util::Timeout,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use time::{Time, TimeProvider};
use trace::{ctx::SpanContext, TraceCollector};
use trace_http::ctx::TraceHeaderParser;

use crate::core::{
    EntryStream, FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting,
};

/// Message header that determines message content type.
pub const HEADER_CONTENT_TYPE: &str = "content-type";

/// Message header for tracing context.
pub const HEADER_TRACE_CONTEXT: &str = "uber-trace-id";

/// Current flatbuffer-based content type.
///
/// This is a value for [`HEADER_CONTENT_TYPE`].
///
/// Inspired by:
/// - <https://stackoverflow.com/a/56502135>
/// - <https://stackoverflow.com/a/48051331>
pub const CONTENT_TYPE_FLATBUFFER: &str =
    r#"application/x-flatbuffers; schema="influxdata.iox.write.v1.Entry""#;

/// IOx-specific headers attached to every Kafka message.
#[derive(Debug)]
struct IoxHeaders {
    content_type: Option<String>,
    span_context: Option<SpanContext>,
}

impl IoxHeaders {
    /// Create new headers with sane default values and given span context.
    fn new(span_context: Option<SpanContext>) -> Self {
        Self {
            content_type: Some(CONTENT_TYPE_FLATBUFFER.to_string()),
            span_context,
        }
    }

    /// Create new headers where all information is missing.
    fn empty() -> Self {
        Self {
            content_type: None,
            span_context: None,
        }
    }

    /// Parse from Kafka headers.
    fn from_kafka<H>(headers: &H, trace_collector: Option<&Arc<dyn TraceCollector>>) -> Self
    where
        H: Headers,
    {
        let mut res = Self::empty();

        for i in 0..headers.count() {
            if let Some((name, value)) = headers.get(i) {
                if name.eq_ignore_ascii_case(HEADER_CONTENT_TYPE) {
                    res.content_type = String::from_utf8(value.to_vec()).ok();
                }

                if let Some(trace_collector) = trace_collector {
                    if name.eq_ignore_ascii_case(HEADER_TRACE_CONTEXT) {
                        if let Ok(header_value) = HeaderValue::from_bytes(value) {
                            let mut headers = HeaderMap::new();
                            headers.insert(HEADER_TRACE_CONTEXT, header_value);

                            let parser = TraceHeaderParser::new()
                                .with_jaeger_trace_context_header_name(HEADER_TRACE_CONTEXT);
                            res.span_context =
                                parser.parse(trace_collector, &headers).ok().flatten();
                        }
                    }
                }
            }
        }

        res
    }
}

impl From<&IoxHeaders> for OwnedHeaders {
    fn from(iox_headers: &IoxHeaders) -> Self {
        let mut res = Self::new();

        if let Some(content_type) = iox_headers.content_type.as_ref() {
            res = res.add(HEADER_CONTENT_TYPE, content_type);
        }

        if let Some(span_context) = iox_headers.span_context.as_ref() {
            res = res.add(
                HEADER_TRACE_CONTEXT,
                &format!(
                    "{:x}:{:x}:{:x}:1",
                    span_context.trace_id.get(),
                    span_context.span_id.get(),
                    span_context
                        .parent_span_id
                        .as_ref()
                        .map(|span_id| span_id.get())
                        .unwrap_or_default()
                ),
            )
        }

        res
    }
}

pub struct KafkaBufferProducer {
    conn: String,
    database_name: String,
    time_provider: Arc<dyn TimeProvider>,
    producer: FutureProducer,
    partitions: BTreeSet<u32>,
}

// Needed because rdkafka's FutureProducer doesn't impl Debug
impl std::fmt::Debug for KafkaBufferProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBufferProducer")
            .field("conn", &self.conn)
            .field("database_name", &self.database_name)
            .finish()
    }
}

#[async_trait]
impl WriteBufferWriting for KafkaBufferProducer {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.partitions.clone()
    }

    /// Send an `Entry` to Kafka using the sequencer ID as a partition.
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
        span_context: Option<&SpanContext>,
    ) -> Result<(Sequence, Time), WriteBufferError> {
        let partition = i32::try_from(sequencer_id)?;

        // truncate milliseconds from timestamps because that's what Kafka supports
        let date_time = self.time_provider.now().date_time();
        let timestamp_millis = date_time.timestamp_millis();
        let timestamp = Time::from_timestamp_millis(timestamp_millis);

        let headers = IoxHeaders::new(span_context.cloned());

        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&self.database_name)
            .payload(entry.data())
            .partition(partition)
            .timestamp(timestamp_millis)
            .headers((&headers).into());

        debug!(db_name=%self.database_name, partition, size=entry.data().len(), "writing to kafka");

        let (partition, offset) = self
            .producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(e, _owned_message)| Box::new(e))?;

        debug!(db_name=%self.database_name, %offset, %partition, size=entry.data().len(), "wrote to kafka");

        Ok((
            Sequence {
                id: partition.try_into()?,
                number: offset.try_into()?,
            },
            timestamp,
        ))
    }

    fn type_name(&self) -> &'static str {
        "kafka"
    }
}

impl KafkaBufferProducer {
    pub async fn new(
        conn: impl Into<String> + Send,
        database_name: impl Into<String> + Send,
        connection_config: &HashMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self, WriteBufferError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();

        // these configs can be overwritten
        cfg.set("message.timeout.ms", "5000");
        cfg.set("message.max.bytes", "31457280");
        cfg.set("queue.buffering.max.kbytes", "31457280");
        cfg.set("request.required.acks", "all"); // equivalent to acks=-1
        cfg.set("compression.type", "snappy");
        cfg.set("statistics.interval.ms", "15000");

        // user overrides
        for (k, v) in connection_config {
            cfg.set(k, v);
        }

        // these configs are set in stone
        cfg.set("bootstrap.servers", &conn);
        cfg.set("allow.auto.create.topics", "false");

        // handle auto-creation
        let partitions =
            maybe_auto_create_topics(&conn, &database_name, creation_config, &cfg).await?;

        let producer: FutureProducer = cfg.create()?;

        Ok(Self {
            conn,
            database_name,
            time_provider,
            producer,
            partitions,
        })
    }
}

pub struct KafkaBufferConsumer {
    conn: String,
    database_name: String,
    consumers: BTreeMap<u32, Arc<StreamConsumer>>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

// Needed because rdkafka's StreamConsumer doesn't impl Debug
impl std::fmt::Debug for KafkaBufferConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBufferConsumer")
            .field("conn", &self.conn)
            .field("database_name", &self.database_name)
            .finish()
    }
}

#[async_trait]
impl WriteBufferReading for KafkaBufferConsumer {
    fn streams(&mut self) -> BTreeMap<u32, EntryStream<'_>> {
        let mut streams = BTreeMap::new();

        for (sequencer_id, consumer) in &self.consumers {
            let sequencer_id = *sequencer_id;
            let consumer_cloned = Arc::clone(consumer);
            let database_name = self.database_name.clone();
            let trace_collector = self.trace_collector.clone();

            let stream = consumer
                .stream()
                .map(move |message| {
                    let message = message?;

                    let headers: IoxHeaders = message.headers().map(|headers| IoxHeaders::from_kafka(headers, trace_collector.as_ref())).unwrap_or_else(IoxHeaders::empty);

                    // Fallback for now https://github.com/influxdata/influxdb_iox/issues/2805
                    let content_type = headers.content_type.unwrap_or_else(|| CONTENT_TYPE_FLATBUFFER.to_string());
                    if content_type != CONTENT_TYPE_FLATBUFFER {
                        return Err(format!("Unknown message format: {}", content_type).into());
                    }

                    let payload = message.payload().ok_or_else::<WriteBufferError, _>(|| {
                        "Payload missing".to_string().into()
                    })?;
                    let entry = Entry::try_from(payload.to_vec())?;

                    // Timestamps were added as part of
                    // [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message).
                    // The tracking issue [KAFKA-2511](https://issues.apache.org/jira/browse/KAFKA-2511) states that
                    // this was completed with Kafka 0.10.0.0, for which the
                    // [release page](https://kafka.apache.org/downloads#0.10.0.0) states a release date of 2016-05-22.
                    // Also see https://stackoverflow.com/a/62936145 which also mentions that fact.
                    //
                    // So instead of making the timestamp optional throughout the stack, we just require an
                    // up-to-date Kafka stack.
                    let timestamp_millis = message.timestamp().to_millis().ok_or_else::<WriteBufferError, _>(|| {
                        "The connected Kafka does not seem to support message timestamps (KIP-32). Please upgrade to >= 0.10.0.0".to_string().into()
                    })?;

                    let timestamp = Time::from_timestamp_millis_opt(timestamp_millis).ok_or_else::<WriteBufferError, _>(|| {
                        format!("Cannot parse timestamp for milliseconds: {}", timestamp_millis).into()
                    })?;

                    let sequence = Sequence {
                        id: message.partition().try_into()?,
                        number: message.offset().try_into()?,
                    };

                    Ok(SequencedEntry::new_from_sequence_and_span_context(sequence, timestamp, entry, headers.span_context))
                })
                .boxed();

            let fetch_high_watermark = move || {
                let consumer_cloned = Arc::clone(&consumer_cloned);
                let database_name = database_name.clone();

                let fut = async move {
                    match tokio::task::spawn_blocking(move || {
                        consumer_cloned.fetch_watermarks(
                            &database_name,
                            sequencer_id as i32,
                            Duration::from_secs(60),
                        )
                    })
                    .await
                    .expect("subtask failed")
                    {
                        Ok((_low, high)) => Ok(high as u64),
                        Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownPartition)) => Ok(0),
                        Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                    }
                };

                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.insert(
                sequencer_id,
                EntryStream {
                    stream,
                    fetch_high_watermark,
                },
            );
        }

        streams
    }

    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        if let Some(consumer) = self.consumers.get(&sequencer_id) {
            let consumer = Arc::clone(consumer);
            let database_name = self.database_name.clone();
            let offset = if sequence_number > 0 {
                Offset::Offset(sequence_number as i64)
            } else {
                Offset::Beginning
            };

            tokio::task::spawn_blocking(move || {
                consumer.seek(
                    &database_name,
                    sequencer_id as i32,
                    offset,
                    Duration::from_secs(60),
                )
            })
            .await
            .expect("subtask failed")?;
        }

        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "kafka"
    }
}

impl KafkaBufferConsumer {
    pub async fn new(
        conn: impl Into<String> + Send + Sync,
        server_id: ServerId,
        database_name: impl Into<String> + Send + Sync,
        connection_config: &HashMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        // `trace_collector` has to be a reference due to https://github.com/rust-lang/rust/issues/63033
        trace_collector: Option<&Arc<dyn TraceCollector>>,
    ) -> Result<Self, WriteBufferError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();

        // these configs can be overwritten
        cfg.set("session.timeout.ms", "6000");
        cfg.set("statistics.interval.ms", "15000");
        cfg.set("queued.max.messages.kbytes", "10000");

        // user overrides
        for (k, v) in connection_config {
            cfg.set(k, v);
        }

        // these configs are set in stone
        cfg.set("bootstrap.servers", &conn);
        cfg.set("enable.auto.commit", "false");
        cfg.set("allow.auto.create.topics", "false");

        // Create a unique group ID for this database's consumer as we don't want to create
        // consumer groups.
        cfg.set("group.id", &format!("{}-{}", server_id, database_name));

        // When subscribing without a partition offset, start from the smallest offset available.
        cfg.set("auto.offset.reset", "smallest");

        // figure out which partitions exists
        let partitions =
            maybe_auto_create_topics(&conn, &database_name, creation_config, &cfg).await?;
        info!(%database_name, ?partitions, "found Kafka partitions");

        // setup a single consumer per partition, at least until https://github.com/fede1024/rust-rdkafka/pull/351 is
        // merged
        let consumers = partitions
            .into_iter()
            .map(|partition| {
                let consumer: StreamConsumer = cfg.create()?;

                let mut assignment = TopicPartitionList::new();
                assignment.add_partition(&database_name, partition as i32);

                // We must set the offset to `Beginning` here to avoid the following error during seek:
                //     KafkaError (Seek error: Local: Erroneous state)
                //
                // Also see:
                // - https://github.com/Blizzard/node-rdkafka/issues/237
                // - https://github.com/confluentinc/confluent-kafka-go/issues/121#issuecomment-362308376
                assignment
                    .set_partition_offset(&database_name, partition as i32, Offset::Beginning)
                    .expect("partition was set just before");

                consumer.assign(&assignment)?;
                Ok((partition, Arc::new(consumer)))
            })
            .collect::<Result<BTreeMap<u32, Arc<StreamConsumer>>, KafkaError>>()?;

        Ok(Self {
            conn,
            database_name,
            consumers,
            trace_collector: trace_collector.map(|x| Arc::clone(x)),
        })
    }
}

async fn get_partitions(
    database_name: &str,
    cfg: &ClientConfig,
) -> Result<BTreeSet<u32>, KafkaError> {
    let database_name = database_name.to_string();
    let cfg = cfg.clone();

    let metadata = tokio::task::spawn_blocking(move || {
        let probe_consumer: BaseConsumer = cfg.create()?;

        probe_consumer.fetch_metadata(Some(&database_name), Duration::from_secs(60))
    })
    .await
    .expect("subtask failed")?;

    let topic_metadata = metadata.topics().get(0).expect("requested a single topic");

    let partitions: BTreeSet<_> = topic_metadata
        .partitions()
        .iter()
        .map(|partition_metdata| partition_metdata.id().try_into().unwrap())
        .collect();

    Ok(partitions)
}

fn admin_client(kafka_connection: &str) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);
    cfg.set("message.timeout.ms", "5000");
    cfg.create()
}

/// Create Kafka topic based on the provided configs.
///
/// This is create a topic with `n_sequencers` partitions.
///
/// This will NOT fail if the topic already exists!
async fn create_kafka_topic(
    kafka_connection: &str,
    database_name: &str,
    n_sequencers: NonZeroU32,
    cfg: &HashMap<String, String>,
) -> Result<(), WriteBufferError> {
    let admin = admin_client(kafka_connection)?;

    let mut topic = NewTopic::new(
        database_name,
        n_sequencers.get() as i32,
        TopicReplication::Fixed(1),
    );
    for (k, v) in cfg {
        topic = topic.set(k, v);
    }

    let opts = AdminOptions::default();
    let mut results = admin.create_topics([&topic], &opts).await?;
    assert_eq!(results.len(), 1, "created exactly one topic");
    let result = results.pop().expect("just checked the vector length");
    match result {
        Ok(topic) | Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
            assert_eq!(topic, database_name);
            Ok(())
        }
        Err((topic, code)) => {
            assert_eq!(topic, database_name);
            Err(format!("Cannot create topic '{}': {}", topic, code).into())
        }
    }
}

async fn maybe_auto_create_topics(
    kafka_connection: &str,
    database_name: &str,
    creation_config: Option<&WriteBufferCreationConfig>,
    cfg: &ClientConfig,
) -> Result<BTreeSet<u32>, WriteBufferError> {
    let mut partitions = get_partitions(database_name, cfg).await?;
    if partitions.is_empty() {
        if let Some(creation_config) = creation_config {
            create_kafka_topic(
                kafka_connection,
                database_name,
                creation_config.n_sequencers,
                &creation_config.options,
            )
            .await?;
            partitions = get_partitions(database_name, cfg).await?;

            // while the number of partitions might be different than `creation_cfg.n_sequencers` due to a
            // conflicting, concurrent topic creation, it must not be empty at this point
            if partitions.is_empty() {
                return Err("Cannot create non-empty topic".to_string().into());
            }
        } else {
            return Err("no partitions found and auto-creation not requested"
                .to_string()
                .into());
        }
    }

    Ok(partitions)
}

pub mod test_utils {
    use std::{collections::HashMap, time::Duration};

    use rdkafka::admin::{AdminOptions, AlterConfig, ResourceSpecifier};
    use uuid::Uuid;

    use super::admin_client;

    /// Get the testing Kafka connection string or return current scope.
    ///
    /// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
    /// caller.
    ///
    /// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
    /// guidance for setting `KAFKA_CONNECTION`.
    ///
    /// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
    #[macro_export]
    macro_rules! maybe_skip_kafka_integration {
        () => {{
            use std::env;
            dotenv::dotenv().ok();

            match (
                env::var("TEST_INTEGRATION").is_ok(),
                env::var("KAFKA_CONNECT").ok(),
            ) {
                (true, Some(kafka_connection)) => kafka_connection,
                (true, None) => {
                    panic!(
                        "TEST_INTEGRATION is set which requires running integration tests, but \
                        KAFKA_CONNECT is not set. Please run Kafka, perhaps by using the command \
                        `docker-compose -f docker/ci-kafka-docker-compose.yml up kafka`, then \
                        set KAFKA_CONNECT to the host and port where Kafka is accessible. If \
                        running the `docker-compose` command and the Rust tests on the host, the \
                        value for `KAFKA_CONNECT` should be `localhost:9093`. If running the Rust \
                        tests in another container in the `docker-compose` network as on CI, \
                        `KAFKA_CONNECT` should be `kafka:9092`."
                    )
                }
                (false, Some(_)) => {
                    eprintln!("skipping Kafka integration tests - set TEST_INTEGRATION to run");
                    return;
                }
                (false, None) => {
                    eprintln!(
                        "skipping Kafka integration tests - set TEST_INTEGRATION and KAFKA_CONNECT to \
                        run"
                    );
                    return;
                }
            }
        }};
    }

    /// Create topic creation config that is ideal for testing and works with [`purge_kafka_topic`]
    pub fn kafka_sequencer_options() -> HashMap<String, String> {
        let mut cfg: HashMap<String, String> = Default::default();
        cfg.insert("cleanup.policy".to_string(), "delete".to_string());
        cfg.insert("retention.ms".to_string(), "-1".to_string());
        cfg.insert("segment.ms".to_string(), "10".to_string());
        cfg
    }

    /// Purge all records from given topic.
    ///
    /// **WARNING: Until <https://github.com/fede1024/rust-rdkafka/issues/385> is fixed, this requires a server-wide
    ///            `log.retention.check.interval.ms` of 100ms!**
    pub async fn purge_kafka_topic(kafka_connection: &str, database_name: &str) {
        let admin = admin_client(kafka_connection).unwrap();
        let opts = AdminOptions::default();

        let cfg =
            AlterConfig::new(ResourceSpecifier::Topic(database_name)).set("retention.ms", "1");
        admin.alter_configs([&cfg], &opts).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let cfg =
            AlterConfig::new(ResourceSpecifier::Topic(database_name)).set("retention.ms", "-1");
        let mut results = admin.alter_configs([&cfg], &opts).await.unwrap();
        assert_eq!(results.len(), 1, "created exactly one topic");
        let result = results.pop().expect("just checked the vector length");
        result.unwrap();
    }

    /// Generated random topic name for testing.
    pub fn random_kafka_topic() -> String {
        format!("test_topic_{}", Uuid::new_v4())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU32,
        sync::atomic::{AtomicU32, Ordering},
    };

    use entry::test_helpers::lp_to_entry;
    use time::TimeProvider;
    use trace::{RingBufferTraceCollector, TraceCollector};

    use crate::{
        core::test_utils::{
            assert_span_context_eq, map_pop_first, perform_generic_tests, set_pop_first,
            TestAdapter, TestContext,
        },
        kafka::test_utils::random_kafka_topic,
        maybe_skip_kafka_integration,
    };

    use super::{test_utils::kafka_sequencer_options, *};

    struct KafkaTestAdapter {
        conn: String,
    }

    impl KafkaTestAdapter {
        fn new(conn: String) -> Self {
            Self { conn }
        }
    }

    #[async_trait]
    impl TestAdapter for KafkaTestAdapter {
        type Context = KafkaTestContext;

        async fn new_context_with_time(
            &self,
            n_sequencers: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            KafkaTestContext {
                conn: self.conn.clone(),
                database_name: random_kafka_topic(),
                server_id_counter: AtomicU32::new(1),
                n_sequencers,
                time_provider,
            }
        }
    }

    struct KafkaTestContext {
        conn: String,
        database_name: String,
        server_id_counter: AtomicU32,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
    }

    impl KafkaTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_sequencers: self.n_sequencers,
                options: kafka_sequencer_options(),
            })
        }
    }

    #[async_trait]
    impl TestContext for KafkaTestContext {
        type Writing = KafkaBufferProducer;

        type Reading = KafkaBufferConsumer;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            KafkaBufferProducer::new(
                &self.conn,
                &self.database_name,
                &Default::default(),
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            let server_id = self.server_id_counter.fetch_add(1, Ordering::SeqCst);
            let server_id = ServerId::try_from(server_id).unwrap();

            let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

            KafkaBufferConsumer::new(
                &self.conn,
                server_id,
                &self.database_name,
                &Default::default(),
                self.creation_config(creation_config).as_ref(),
                Some(&collector),
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_generic() {
        let conn = maybe_skip_kafka_integration!();

        perform_generic_tests(KafkaTestAdapter::new(conn)).await;
    }

    #[tokio::test]
    async fn topic_create_twice() {
        let conn = maybe_skip_kafka_integration!();
        let database_name = random_kafka_topic();

        create_kafka_topic(
            &conn,
            &database_name,
            NonZeroU32::try_from(1).unwrap(),
            &kafka_sequencer_options(),
        )
        .await
        .unwrap();

        create_kafka_topic(
            &conn,
            &database_name,
            NonZeroU32::try_from(1).unwrap(),
            &kafka_sequencer_options(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn error_no_payload() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let record: FutureRecord<'_, String, [u8]> =
            FutureRecord::to(&writer.database_name).partition(partition);
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        let err = stream.stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "Payload missing");
    }

    #[tokio::test]
    async fn content_type_header_missing() {
        // Fallback for now https://github.com/influxdata/influxdb_iox/issues/2805
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let entry = lp_to_entry("upc,region=east user=1 100");
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&writer.database_name)
            .payload(entry.data())
            .partition(partition);
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        stream.stream.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn content_type_header_unknown() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let entry = lp_to_entry("upc,region=east user=1 100");
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&writer.database_name)
            .payload(entry.data())
            .partition(partition)
            .headers(OwnedHeaders::new().add(HEADER_CONTENT_TYPE, "foo"));
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        let err = stream.stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "Unknown message format: foo");
    }

    #[test]
    fn headers_roundtrip() {
        let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

        let span_context_parent = SpanContext::new(Arc::clone(&collector));
        let span_context = span_context_parent.child("foo").ctx;

        let iox_headers1 = IoxHeaders::new(Some(span_context));

        let kafka_headers: OwnedHeaders = (&iox_headers1).into();
        let iox_headers2 = IoxHeaders::from_kafka(&kafka_headers, Some(&collector));

        assert_eq!(iox_headers1.content_type, iox_headers2.content_type);
        assert_span_context_eq(
            iox_headers1.span_context.as_ref().unwrap(),
            iox_headers2.span_context.as_ref().unwrap(),
        );
    }

    #[test]
    fn headers_case_handling() {
        let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

        let kafka_headers = OwnedHeaders::new()
            .add("content-type", "a")
            .add("CONTENT-TYPE", "b")
            .add("content-TYPE", "c")
            .add("uber-trace-id", "1:2:3:1")
            .add("uber-trace-ID", "10:20:30:1");

        let actual = IoxHeaders::from_kafka(&kafka_headers, Some(&collector));
        assert_eq!(actual.content_type, Some("c".to_string()));
        assert_eq!(actual.span_context.unwrap().span_id.get(), 32);
    }

    #[test]
    fn headers_no_trace_collector_on_consumer_side() {
        let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

        let span_context = SpanContext::new(Arc::clone(&collector));

        let iox_headers1 = IoxHeaders::new(Some(span_context));

        let kafka_headers: OwnedHeaders = (&iox_headers1).into();
        let iox_headers2 = IoxHeaders::from_kafka(&kafka_headers, None);

        assert!(iox_headers2.span_context.is_none());
    }
}
