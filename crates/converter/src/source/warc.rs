use std::{
    io::{BufRead, BufReader, Read},
    sync::{Arc, LazyLock},
};

use arrow::{
    array::{
        ArrayRef, BinaryViewBuilder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use async_trait::async_trait;
use flate2::read::MultiGzDecoder;
use futures::stream;
use lance::deps::datafusion::physical_plan::{
    SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
};
use tokio::sync::mpsc;
use tokio_util::io::SyncIoBridge;
use warc::{Record, StreamingBody, WarcHeader, WarcReader};

use super::Source;
use crate::{ConversionError, storage, validation};

const BATCH_SIZE: usize = 8_192;
const MAX_BATCH_BODY_BYTES: usize = 16 * 1024 * 1024;
const READ_BUFFER_SIZE: usize = 1024 * 1024;
const BODY_COLUMN_INDEX: usize = 19;

static WARC_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("content_length", DataType::UInt64, false),
        Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("type", DataType::Utf8, false),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("concurrent_to", DataType::Utf8, true),
        Field::new("block_digest", DataType::Utf8, true),
        Field::new("payload_digest", DataType::Utf8, true),
        Field::new("ip_address", DataType::Utf8, true),
        Field::new("refers_to", DataType::Utf8, true),
        Field::new("target_uri", DataType::Utf8, true),
        Field::new("truncated", DataType::Utf8, true),
        Field::new("warc_info_id", DataType::Utf8, true),
        Field::new("filename", DataType::Utf8, true),
        Field::new("profile", DataType::Utf8, true),
        Field::new("identified_payload_type", DataType::Utf8, true),
        Field::new("segment_number", DataType::UInt64, true),
        Field::new("segment_origin_id", DataType::Utf8, true),
        Field::new("segment_total_length", DataType::UInt64, true),
        Field::new("body", DataType::BinaryView, false),
    ]))
});

pub(super) struct WarcSource {
    uri: String,
}

impl WarcSource {
    pub(super) fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_owned(),
        }
    }

    pub(super) async fn schema(uri: &str) -> Result<SchemaRef, ConversionError> {
        verify_file(uri).await?;
        let schema = warc_schema();
        validation::validate_schema(schema.fields())?;
        Ok(schema)
    }
}

#[async_trait]
impl Source for WarcSource {
    async fn into_stream(self: Box<Self>) -> Result<SendableRecordBatchStream, ConversionError> {
        let schema = Self::schema(&self.uri).await?;
        let storage = storage::open(&self.uri)?;
        let gzipped = storage.path().to_ascii_lowercase().ends_with(".gz");
        open_sequential(storage, gzipped, schema).await
    }
}

fn warc_schema() -> SchemaRef {
    Arc::clone(&WARC_SCHEMA)
}

async fn verify_file(uri: &str) -> Result<(), ConversionError> {
    if !storage::open(uri)?.metadata().await?.is_file {
        return Err(ConversionError::InvalidSource(
            "WARC source must be a file".to_owned(),
        ));
    }
    Ok(())
}

async fn open_sequential(
    storage: storage::StorageRef,
    gzipped: bool,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream, ConversionError> {
    let reader = storage.reader().await?;
    let runtime = tokio::runtime::Handle::current();
    let (sender, receiver) = mpsc::channel(1);
    let stream_schema = Arc::clone(&schema);
    tokio::task::spawn_blocking(move || {
        let result = ConversionError::catch_panic("WARC parser", || {
            let reader = SyncIoBridge::new_with_handle(reader, runtime);
            stream_records(buffered_reader(reader, gzipped), stream_schema, |batch| {
                Ok(sender.blocking_send(Ok(batch.finish()?)).is_ok())
            })
        });
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(error.into()));
        }
    });
    let batches = stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|batch| (batch, receiver))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

fn buffered_reader<R: Read + 'static>(reader: R, gzipped: bool) -> Box<dyn BufRead> {
    let reader = BufReader::with_capacity(READ_BUFFER_SIZE, reader);
    if gzipped {
        Box::new(BufReader::new(MultiGzDecoder::new(reader)))
    } else {
        Box::new(reader)
    }
}

fn stream_records(
    reader: Box<dyn BufRead>,
    schema: SchemaRef,
    mut emit: impl FnMut(WarcBatchBuilder) -> Result<bool, ConversionError>,
) -> Result<(), ConversionError> {
    let mut reader = WarcReader::new(reader);
    let mut batch = WarcBatchBuilder::new(schema);
    let mut records = reader.stream_records();
    while let Some(record) = records.next_item() {
        let record =
            record.map_err(|error| ConversionError::Read(format!("WARC parse error: {error}")))?;
        batch.append_metadata(&record)?;
        let body = record
            .into_buffered()
            .map_err(|error| ConversionError::Read(format!("WARC body error: {error}")))?
            .into_raw_parts()
            .1;
        batch.append_body(body)?;
        batch.len += 1;
        if (batch.len == BATCH_SIZE || batch.body_bytes >= MAX_BATCH_BODY_BYTES)
            && !emit(batch.take_empty())?
        {
            return Ok(());
        }
    }
    if !batch.is_empty() {
        emit(batch)?;
    }
    Ok(())
}

fn optional_u64(value: Option<String>, name: &str) -> Result<Option<u64>, ConversionError> {
    value
        .map(|value| {
            value.parse().map_err(|_| {
                ConversionError::InvalidSource(format!("invalid WARC {name}: {value}"))
            })
        })
        .transpose()
}

fn optional_header<R: Read>(
    record: &Record<StreamingBody<'_, R>>,
    header: WarcHeader,
) -> Option<String> {
    record.header(header).map(std::borrow::Cow::into_owned)
}

fn string_header(index: usize) -> Option<WarcHeader> {
    match index {
        4 => Some(WarcHeader::ContentType),
        5 => Some(WarcHeader::ConcurrentTo),
        6 => Some(WarcHeader::BlockDigest),
        7 => Some(WarcHeader::PayloadDigest),
        8 => Some(WarcHeader::IPAddress),
        9 => Some(WarcHeader::RefersTo),
        10 => Some(WarcHeader::TargetURI),
        11 => Some(WarcHeader::Truncated),
        12 => Some(WarcHeader::WarcInfoID),
        13 => Some(WarcHeader::Filename),
        14 => Some(WarcHeader::Profile),
        15 => Some(WarcHeader::IdentifiedPayloadType),
        17 => Some(WarcHeader::SegmentOriginID),
        _ => None,
    }
}

enum ColumnBuilder {
    Utf8(StringBuilder),
    UInt64(UInt64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Body(BinaryViewBuilder),
}

impl ColumnBuilder {
    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::UInt64(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
            Self::Body(mut builder) => Arc::new(builder.finish()),
        }
    }
}

struct WarcBatchBuilder {
    len: usize,
    body_bytes: usize,
    schema: SchemaRef,
    columns: Vec<ColumnBuilder>,
}

impl WarcBatchBuilder {
    fn new(schema: SchemaRef) -> Self {
        let columns = (0..schema.fields().len())
            .map(|index| match index {
                index if matches!(index, 0 | 3) || string_header(index).is_some() => {
                    ColumnBuilder::Utf8(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16))
                }
                1 | 16 | 18 => ColumnBuilder::UInt64(UInt64Builder::with_capacity(BATCH_SIZE)),
                2 => {
                    ColumnBuilder::Timestamp(TimestampMillisecondBuilder::with_capacity(BATCH_SIZE))
                }
                BODY_COLUMN_INDEX => {
                    ColumnBuilder::Body(BinaryViewBuilder::with_capacity(BATCH_SIZE))
                }
                _ => unreachable!("WARC schema and builders must stay aligned"),
            })
            .collect();
        Self {
            len: 0,
            body_bytes: 0,
            schema,
            columns,
        }
    }

    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn take_empty(&mut self) -> Self {
        let replacement = Self::new(Arc::clone(&self.schema));
        std::mem::replace(self, replacement)
    }

    fn append_metadata<R: Read>(
        &mut self,
        record: &Record<StreamingBody<'_, R>>,
    ) -> Result<(), ConversionError> {
        for (index, builder) in self.columns.iter_mut().enumerate() {
            match (index, builder) {
                (0, ColumnBuilder::Utf8(builder)) => builder.append_value(record.warc_id()),
                (1, ColumnBuilder::UInt64(builder)) => {
                    builder.append_value(record.content_length());
                }
                (2, ColumnBuilder::Timestamp(builder)) => {
                    builder.append_value(record.date().timestamp_millis());
                }
                (3, ColumnBuilder::Utf8(builder)) => {
                    builder.append_value(record.warc_type().to_string());
                }
                (16, ColumnBuilder::UInt64(builder)) => builder.append_option(optional_u64(
                    optional_header(record, WarcHeader::SegmentNumber),
                    "segment number",
                )?),
                (18, ColumnBuilder::UInt64(builder)) => builder.append_option(optional_u64(
                    optional_header(record, WarcHeader::SegmentTotalLength),
                    "segment total length",
                )?),
                (BODY_COLUMN_INDEX, ColumnBuilder::Body(_)) => {}
                (index, ColumnBuilder::Utf8(builder)) => {
                    builder.append_option(optional_header(
                        record,
                        string_header(index).expect("string WARC column has a header"),
                    ));
                }
                _ => unreachable!("WARC schema and builders must stay aligned"),
            }
        }
        Ok(())
    }

    fn append_body(&mut self, body: Vec<u8>) -> Result<(), ConversionError> {
        let body_len = u32::try_from(body.len())
            .map_err(|error| ConversionError::Read(format!("WARC body is too large: {error}")))?;
        let ColumnBuilder::Body(builder) = &mut self.columns[BODY_COLUMN_INDEX] else {
            unreachable!("WARC body builder must stay aligned with its schema");
        };
        let block = builder.append_block(body.into());
        builder
            .try_append_view(block, 0, body_len)
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        self.body_bytes += body_len as usize;
        Ok(())
    }

    fn finish(self) -> Result<RecordBatch, ConversionError> {
        let columns = self
            .columns
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect();
        RecordBatch::try_new(self.schema, columns)
            .map_err(|error| ConversionError::Read(error.to_string()))
    }
}
