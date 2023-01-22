//! Read parquet files in parallel from the Object Store by using the RangedAsyncReader.
use futures::future::BoxFuture;
use object_store::{path::Path, ObjectStore};
use range_reader::{RangeOutput, RangedAsyncReader};
use std::sync::Arc;
use tracing;

use arrow2::error::Error as ArrowError;
use arrow2::io::parquet::read;

type MyResult<T> = Result<T, String>;

#[tracing::instrument]
pub async fn parallel_read(
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    row_group: usize,
) -> MyResult<()> {
    // at least 4kb per s3 request. Adjust to your liking.
    let min_request_size = 4 * 1024;
    let length = object_store
        .head(&path)
        .await
        .map_err(|e| format!("Error in head() {}", e))?
        .size;

    // this maps a range (start, length) to a s3 ranged-bytes request
    let range_get = Box::new(move |start: u64, length: usize| {
        let object_store = object_store.clone();
        let path = path.clone();
        Box::pin(async move {
            let object_store = object_store.clone();
            let path = path.clone();
            // to get a sense of what is being queried in s3
            println!("getting {} bytes starting at {}", length, start);
            let mut data = object_store
                .get_range(&path, start as usize..(start as usize + length))
                .await
                .map_err(|x| std::io::Error::new(std::io::ErrorKind::Other, x.to_string()))?;
            println!("got {}/{} bytes starting at {}", data.len(), length, start);
            data.truncate(length);
            let as_vec = data.to_vec();
            Ok(RangeOutput {
                start,
                data: as_vec,
            })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // this factory is used to create multiple http requests concurrently. Each
    // task will get its own `RangedAsyncReader`.
    let reader_factory = || {
        let range_get = range_get.clone();
        Box::pin(futures::future::ready(Ok(RangedAsyncReader::new(
            length,
            min_request_size,
            range_get,
        )))) as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
    };

    // we need one reader to read the files' metadata
    let mut reader = reader_factory()
        .await
        .map_err(|e| format!("reader_factory {}", e))?;

    let metadata = read::read_metadata_async(&mut reader)
        .await
        .map_err(|e| format!("read metadata async {}", e))?;

    let schema =
        read::infer_schema(&metadata).map_err(|e| format!("infer schema {}", e))?;

    println!("total number of rows: {}", metadata.num_rows);
    println!("Infered Arrow schema: {:#?}", schema);

    // let's read the first row group only. Iterate over them to your liking
    let group = &metadata.row_groups[row_group];

    // no chunk size in deserializing
    let chunk_size = None;

    // this is IO-bounded (and issues a join, thus the reader_factory)
    let column_chunks =
        read::read_columns_many_async(reader_factory, group, schema.fields, chunk_size, None, None)
            .await
            .map_err(|e| format!("read_columns_many_async {}", e))?;

    // this is CPU-bounded and should be sent to a separate thread-pool.
    // We do it here for simplicity
    let chunks = read::RowGroupDeserializer::new(column_chunks, group.num_rows(), None);
    let chunks = chunks
        .collect::<Result<Vec<_>, ArrowError>>()
        .map_err(|e| format!("row group deserializer {}", e))?;

    // this is a single chunk because chunk_size is `None`
    let chunk = &chunks[0];

    let array = &chunk.arrays()[0];
    // ... and have fun with it.
    println!("len: {}", array.len());
    println!("null_count: {}", array.null_count());

    Ok(())
}
