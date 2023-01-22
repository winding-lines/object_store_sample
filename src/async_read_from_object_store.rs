//! Read parquet files in parallel from the Object Store without a third party crate.
use futures::executor::block_on;
use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::{AsyncRead, AsyncSeek, Future, TryFutureExt};
use object_store::{path::Path, ObjectStore};
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use arrow2::error::Error as ArrowError;
use arrow2::io::parquet::read;

type MyResult<T> = Result<T, String>;
type MyFuture = BoxFuture<'static, std::io::Result<Vec<u8>>>;

pub struct AsyncCloudObject {
    pos: u64,
    length: u64, // total size
    object_store: Arc<Mutex<dyn ObjectStore>>,
    path: Path,
    active: Arc<Mutex<Option<MyFuture>>>,
}

impl AsyncCloudObject {
    pub fn new(length: usize, object_store: Arc<Mutex<dyn ObjectStore>>, path: Path) -> Self {
        let length = length as u64;
        Self {
            pos: 0,
            length,
            object_store,
            path,
            active: Arc::new(Mutex::new(None)),
        }
    }

    async fn create_future_once(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        lenght: usize,
    ) -> std::task::Poll<std::io::Result<Vec<u8>>> {
        let start = self.pos as usize;
        println!("create_future_once: {} {}.", lenght, start);

        // If we already have a future just poll it.
        if let Some(fut) = self.active.lock().await.as_mut() {
            println!("create_future_once: pre-existing future, polling.");
            return Future::poll(fut.as_mut(), cx);
        }

        // Create the future.
        let future = {
            let path = self.path.clone();
            let arc = self.object_store.clone();
            // Use an async move block to get our owned objects.
            async move {
                let object_store = arc.lock().await;
                object_store
                    .get_range(&path, start..start + lenght)
                    .map_ok(|r| r.to_vec())
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("object store error {}", e),
                        )
                    })
                    .await
            }
        };
        let mut future = Box::pin(future);

        // Need to poll it once to get the pump going.
        let polled = Future::poll(future.as_mut(), cx);

        // Save for next time.
        let mut state = self.active.lock().await;
        *state = Some(future);
        println!("create_future_once: created new future.");
        polled
    }
}

impl AsyncRead for AsyncCloudObject {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        println!("poll_read: entering");
        match block_on(self.create_future_once(cx, buf.len())) {
            Poll::Ready(Ok(bytes)) => {
                buf.copy_from_slice(&bytes[..]);
                println!("poll_read: returning ready");
                Poll::Ready(Ok(bytes.len()))
            }
            Poll::Ready(Err(e)) => {
                println!("poll_read: returning error");
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                println!("poll_read: returning pending");
                Poll::Pending
            }
        }
    }
}
impl AsyncSeek for AsyncCloudObject {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        pos: SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        println!("Entering poll_seek {:?}", pos);
        match pos {
            SeekFrom::Start(pos) => self.pos = pos,
            SeekFrom::End(pos) => self.pos = (self.length as i64 + pos) as u64,
            SeekFrom::Current(pos) => self.pos = (self.pos as i64 + pos) as u64,
        };
        std::task::Poll::Ready(Ok(self.pos))
    }
}

pub async fn parallel_read(
    object_store: Arc<Mutex<dyn ObjectStore>>,
    path: Path,
    row_group: usize,
) -> MyResult<()> {
    println!("Entering parallel_read.");
    let length = {
        let locked_store = object_store.lock().await;
        locked_store
            .head(&path)
            .await
            .map_err(|e| format!("Error in head() {}", e))?
            .size
    };
    println!("  length {}", length);

    let reader_factory = || {
        let object_store = object_store.clone();
        let path = path.clone();
        Box::pin(futures::future::ready(Ok(AsyncCloudObject::new(
            length,
            object_store,
            path,
        ))))
    }
        as BoxFuture<'static, std::result::Result<AsyncCloudObject, std::io::Error>>;
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
