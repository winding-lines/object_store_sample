/// Test the AWS implementation of the object store.

use awscreds::Credentials;
use futures::lock::Mutex;
use object_store::aws::{AmazonS3Builder, AmazonS3};
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;
use std::time::SystemTime;
use object_store_sample::read_from_object_store::parallel_read;
use object_store_sample::async_read_from_object_store::parallel_read as async_parallel_read;
use object_store_sample::list_bucket::list_bucket;
use std::env;

const PARQUET: &str = "delta-rs/rust/tests/data/simple_table/part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet";

fn build_s3() -> AmazonS3 {
    let cred = Credentials::default().unwrap();
    AmazonS3Builder::new()
        .with_access_key_id(cred.access_key.unwrap())
        .with_secret_access_key(cred.secret_key.unwrap())
        .with_region("us-west-2")
        .with_bucket_name("lov2test")
        .build()
        .unwrap()
}

#[tokio::main]
async fn main() {
    // Setup tracing.
    tracing_subscriber::fmt::try_init().unwrap();
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    // Create an ObjectStore.

    // Process cli arguments.
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        2 => {
            let start = SystemTime::now();
            let arg = &args[1];
            match &arg[..] {
                "list" => {
                    let s3 = build_s3();
                    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
                    list_bucket(&object_store).await
                }
                "ranged" => {
                    let s3 = build_s3();
                    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
                    let path = Path::from(PARQUET);
                    parallel_read(object_store, path, 0).await.unwrap();
                }
                "async" => {
                    let s3=build_s3();
                    let object_store = Arc::new(Mutex::new(s3));
                    let path = Path::from(PARQUET);
                    async_parallel_read(object_store, path, 0).await.unwrap();
                }
                other => {
                    println!("Unknow argument {other}, use one of:\n  list\n  ranged\n  async\n")
                }
            }

            println!("took: {} ms", start.elapsed().unwrap().as_millis());
        }

        num => {
            println!(
                "Unexpected number of arguments {num}, use one of:\n  list\n  ranged\n  async\n"
            )
        }
    }
}
