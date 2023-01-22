
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;
use futures::StreamExt;

pub async fn list_bucket(object_store: &Arc<dyn ObjectStore>) {
    // Recursively list all files in the bucket.
    let prefix: Path = "".try_into().unwrap();

    // Get an `async` stream of Metadata objects:
    let list_stream = object_store
        .list(Some(&prefix))
        .await
        .expect("Error listing files");

    // Print a line about each object based on its metadata
    // using for_each from `StreamExt` trait.
    list_stream
        .for_each(move |meta| async {
            let meta = meta.expect("Error listing");
            println!("Name: {}, size: {}", meta.location, meta.size);
        })
        .await;
}
