/// List a known GCP bucket in order to test the GCP implementation.
/// 
/// Locally this expects a file `~/.config/gcloud/application_default_credentials.json` to exist.
/// <https://cloud.google.com/docs/authentication/provide-credentials-adc>
/// 
/// To test the instance credentials, run this on a GCP instance with the appropriate permissions.
/// <https://cloud.google.com/compute/docs/instances/create-start-instance>

use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path, ObjectStore};
use futures::stream::StreamExt;


#[tokio::main]
async fn main() {
    let gcp = GoogleCloudStorageBuilder::default()
        .with_bucket_name("lov2023test")

        .build()
        .unwrap();
    gcp.list(Some(&Path::from("firmware")))
        .await
        .expect("listing gcp bucket")
        .for_each(move |entry| async {
            println!("{:?}", entry.expect("entry").location);
        })
        .await;
}
