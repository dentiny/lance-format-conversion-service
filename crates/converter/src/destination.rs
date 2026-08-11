use std::sync::Arc;

use lance::{dataset::WriteParams, io::ObjectStoreParams};
use lance_table::io::commit::ConditionalPutCommitHandler;
use object_store::{ObjectStore, aws::AmazonS3Builder};
use reqwest::Url;

use crate::ConversionError;

pub(crate) fn configure(
    destination_uri: &str,
    params: &mut WriteParams,
) -> Result<(), ConversionError> {
    let url = Url::parse(destination_uri);
    if url.as_ref().map(Url::scheme) != Ok("s3") {
        return Ok(());
    }
    let url = url.map_err(|error| ConversionError::InvalidDestination(error.to_string()))?;
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| ConversionError::InvalidDestination("S3 bucket is missing".to_owned()))?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|error| ConversionError::InvalidDestination(error.to_string()))?,
    );
    let root = Url::parse(&format!("s3://{bucket}"))
        .map_err(|error| ConversionError::InvalidDestination(error.to_string()))?;

    #[allow(deprecated)]
    {
        params.store_params = Some(ObjectStoreParams {
            object_store: Some((store, root)),
            list_is_lexically_ordered: Some(true),
            ..ObjectStoreParams::default()
        });
    }
    params.commit_handler = Some(Arc::new(ConditionalPutCommitHandler));
    Ok(())
}
