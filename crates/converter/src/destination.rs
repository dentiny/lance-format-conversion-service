use std::sync::Arc;

use lance::{dataset::WriteParams, io::ObjectStoreParams};
use lance_table::io::commit::ConditionalPutCommitHandler;
use object_store::{ObjectStore, aws::AmazonS3Builder};
use reqwest::Url;

use crate::ConversionError;

pub(crate) struct Destination<'a> {
    uri: &'a str,
}

impl<'a> Destination<'a> {
    pub(crate) const fn new(uri: &'a str) -> Self {
        Self { uri }
    }

    pub(crate) fn configure(self, params: &mut WriteParams) -> Result<(), ConversionError> {
        let Ok(mut root) = Url::parse(self.uri) else {
            return Ok(());
        };
        if root.scheme() != "s3" {
            return Ok(());
        }

        let store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::from_env()
                .with_url(self.uri)
                .build()
                .map_err(invalid_destination)?,
        );
        root.set_path("");
        root.set_query(None);
        root.set_fragment(None);

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
}

fn invalid_destination(error: object_store::Error) -> ConversionError {
    ConversionError::InvalidDestination(error.to_string())
}
