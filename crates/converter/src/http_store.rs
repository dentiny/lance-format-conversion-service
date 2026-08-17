use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lance::Error;
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_DOWNLOAD_RETRY_COUNT, ObjectStore, ObjectStoreParams,
    ObjectStoreProvider, ObjectStoreRegistry,
};
use object_store::{ClientOptions, http::HttpBuilder};
use url::Url;

/// Fetches `http://` and `https://` blob URLs during Lance ingest.
///
/// Lance's default registry has no HTTP provider, so blob columns whose values
/// are web URLs fail with "No object store provider found for scheme: 'http'".
#[derive(Debug, Default)]
struct HttpStoreProvider;

#[async_trait]
impl ObjectStoreProvider for HttpStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        let origin = format!("{}://{}", base_path.scheme(), base_path.authority());
        let location = Url::parse(&origin).map_err(|error| Error::io(error.to_string()))?;
        let inner = HttpBuilder::new()
            .with_url(origin)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .build()
            .map_err(|error| Error::io(error.to_string()))?;
        Ok(ObjectStore::new(
            Arc::new(inner),
            location,
            params.block_size,
            None,
            false,
            true,
            DEFAULT_CLOUD_IO_PARALLELISM,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            params.storage_options(),
        ))
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(format!("{}${}", url.scheme(), url.authority()))
    }
}

pub(crate) fn register(registry: &ObjectStoreRegistry) {
    let provider: Arc<dyn ObjectStoreProvider> = Arc::new(HttpStoreProvider);
    registry.insert("http", Arc::clone(&provider));
    registry.insert("https", provider);
}
