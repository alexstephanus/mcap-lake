//! S3/MinIO storage wiring for both "halves" of the pipeline.
//!
//! Two mutually-exclusive packages touch object storage.
//! We use the `object_store` crate to read MCAPs from MinIO, but the iceberg crate
//! requires us to use the OpenDAL storage layer, so we centralize config management
//! into one struct, which we build the ObjectStore and RestCatalog off of in order
//! to keep them tightly-coupled.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::TryStreamExt;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use object_store::{ObjectMeta, ObjectStore, aws::AmazonS3Builder, path::Path};

/// The shared S3/MinIO connection: endpoint, region, and credentials.  
#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

impl StorageConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("AWS_ENDPOINT_URL")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key: std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"),
            secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .expect("AWS_SECRET_ACCESS_KEY not set"),
        }
    }

    /// ObjectStors is used to read MCAP files.
    pub fn build_store(&self, bucket: &str) -> Arc<dyn ObjectStore> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_bucket_name(bucket)
            // Plain HTTP is allowed for the local MinIO endpoint.
            .with_allow_http(true)
            .build()
            .expect("failed to build S3 store");
        Arc::new(store)
    }

    pub async fn build_catalog(&self, uri: &str, warehouse: &str) -> RestCatalog {
        let props = HashMap::from([
            ("uri".to_string(), uri.to_string()),
            ("warehouse".to_string(), warehouse.to_string()),
            ("s3.endpoint".to_string(), self.endpoint.clone()),
            ("s3.region".to_string(), self.region.clone()),
            ("s3.access-key-id".to_string(), self.access_key.clone()),
            ("s3.secret-access-key".to_string(), self.secret_key.clone()),
            // OpenDAL defaults to virtual-host addressing ({bucket}.{endpoint}).
            // This doesn't resolve against MinIO, so we need path-style access here
            ("s3.path-style-access".to_string(), "true".to_string()),
        ]);
        RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("mcap-lake", props)
            .await
            .expect("failed to build rest catalog")
    }
}

pub async fn list_all(store: &dyn ObjectStore, prefix: Option<&Path>) -> Vec<ObjectMeta> {
    store
        .list(prefix)
        .try_collect()
        .await
        .expect("failed to list objects")
}
