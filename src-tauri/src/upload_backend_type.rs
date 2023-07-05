
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize,Debug)]
pub struct UploadDatasetRequest{
    dataset_id: String,
    server_endpoint: String,
}