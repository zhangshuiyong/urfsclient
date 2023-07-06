
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize,Debug)]
pub struct UiResponse{
    pub status_code: i32,
    pub status_msg: String,
}

#[derive(Serialize, Deserialize,Debug)]
pub struct UiStartUploadDatasetRequest{
    dataset_id: String,
    server_endpoint: String,
}

#[derive(Serialize, Deserialize,Debug)]
pub struct UiStopUploadDatasetRequest{
    pub dataset_id: String,
}