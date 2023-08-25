
use serde::{Deserialize, Serialize};

use tokio::sync::{mpsc,oneshot};

/// return Error to Ui Layer, we must manually implement serde::Serialize
/// 
/// to serialize Rust UiError enum types to std::Result::Err(String)
#[derive(Debug, thiserror::Error)]
pub enum UiError {
  #[error(transparent)]
  SendCmd(#[from] mpsc::error::SendError<(String,String,oneshot::Sender<UiResponse>)>),
  #[error(transparent)]
  RecvCmdResp(#[from] oneshot::error::RecvError),
  #[error(transparent)]
  SerializeRespJson(#[from] serde_json::error::Error)
}

impl serde::Serialize for UiError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: serde::ser::Serializer,
    {
      serializer.serialize_str(self.to_string().as_ref())
    }
}


#[derive(Serialize, Deserialize,Debug)]
pub struct UiResponse{
    pub status_code: i32,
    pub status_msg: String,
}

#[derive(Clone,Serialize, Deserialize,Debug)]
pub struct UiStartUploadDatasetRequest{
    pub dataset_id: String,
    pub dataset_version_id: String,
    pub dataset_cache_dir: String,
    pub dataset_source: String,
    pub server_endpoint: String,
}

#[derive(Serialize, Deserialize,Debug)]
pub struct UiStopUploadDatasetRequest{
    pub dataset_id: String,
    pub dataset_version_id: String,
}

#[derive(Serialize, Deserialize,Debug)]
pub struct UiTerminateUploadDatasetRequest{
    pub dataset_id: String,
    pub dataset_version_id: String,
}
