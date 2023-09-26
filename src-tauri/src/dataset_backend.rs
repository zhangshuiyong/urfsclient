
//======== dataset_image_cmd.rs ==========
use crate::dataset_image_cmd;
//======== dataset_image_cmd.rs ==========

use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::{Ok, Result};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt,AsyncSeekExt};

use reqwest::{multipart, Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc,oneshot,broadcast,Semaphore};
use tokio::time::Duration;
use std::sync::Arc;
use std::convert::From;

use nydus_storage::{RAFS_MAX_CHUNK_SIZE,RAFS_MAX_CHUNKS_PER_BLOB};

pub const HTTP_CONN_POOL_DEFAULT_SIZE: usize = 10;
pub const HTTP_CONN_RECYCLE_TIMEOUT: u64 = 60;
pub const CHUNK_UPLOADER_MAX_CONCURRENCY: usize = 4;
pub const HISTORY_TASK_DB_PATH: &str = "./upload_history_data.db";
pub const HISTORY_TASK_KEY_PREFIX: &str = "urfs";
pub const HISTORY_TASK_LIST_MAX_LENGTH: usize = 300;

use std::fmt;
use nydus_utils::digest;

use crate::dataset_backend_type::UiResponse;
use crate::dataset_backend_type::UiStartUploadDatasetRequest;
use crate::dataset_backend_type::UiStopUploadDatasetRequest;
use crate::dataset_backend_type::UiDeleteDatasetTaskRequest;

#[derive(Debug,Clone,Serialize, Deserialize)]
pub enum DataSetStatus{
    Wait,//wait
    Init,//init dataset image
    ReadyUpload,//upload
    Uploading(f32),//uploading
    Stop,
    AsyncProcessing,
    Success,
    Failed,
    UnKnown,
}

#[derive(Serialize, Deserialize,Debug)]
pub struct DatasetHistoryTask{
    pub dataset_id: String,
    pub dataset_version_id: String,
    pub local_dataset_path: String,
    pub local_dataset_size: u64,
    pub dataset_status: DataSetStatus,
    pub payload_type: bool,
    pub create_timestamp: u64,
}

//json std is deserialize String to enum
//json std is serialize enum to String
#[derive(Debug,PartialEq,Serialize, Deserialize)]
enum DataMode{
    Source,
    Meta,
    Ephemeral,
    Chunk,
    ChunkEnd,
    Blob,
    UnKnown
}

impl fmt::Display for DataMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Source  => write!(f, "{}", "Source"),
            Self::Meta => write!(f, "{}", "Meta"),
            Self::Ephemeral => write!(f, "{}", "Ephemeral"),
            Self::Chunk => write!(f, "{}", "Chunk"),
            Self::ChunkEnd => write!(f, "{}", "ChunkEnd"),
            Self::Blob => write!(f, "{}", "Blob"),
            Self::UnKnown => write!(f, "{}", "UnKnown"),
        }
    }
}

impl From<i32> for DataMode{
    fn from(value: i32) -> Self {
        match value {
            0  => Self::Source,
            1 => Self::Meta,
            2 => Self::Ephemeral,
            3 => Self::Chunk,
            4 => Self::ChunkEnd,
            5 => Self::Blob,
            _ => Self::UnKnown
        }
    }
}

#[derive(Debug,PartialEq,Clone)]
struct DatasetDigest{
    algorithm: String,
    hash: String
}
impl DatasetDigest {
    pub fn new(algo:String, hash_str: String) -> Self {
        Self {
            algorithm: algo,
            hash: hash_str,
        }
    }
}

impl fmt::Display for DatasetDigest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm,self.hash)
    }
}


/// get http client from connection pool
fn get_http_client() -> Result<Client> {

    let httpclient = ClientBuilder::new()
        .pool_idle_timeout(Duration::from_secs(HTTP_CONN_RECYCLE_TIMEOUT))
        .pool_max_idle_per_host(HTTP_CONN_POOL_DEFAULT_SIZE).build()?;

    Ok(httpclient)
}

use anyhow::{ensure, Context};
use std::fs::metadata;

use std::path::Path;
use nydus_api::ConfigV2;
use crate::inspect;

fn ensure_directory<P: AsRef<Path>>(path: P) -> Result<()> {
    let dir = metadata(path.as_ref())
        .context(format!("failed to access path {:?}", path.as_ref()))?;
    ensure!(
            dir.is_dir(),
            "specified path must be a directory: {:?}",
            path.as_ref()
        );
    Ok(())
}

fn inspect_blob_info(bootstrap_path: &Path) -> Result<String> {
    let mut config = Arc::new(ConfigV2::default());

    // For backward compatibility with v2.1
    config.internal.set_blob_accessible(true);

    if let Some(cache) = Arc::get_mut(&mut config).unwrap().cache.as_mut() {
        cache.cache_validate = true;
    }

    let cmd = "blobs".to_string();
    let request_mode = true;

    let mut inspector = inspect::RafsInspector::new(bootstrap_path, request_mode, config)
        .map_err(|e| {
            error!("failed to create dataset image inspector, {:?}", e);
            e
        })?;

    let o = inspect::Executor::execute(&mut inspector, cmd).unwrap();
    let jsons = serde_json::to_string( &o)?;

    Ok(jsons)
}

//UrchinStatusCode subset contains UrchinFileStatus
#[derive(Debug,PartialEq,Clone)]
enum UrchinFileStatus{
    Exist,
    NotFound,
    Partial,
    UnKnown
}

impl From<UrchinStatusCode> for UrchinFileStatus{
    fn from(value: UrchinStatusCode) -> Self {
        match value {
            UrchinStatusCode::Exist  => Self::Exist,
            UrchinStatusCode::NotFound => Self::NotFound,
            UrchinStatusCode::PartialUploaded => Self::Partial,
            _ => Self::UnKnown
        }
    }
}

impl fmt::Display for UrchinFileStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Exist  => write!(f, "{}", "Exist"),
            Self::NotFound => write!(f, "{}", "NotFound"),
            Self::Partial => write!(f, "{}", "Partial"),
            Self::UnKnown => write!(f, "{}", "UnKnown"),
        }
    }
}

async fn stat_file(server_endpoint: &str,mode:&str,dataset_id:&str,dataset_version_id:&str,digest: &DatasetDigest,total_size:u64) -> Result<UrchinFileStatus> {

    let httpclient = get_http_client()?;

    let stat_endpoint = server_endpoint.to_string() + "/api/v1/file/stat";
    let digest_str = digest.to_string();
    let total_size_str = total_size.to_string();
    let stat_params = [
        ("mode",mode),
        ("dataset_id", dataset_id),
        ("dataset_version_id", dataset_version_id),
        ("digest", digest_str.as_str()),
        ("total_size",total_size_str.as_str())
    ];

    let stat_url = reqwest::Url::parse_with_params(stat_endpoint.as_str(), &stat_params)?;

    debug!("[stat_file] url {:?}. dataset_id:{},dataset_version_id:{}",stat_url,dataset_id,dataset_version_id);

    let resp: StatFileResponse = httpclient
        .get(stat_url)
        .send()
        .await?
        .json()
        .await?;

    debug!("[stat_file] result:{:?}. dataset_id:{},dataset_version_id:{}",resp,dataset_id,dataset_version_id);

    let status_code = resp.status_code.into();

    match status_code {
        UrchinStatusCode::Exist | UrchinStatusCode::NotFound | UrchinStatusCode::PartialUploaded  => Ok(status_code.into()),
        _ => Ok(UrchinFileStatus::UnKnown)
    }

}

async fn stat_chunk_file(server_endpoint: &str,mode:&str,dataset_id:&str,dataset_version_id:&str,
                         digest: DatasetDigest,total_size:u64,
                         chunk_size:u64,chunk_start:u64,chunk_num:u64) -> Result<UrchinFileStatus> {

    let httpclient = get_http_client()?;

    let stat_endpoint = server_endpoint.to_string() + "/api/v1/file/stat";
    let digest_str = digest.to_string();
    let total_size_str = total_size.to_string();
    let chunk_size_str = chunk_size.to_string();
    let chunk_start_str = chunk_start.to_string();
    let chunk_num_str = chunk_num.to_string();

    let stat_params = [
        ("mode",mode),
        ("dataset_id", dataset_id),
        ("dataset_version_id", dataset_version_id),
        ("digest", digest_str.as_str()),
        ("total_size",total_size_str.as_str()),
        ("chunk_size",chunk_size_str.as_str()),
        ("chunk_start",chunk_start_str.as_str()),
        ("chunk_num",chunk_num_str.as_str()),
    ];

    let stat_url = reqwest::Url::parse_with_params(stat_endpoint.as_str(), &stat_params)?;

    debug!("[stat_chunk_file]: stat_chunk_file url {:?}. dataset_id:{}, dataset_version_id:{}",stat_url,dataset_id,dataset_version_id);

    let resp: StatFileResponse = httpclient
        .get(stat_url)
        .send()
        .await?
        .json()
        .await?;

    debug!("[stat_chunk_file], result:{:?}. dataset_id:{}, dataset_version_id:{}",resp,dataset_id,dataset_version_id);

    let status_code = resp.status_code.into();

    match status_code {
        UrchinStatusCode::Exist | UrchinStatusCode::NotFound | UrchinStatusCode::PartialUploaded  => Ok(status_code.into()),
        _ => Ok(UrchinFileStatus::UnKnown)
    }

}

fn get_dataset_image_cache_path(req:UiStartUploadDatasetRequest) -> Result<PathBuf> {

    let app_cache_dir = std::env::var("URFS_APP_CACHE_DIR")?;

    debug!("[get_dataset_image_cache_path] app_cache_dir:{:?}",app_cache_dir);

    let dataset_cache_dir = Path::new(&app_cache_dir);

    Ok(dataset_cache_dir.join(req.dataset_id.as_str()))
}

async fn create_dataset_image(req:UiStartUploadDatasetRequest) -> Result<()> {

    let dataset_image_cache_path = get_dataset_image_cache_path(req.clone())?;

    if ensure_directory(dataset_image_cache_path.as_path()).is_err() {
        info!("dataset_image_cache_path:{:?} not exist!!!",dataset_image_cache_path);
        tokio::fs::create_dir_all(dataset_image_cache_path.as_path()).await?;
    };

    info!("source_image_dir exist:{:?}",dataset_image_cache_path);

    let meta_file_path_buf = dataset_image_cache_path.join("meta");

    let meta_file_path_op = meta_file_path_buf.as_path().as_os_str().to_str();
    if meta_file_path_op.is_none() {
        error!("failed to get meta_file_path: none");
        return Err(anyhow!("failed to get meta_file_path: none"));
    }

    let source_image_dir_op = dataset_image_cache_path.as_os_str().to_str();
    if source_image_dir_op.is_none() {
        error!("failed to get source_image_dir: none");
        return Err(anyhow!("failed to get source_image_dir: none"));
    }

    let arg_vec = vec!["","create","-B", meta_file_path_op.unwrap_or_default(),"-D",source_image_dir_op.unwrap_or_default(),req.dataset_source.as_str()];
    
    info!("urchin dataset image create command:{:?}",arg_vec);

    let result = execute_dataset_image_cmd(arg_vec).await;

    return result;
}

async fn rename_dataset_image_cache(req:UiStartUploadDatasetRequest) -> Result<()> {

    let dataset_image_cache_path = get_dataset_image_cache_path(req.clone())?;

    let dataset_meta_path = dataset_image_cache_path.join("meta");

    let blobs_info_json =  inspect_blob_info(dataset_meta_path.as_path())?;

    let dataset_metas: Vec<DatasetMeta> =  serde_json::from_str(blobs_info_json.as_str())?;

    let dataset_meta = &dataset_metas[0];

    let upload_dataset_meta = dataset_meta.clone();

    let dataset_blob_path = dataset_image_cache_path.join(upload_dataset_meta.digest.as_str());

    let new_dataset_meta_path = dataset_image_cache_path.join(format!("meta_urfs:{}_{}",upload_dataset_meta.digest.as_str(),req.dataset_version_id.as_str()).as_str());

    let new_dataset_blob_path = dataset_image_cache_path.join(format!("blob_urfs:{}_{}",upload_dataset_meta.digest.as_str(),req.dataset_version_id.as_str()).as_str());

    tokio::fs::rename(dataset_meta_path.as_path(),new_dataset_meta_path.as_path()).await?;

    tokio::fs::rename(dataset_blob_path.as_path(),new_dataset_blob_path.as_path()).await?;

    Ok(())
}

async fn execute_dataset_image_cmd(arg_vec:Vec<&str>) -> Result<()> {

    let build_info = dataset_image_cmd::BTI.to_owned();
    let mut app = dataset_image_cmd::prepare_cmd_args(dataset_image_cmd::BTI_STRING.as_str());
    let usage = app.render_usage();
    
    let cmd = app.get_matches_from(arg_vec);

    let mut result = Ok(());

    if let Some(matches) = cmd.subcommand_matches("create") {
        result = dataset_image_cmd::Command::create(matches, &build_info);
    } else if let Some(matches) = cmd.subcommand_matches("merge") {
        result = dataset_image_cmd::Command::merge(matches, &build_info);
    } else if let Some(matches) = cmd.subcommand_matches("check") {
        result = dataset_image_cmd::Command::check(matches, &build_info);
    } else if let Some(matches) = cmd.subcommand_matches("inspect") {
        result = dataset_image_cmd::Command::inspect(matches);
    } else if let Some(matches) = cmd.subcommand_matches("stat") {
        result = dataset_image_cmd::Command::stat(matches);
    } else if let Some(matches) = cmd.subcommand_matches("compact") {
        result = dataset_image_cmd::Command::compact(matches, &build_info);
    } else if let Some(matches) = cmd.subcommand_matches("unpack") {
        result = dataset_image_cmd::Command::unpack(matches);
    } else {
        error!("please see the urchin-image command usage {}", usage);
    }

    if let Err(e) = result {
        error!("failed to execute urchin dataset image command, {:?}", e);
        return Err(anyhow!("failed to execute urchin dataset image command, {:?}", e));
    }

    Ok(())
}

fn get_history_task_db_path() -> Result<PathBuf> {

    let app_cache_dir = std::env::var("URFS_APP_CACHE_DIR")?;

    debug!("[get_history_task_db_path] history_task_to_db in app_cache_dir:{:?}",app_cache_dir);

    let db_parent_dir_path_buf = PathBuf::from(app_cache_dir);

    let history_task_db_path_buf = db_parent_dir_path_buf.join(HISTORY_TASK_DB_PATH);

    Ok(history_task_db_path_buf)
}

fn new_history_task_to_db(dataset_id:&str,dataset_version_id:&str,local_dataset_path:&str) -> Result<()> {

    info!("[new_history_task_to_db] add dataset_id:{}, dataset_version_id:{}, local_dataset_path:{}", dataset_id,dataset_version_id,local_dataset_path);

    let history_task_db_path = get_history_task_db_path()?;
    
    info!("[new_history_task_to_db] history_task_db_path: {:?}", history_task_db_path);

    let dataset_history_task_db: sled::Db = sled::open(history_task_db_path)?;

    let task_key = format!("{}:{}:{}",HISTORY_TASK_KEY_PREFIX,dataset_id,dataset_version_id);

    let timestamp_duration = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

    let task = DatasetHistoryTask{
        dataset_id: dataset_id.to_string(),
        dataset_version_id: dataset_version_id.to_string(),
        local_dataset_path: local_dataset_path.to_string(),
        local_dataset_size: 0,
        dataset_status: DataSetStatus::Wait,
        payload_type: true,
        create_timestamp: timestamp_duration.as_secs(),
    };

    let task_json = serde_json::to_string(&task)?;

    info!("[new_history_task_to_db] add task_key:{}, task:{}", task_key,task_json);

    dataset_history_task_db.insert(task_key.as_bytes(),task_json.as_bytes())?;

    Ok(())
    
}

fn update_history_task_to_db(dataset_id:&str,dataset_version_id:&str,local_dataset_size: u64,ds: DataSetStatus) -> Result<()> {
    
    let history_task_db_path = get_history_task_db_path()?;

    let dataset_history_task_db: sled::Db = sled::open(history_task_db_path)?;

    let task_key = format!("{}:{}:{}",HISTORY_TASK_KEY_PREFIX,dataset_id,dataset_version_id);

    let old_task_json_op = dataset_history_task_db.get(task_key.as_bytes())?;
    
    if let Some(old_task_json_bytes) = old_task_json_op {

        let old_task_json = String::from_utf8(old_task_json_bytes.to_vec())?;

        info!("[update_history_task_to_db] old task:{},key:{}",old_task_json,task_key);

        let mut old_task = serde_json::from_str::<DatasetHistoryTask>(&old_task_json)?;

        if local_dataset_size>0 {
            old_task.local_dataset_size = local_dataset_size;
        }

        old_task.dataset_status = ds;

        let new_task_json = serde_json::to_string(&old_task)?;

        info!("[update_history_task_to_db] new task:{},key:{}",new_task_json,task_key);

        dataset_history_task_db.insert(task_key.as_bytes(),new_task_json.as_bytes())?;

    }else{
        return Err(anyhow!("[update_history_task_to_db] Can not get history task of key {:?}",task_key));
    }

    Ok(())
}

fn update_history_task_status_to_db(dataset_id:&str,dataset_version_id:&str,ds: DataSetStatus) -> Result<()> {

    let history_task_db_path = get_history_task_db_path()?;

    let dataset_history_task_db: sled::Db = sled::open(history_task_db_path)?;

    let task_key = format!("{}:{}:{}",HISTORY_TASK_KEY_PREFIX,dataset_id,dataset_version_id);

    let old_task_json_op = dataset_history_task_db.get(task_key.as_bytes())?;
    
    if let Some(old_task_json_bytes) = old_task_json_op {

        let old_task_json = String::from_utf8(old_task_json_bytes.to_vec())?;

        info!("[update_history_task_status_to_db] old task:{},key:{}",old_task_json,task_key);

        let mut task = serde_json::from_str::<DatasetHistoryTask>(&old_task_json)?;

        task.dataset_status = ds;

        let new_task_json = serde_json::to_string(&task)?;

        info!("[update_history_task_status_to_db] new task:{},key:{}",new_task_json,task_key);

        dataset_history_task_db.insert(task_key.as_bytes(),new_task_json.as_bytes())?;

    }else{
        return Err(anyhow!("[update_history_task_status_to_db] Can not get task of key {:?}",task_key));
    }

    Ok(())
    
}


fn update_history_task_status_with_db(dataset_history_task_db:&sled::Db,dataset_id:&str,dataset_version_id:&str,ds: DataSetStatus) -> Result<()> {

    let task_key = format!("{}:{}:{}",HISTORY_TASK_KEY_PREFIX,dataset_id,dataset_version_id);

    let old_task_json_op = dataset_history_task_db.get(task_key.as_bytes())?;

    if let Some(old_task_json_bytes) = old_task_json_op {

        let old_task_json = String::from_utf8(old_task_json_bytes.to_vec())?;

        info!("[update_history_task_status_with_db] old task:{},key:{}",old_task_json,task_key);

        let mut task = serde_json::from_str::<DatasetHistoryTask>(&old_task_json)?;

        task.dataset_status = ds;

        let new_task_json = serde_json::to_string(&task)?;

        info!("[update_history_task_status_with_db] new task:{},key:{}",new_task_json,task_key);

        dataset_history_task_db.insert(task_key.as_bytes(),new_task_json.as_bytes())?;

    }else{
        return Err(anyhow!("[update_history_task_status_with_db] Can not get task of key {:?}",task_key));
    }

    Ok(())

}


fn delete_history_task_to_db(dataset_id:&str,dataset_version_id:&str) -> Result<()> {
    let history_task_db_path = get_history_task_db_path()?;

    let dataset_history_task_db: sled::Db = sled::open(history_task_db_path)?;

    let task_key = format!("{}:{}:{}",HISTORY_TASK_KEY_PREFIX,dataset_id,dataset_version_id);

    info!("[delete_history_task_to_db] task key:{}",task_key);

    dataset_history_task_db.remove(task_key.as_bytes())?;

    Ok(())
}

fn get_history_task_list_from_db(pre_len: usize) -> Result<String> {
    let history_task_db_path = get_history_task_db_path()?;

    let dataset_history_task_db: sled::Db = sled::open(history_task_db_path)?;

    let history_tasks_iter = dataset_history_task_db.scan_prefix(HISTORY_TASK_KEY_PREFIX.as_bytes());

    let mut history_tasks = vec![];

    for kv_result in history_tasks_iter {
        if let std::result::Result::Ok(kv) = kv_result {

            let task_json = String::from_utf8(kv.1.to_vec())?;

            let mut task =  serde_json::from_str::<DatasetHistoryTask>(&task_json)?;

            if std::env::var("URFS_IS_FIRST_REQUEST").is_err(){

                let dataset_status_json_str =  serde_json::to_string(&task.dataset_status)?;

                debug!("First request history task list, dataset status need to change:{:?}",dataset_status_json_str);

                if dataset_status_json_str.contains("Wait") || dataset_status_json_str.contains("Init")
                   || dataset_status_json_str.contains("ReadUpload") || dataset_status_json_str.contains("Uploading")  {

                    let result = update_history_task_status_with_db(&dataset_history_task_db,task.dataset_id.as_str(),task.dataset_version_id.as_str(),DataSetStatus::Stop);
                    if result.is_err() {
                        return Err(anyhow!("[get_history_task_list_from_db][update_history_task_status_with_db] err {:?},task:{:?}",result,task));
                    }

                    task.dataset_status = DataSetStatus::Stop;
                }
            }

            history_tasks.push(task);

        }else{
            break;
        }
    }

    //First request history_task_list will set Uploading DatasetStatus to Stop
    //After that change the state
    if std::env::var("URFS_IS_FIRST_REQUEST").is_err() {
        std::env::set_var("URFS_IS_FIRST_REQUEST", "false");
    }

    history_tasks.sort_by(|a, b| b.create_timestamp.cmp(&a.create_timestamp));
    //get pre-len tasks
    history_tasks.truncate(pre_len);    

    //info!("[get_history_task_list_from_db] task list:{:?} ",history_tasks);

    let history_tasks_json_str =  serde_json::to_string(&history_tasks)?;

    Ok(history_tasks_json_str)
}

async fn start_dataset_uploader(all_dataset_chunk_sema: Arc<Semaphore>, dataset_status_sender: mpsc::Sender<(String,String,DataSetStatus,u64)>,
                                uploader_shutdown_cmd_suber: broadcast::Sender<()>,uploader_shutdown_cmd_rx:broadcast::Receiver<()>,
                                req:UiStartUploadDatasetRequest) -> Result<()> {
       
    let dataset_image_cache_path = get_dataset_image_cache_path(req.clone())?;

    ensure_directory(dataset_image_cache_path.as_path())?;

    let dataset_meta_path = dataset_image_cache_path.join("meta");

    debug!("cdataset_source:{:?},dataset_image_cache_path:{:?}, dataset_meta_path: {:?}",req.dataset_source,dataset_image_cache_path, dataset_meta_path);

    let blobs_info_json =  inspect_blob_info(dataset_meta_path.as_path())?;

    let dataset_metas: Vec<DatasetMeta> =  serde_json::from_str(blobs_info_json.as_str())?;

    if dataset_metas.len() <=0 {
        error!("[start_dataset_uploader]: dataset upload empty content is not allowed!!! dataset_id:{:?},dataset_version_id:{:?}",req.dataset_id,req.dataset_version_id);
        return Err(anyhow!("[start_dataset_uploader]: dataset upload empty content is not allowed!!! dataset_id:{:?},dataset_version_id:{:?}",req.dataset_id,req.dataset_version_id));
    }

    let dataset_meta = &dataset_metas[0];

    let upload_dataset_meta = dataset_meta.clone();

    let dataset_blob_path = dataset_image_cache_path.join(upload_dataset_meta.digest.as_str());

    let upload_server_endpoint = req.server_endpoint.clone();

    let dataset_id = req.dataset_id.clone();
    let dataset_version_id = req.dataset_version_id.clone();

    debug!("start_dataset_uploader,DataSetStatus ReadyUpload update_history_task_to_db start!!!!");

    let dataset_status = DataSetStatus::ReadyUpload;
    dataset_status_sender.send((req.dataset_id.clone(), req.dataset_version_id.clone(),dataset_status,dataset_meta.uncompressed_size)).await?;

    debug!("start_dataset_uploader,DataSetStatus ReadyUpload update_history_task_to_db end!!!!");

    //Concurent upload futures tree
    let mut uploader: DatasetUploader = DatasetUploader::new(dataset_id,
                                                                dataset_version_id,
                                                                dataset_meta_path,
                                                                upload_dataset_meta,
                                                                dataset_blob_path,
                                                                upload_server_endpoint,
                                                                dataset_status_sender,
                                                uploader_shutdown_cmd_suber,
                                                    uploader_shutdown_cmd_rx,
                                                                all_dataset_chunk_sema);

    //ToDo: process upload result Error!!!
    uploader.upload().await?;

    Ok(())
}


async fn start_upload(all_dataset_sema: Arc<Semaphore>, dataset_status_sender: mpsc::Sender<(String,String,DataSetStatus,u64)>,
                      uploader_shutdown_cmd_suber: broadcast::Sender<()>,uploader_shutdown_cmd_rx:broadcast::Receiver<()>,
                      req:UiStartUploadDatasetRequest) -> Result<()> {
    let mut result;
                                            
    let mut dataset_status;

    debug!("[start_upload][create_dataset_image]: request permit, all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{}",
            all_dataset_sema.available_permits(),req.dataset_id,req.dataset_version_id);

    let permitted_by_all_dataset_to_create_dataset_image = all_dataset_sema.acquire().await?;

    debug!("[start_upload][create_dataset_image]: permitted by all_dataset! all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{}",
            all_dataset_sema.available_permits(),req.dataset_id,req.dataset_version_id);

    dataset_status = DataSetStatus::Init;
    dataset_status_sender.send((req.dataset_id.clone(),req.dataset_version_id.clone(),dataset_status,0)).await?;

    info!("[{:?}][start_upload][create_dataset_image] start !",std::thread::current().id());

    result = create_dataset_image(req.clone()).await;

    info!("[start_upload][create_dataset_image] end !");

    drop(permitted_by_all_dataset_to_create_dataset_image);

    debug!("[start_upload][create_dataset_image]: drop permitted by all_dataset! all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{}",
            all_dataset_sema.available_permits(),req.dataset_id,req.dataset_version_id);

    if result.is_ok() {
        
        result = start_dataset_uploader(all_dataset_sema.clone(), dataset_status_sender.clone(),uploader_shutdown_cmd_suber,uploader_shutdown_cmd_rx,req.clone()).await;
        
        if result.is_err() {

            error!("[start_upload][start_dataset_uploader] err:{:?} !",result);
            dataset_status = DataSetStatus::Failed;
            dataset_status_sender.send((req.dataset_id.clone(),req.dataset_version_id.clone(),dataset_status,0)).await?;
        }
    }else{
        error!("[start_upload][create_dataset_image] err:{:?} !",result);
        dataset_status = DataSetStatus::Failed;
        dataset_status_sender.send((req.dataset_id.clone(),req.dataset_version_id.clone(),dataset_status,0)).await?;
    }

    result = rename_dataset_image_cache(req.clone()).await;
    if result.is_err() {
        error!("[start_upload][rename_dataset_image_cache]: req {:?} , start_upload finish. err: {:?},",req,result);
    }

    Ok(())
}


#[derive(Debug,PartialEq,Serialize, Deserialize)]
enum UrchinStatusCode{
    Succeed,
    NotFound,
    Exist,
    PartialUploaded,
    UnKnown,
}

impl From<i32> for UrchinStatusCode {
    fn from(value: i32) -> Self {
        match value {
            1001  => Self::Succeed,
            1002 => Self::NotFound,
            1003 => Self::Exist,
            1004 => Self::PartialUploaded,
            _ => Self::UnKnown
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StatFileResponse {
    status_code: i32,
    status_msg: String,
    mode: DataMode,
    dataset_id: String,
    dataset_version_id: String,
    digest: String,
}

#[derive(Debug)]
//ToDo: DatasetManager work for dataset upload filter and collector and CC controller, do not need to upload a uploading dataset
//refer loop & TCP::Listener
pub struct DatasetManager {
    //upload_dataset_history: HashMap<String,DataSetStatus>,
    all_dataset_sema :Arc<Semaphore>,
    dataset_status_sender: mpsc::Sender<(String,String,DataSetStatus,u64)>,
    dataset_status_collector: mpsc::Receiver<(String,String,DataSetStatus,u64)>,
    ui_cmd_collector: mpsc::Receiver<(String,String,oneshot::Sender<UiResponse>)>,
    dataset_uploader_shutdown_cmd_senders: HashMap<String,broadcast::Sender<()>>,
}

//start upload dataset through DatasetManager
//then DatasetManager collect dataset upload status
//DatasetManager Layer is the External Interact Entry
impl DatasetManager {
    pub fn new(ui_cmd_collector: mpsc::Receiver<(String,String,oneshot::Sender<UiResponse>)>) -> Self {
        let (dataset_status_sender,dataset_status_collector) = mpsc::channel(100);

        Self {
            //upload_dataset_history: HashMap::new(),
            all_dataset_sema: Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY)),
            dataset_status_sender,
            dataset_status_collector,
            ui_cmd_collector,
            dataset_uploader_shutdown_cmd_senders: HashMap::new(),
        }
    }

    /// DatasetManager must be call the run func to start working
    /// 
    /// Run for loop in current thread
    ///
    /// If you want to run in diff thread, you should use tokio::spawn
    pub async fn run(&mut self) {
        loop{
            tokio::select! {
                //allow dataset_status_sender free!
                Some((dataset_id,dataset_version_id,dataset_status,dataset_size)) = self.dataset_status_collector.recv() => {
                    debug!("[DatasetManager]: received dataset_status: {:?}. dataset_id:{},dataset_version_id:{}, dataset_size:{}",dataset_status,dataset_id,dataset_version_id,dataset_size);
        
                   let update_hist_task_result = update_history_task_to_db(dataset_id.as_str(),dataset_version_id.as_str(),dataset_size, dataset_status.clone());

                   if update_hist_task_result.is_err() {
                       error!("[DatasetManager]: update_history_task_status_to_db failed, dataset_id:{},dataset_version_id:{},dataset_status:{:?}, err:{:?}",
                       dataset_id,dataset_version_id,dataset_status,update_hist_task_result);
                   }
                },
                //allow ui_cmd_sender free!
                Some((cmd,req_json,resp_sender)) =  self.ui_cmd_collector.recv() => {
                    
                    match cmd.as_str() {
                        "start_upload" => {

                            debug!("[{:?}][DatasetManager]: ui_cmd_collector received cmd: {}, request: {:?}",std::thread::current().id(),cmd,req_json);
                            
                            let req_json_result =  serde_json::from_str::<UiStartUploadDatasetRequest>(&req_json);

                            match req_json_result {
                                std::result::Result::Ok(req) => {
                            
                                    let (uploader_shutdown_cmd_sx,uploader_shutdown_cmd_rx) = broadcast::channel(1);

                                    let uploader_shutdown_cmd_suber = uploader_shutdown_cmd_sx.clone();
                                    // after Concurent create Dataset Uploader, should add meta to DatasetManager
                                    self.set_dataset_uploader_shutdown_cmd_sender(req.dataset_id.clone(), req.dataset_version_id.clone(), uploader_shutdown_cmd_sx);

                                    let new_hist_task_result = new_history_task_to_db(req.dataset_id.as_str(), req.dataset_version_id.as_str(), req.dataset_source.as_str());

                                    if new_hist_task_result.is_err() {
                                        error!("[DatasetManager]:[start_upload] new_history_task_to_db failed, dataset_id:{},dataset_version_id:{},err:{:?}",
                                        req.dataset_id,req.dataset_version_id,new_hist_task_result);
                                    }else{

                                        let dataset_status_sender = self.dataset_status_sender.clone();
                                        let all_dataset_sema = self.all_dataset_sema.clone();
                                        
                                        //Concurent upload tasks futures chain
                                        warn!("[DatasetManager]:[start_upload] new tokio runtime for finish tokio::select! upload tasks future chain");
                                        std::thread::spawn(move || {
                                            let rt_result = tokio::runtime::Runtime::new();
                                            match rt_result {
                                                std::result::Result::Ok(rt) => {
                                                    rt.block_on(async move {

                                                        let result = start_upload(all_dataset_sema, dataset_status_sender.clone(),uploader_shutdown_cmd_suber,uploader_shutdown_cmd_rx,req.clone()).await;
                                                        match result {
                                                            std::result::Result::Ok(_) => {
                                                                debug!("[DatasetManager]:[start_upload] finish. result: {:?},",result);
                                                            },std::result::Result::Err(e)=> {
                                                                error!("[DatasetManager]:[start_upload] occur err:{:?}",e);
                                                            }
                                                        }

                                                    });
                                                },std::result::Result::Err(e)=> {
                                                    error!("[DatasetManager]:[start_upload] in new tokio runtime err:{:?}",e);
                                                }
                                            }
                                        });

                                        let resp = UiResponse{status_code: 0, status_msg:"".to_string(),payload_json:"".to_string()};

                                        debug!("[DatasetManager]: ui_cmd_collector cmd: {:?},resp:{:?}",cmd,resp);

                                        if resp_sender.send(resp).is_err(){
                                            //Do not need process next step, here is Err-Topest-Process Layer!
                                            error!("[DatasetManager]:[start_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                        }
                                    }
                                },
                                std::result::Result::Err(e)=> {

                                    error!("[DatasetManager]:[start_upload] err:{:?}",e);

                                    let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};

                                    if resp_sender.send(resp).is_err(){
                                            //Do not need process next step, here is Err-Topest-Process Layer!
                                            error!("[DatasetManager]:[start_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                    }
                                }
                            }

                        },
                        "stop_upload" => {
                            debug!("[DatasetManager]: ui_cmd_collector received cmd: {}, request: {:?}",cmd,req_json);
                            let req_json_result =  serde_json::from_str::<UiStopUploadDatasetRequest>(&req_json);
                            match req_json_result {
                                std::result::Result::Ok(req) => {

                                    let stop_result =  self.stop_dataset_uploader(req).await;
                                    match stop_result {
                                        std::result::Result::Ok(_) => {
                                            let resp = UiResponse{status_code: 0, status_msg:"".to_string(),payload_json:"".to_string()};

                                            if resp_sender.send(resp).is_err(){
                                                    //Do not need process next step, here is Err-Topest-Process Layer!
                                                    error!("[DatasetManager]:[stop_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                            }
                                        },
                                        std::result::Result::Err(e)=> {

                                            error!("[DatasetManager]:[stop_upload] err:{:?}",e);

                                            let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};

                                            if resp_sender.send(resp).is_err(){
                                                    //Do not need process next step, here is Err-Topest-Process Layer!
                                                    error!("[DatasetManager]:[stop_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                            }
                                        }
                                    }
                               },
                               std::result::Result::Err(e)=> {

                                  error!("[DatasetManager]:[stop_upload] err:{:?}",e);

                                  let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};
                                  if resp_sender.send(resp).is_err(){
                                       //Do not need process next step, here is Err-Topest-Process Layer!
                                       error!("[DatasetManager]:[stop_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                  }
                               }
                            }
                        },
                        "terminate_upload" => {
                            debug!("[DatasetManager]: ui_cmd_collector received cmd: {}, request: {:?}",cmd,req_json);

                            let req_json_result =  serde_json::from_str::<UiStopUploadDatasetRequest>(&req_json);
                            match req_json_result {
                                std::result::Result::Ok(req) => {

                                    let stop_result =  self.stop_dataset_uploader(req.clone()).await;
                                    match stop_result {
                                        std::result::Result::Ok(_) => {

                                            let delete_result = delete_history_task_to_db(req.dataset_id.as_str(),req.dataset_version_id.as_str());

                                            match delete_result {
                                                std::result::Result::Ok(_) => {

                                                    let resp = UiResponse{status_code: 0, status_msg:"".to_string(),payload_json:"".to_string()};

                                                    if resp_sender.send(resp).is_err(){
                                                            //Do not need process next step, here is Err-Topest-Process Layer!
                                                            error!("[DatasetManager]:[terminate_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                                    }
                                                },
                                                std::result::Result::Err(e)=> {

                                                    error!("[DatasetManager]:[terminate_upload] err:{:?}",e);

                                                    let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};

                                                    if resp_sender.send(resp).is_err(){
                                                            //Do not need process next step, here is Err-Topest-Process Layer!
                                                            error!("[DatasetManager]:[terminate_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                                    }
                                                }
                                            }
                                        },
                                        std::result::Result::Err(e)=> {

                                            error!("[DatasetManager]:[terminate_upload] err:{:?}",e);

                                            let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};
                                            if resp_sender.send(resp).is_err(){
                                                    //Do not need process next step, here is Err-Topest-Process Layer!
                                                    error!("[DatasetManager]:[terminate_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                            }
                                        }
                                    }
                                },
                                std::result::Result::Err(e)=> {
                                   error!("[DatasetManager]:[terminate_upload] err:{:?}",e);

                                   let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        error!("[DatasetManager]:[terminate_upload] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                   }
                                }
                            }

                        },
                        "get_history" => {
                            debug!("[{:?}][DatasetManager]:[get_history] ui_cmd_collector received cmd: {}, request: {:?}",std::thread::current().id(),cmd,req_json);

                            let history_task_list_json_result = get_history_task_list_from_db(HISTORY_TASK_LIST_MAX_LENGTH);

                            match history_task_list_json_result {
                                std::result::Result::Ok(history_task_list_json) => {
                                    
                                    let resp = UiResponse{status_code: 0, status_msg:"".to_string(),payload_json: history_task_list_json};

                                    if resp_sender.send(resp).is_err() {
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        error!("[DatasetManager]:[get_history] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                    }
                                },
                                std::result::Result::Err(e)=> {

                                    error!("[DatasetManager]:[get_history] err:{:?}",e);

                                    let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};

                                    if resp_sender.send(resp).is_err(){
                                            //Do not need process next step, here is Err-Topest-Process Layer!
                                            error!("[DatasetManager]:[get_history] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                    }
                                }
                            }
                        },
                        "delete_history_task" => {
                            debug!("[DatasetManager]: ui_cmd_collector received cmd: {}, request: {:?}",cmd,req_json);

                            let req_json_result =  serde_json::from_str::<UiDeleteDatasetTaskRequest>(&req_json);
                            match req_json_result {
                                std::result::Result::Ok(req) => {

                                    let delete_result = delete_history_task_to_db(req.dataset_id.as_str(),req.dataset_version_id.as_str());

                                    match delete_result {
                                        std::result::Result::Ok(_) => {

                                            let resp = UiResponse{status_code: 0, status_msg:"".to_string(),payload_json:"".to_string()};

                                            if resp_sender.send(resp).is_err(){
                                                    //Do not need process next step, here is Err-Topest-Process Layer!
                                                    error!("[DatasetManager]:[delete_history_task] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                            }
                                        },
                                        std::result::Result::Err(e)=> {

                                            error!("[DatasetManager]:[delete_history_task] err:{:?}",e);

                                            let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};

                                            if resp_sender.send(resp).is_err(){
                                                    //Do not need process next step, here is Err-Topest-Process Layer!
                                                    error!("[DatasetManager]:[delete_history_task] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                            }
                                        }
                                    }
                                },
                                std::result::Result::Err(e)=> {
                                   error!("[DatasetManager]:[delete_history_task] err:{:?}",e);

                                   let resp = UiResponse{status_code: -1, status_msg: e.to_string(),payload_json:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        error!("[DatasetManager]:[delete_history_task] can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                                   }
                                }
                            }
                        },
                        _ => {
                            error!("[DatasetManager]: ui_cmd_collector received unknow cmd: {}, request: {:?}",cmd,req_json);

                            let resp = UiResponse{status_code: -1, status_msg: format!("unknown cmd:{}",cmd),payload_json:"".to_string()};

                            if resp_sender.send(resp).is_err() {
                                //Do not need process next step, here is Err-Topest-Process Layer!
                                error!("[DatasetManager]: can not handle this err, just log!!! ui {} cmd resp channel err", cmd);
                            }
                        }
                    }
                },
            }
        }
    }

    fn get_dataset_uploader_shutdown_cmd_sender(&self, dataset_id:String,dataset_version_id:String) -> Option<broadcast::Sender<()>> {
        let op_dataset_uploader_shutdown_cmd_sender = self.dataset_uploader_shutdown_cmd_senders.get(format!("{}:{}",dataset_id,dataset_version_id).as_str());
        match op_dataset_uploader_shutdown_cmd_sender {
            Some(sender) => {
                return Some(sender.clone());
            },
            None => {
                return None;
            }
        }
    }

    fn set_dataset_uploader_shutdown_cmd_sender(&mut self, dataset_id:String,dataset_version_id:String,shutdown_cmd_sender:broadcast::Sender<()>) {
        self.dataset_uploader_shutdown_cmd_senders.insert(format!("{}:{}",dataset_id,dataset_version_id),shutdown_cmd_sender);
    }

    async fn stop_dataset_uploader(&self, req:UiStopUploadDatasetRequest) -> Result<()> {

        let try_uploader_shutdown_sx = self.get_dataset_uploader_shutdown_cmd_sender(req.dataset_id.clone(), req.dataset_version_id.clone());
        
        match try_uploader_shutdown_sx {
            Some(shutdown_sx) => {
                let uploader_shutdown_sx = shutdown_sx.clone();

                debug!("[DatasetManager]: stop_dataset_uploader, send dataset shutdown cmd!!! dataset_id:{},dataset_version_id:{}",
                req.dataset_id,req.dataset_version_id);
                if uploader_shutdown_sx.send(()).is_err() {
                    //ToDo: process error log
                    error!("[DatasetManager]: stop_dataset_uploader, uploader_shutdown_sx send shutdown cmd chan err!!! dataset_id:{},dataset_version_id:{}",
                    req.dataset_id,req.dataset_version_id);
                    return Err(anyhow!("[DatasetManager]: stop_dataset_uploader, uploader_shutdown_sx send shutdown cmd chan err!!! dataset_id:{},dataset_version_id:{}",
                    req.dataset_id,req.dataset_version_id));
                }
            },
            None => {
                error!("[DatasetManager]: stop_dataset_uploader, not found uploader_shutdown_sx. dataset_id:{},dataset_version_id:{}", 
                req.dataset_id, req.dataset_version_id);
                return Err(anyhow!("[DatasetManager]:stop_dataset_uploader, dataset_uploader_cmd_senders not found uploader_shutdown_sx. dataset_id:{},dataset_version_id:{}", 
                req.dataset_id, req.dataset_version_id));
            }
        }

        Ok(())
    }

}

//Support HTTP/HTTPS
pub struct DatasetUploader {
    dataset_id:String,
    dataset_version_id:String,
    dataset_meta_path: PathBuf,
    dataset_meta:DatasetMeta,
    dataset_blob_path:PathBuf,
    upload_server_endpoint: String,
    all_dataset_chunk_sema :Arc<Semaphore>,
    dataset_status_sender: mpsc::Sender<(String,String,DataSetStatus,u64)>,
    shutdown_suber: broadcast::Sender<()>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl Drop for DatasetUploader {
    fn drop(&mut self) {
        debug!("[DatasetUploader]: dataset uploader droped !!! dataset_id:{},dataset_version_id:{}",self.dataset_id,self.dataset_version_id);
    }
}

impl DatasetUploader {
    pub fn new(dataset_id:String,dataset_version_id:String,
               dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,dataset_blob_path:PathBuf,upload_server_endpoint: String,
               dataset_status_sender: mpsc::Sender<(String,String,DataSetStatus,u64)>,
               shutdown_suber: broadcast::Sender<()>,shutdown_rx: broadcast::Receiver<()>,
               all_dataset_chunk_sema :Arc<Semaphore>) -> Self {

        Self {
            dataset_id,
            dataset_version_id,
            dataset_meta_path,
            dataset_meta,
            dataset_blob_path,
            upload_server_endpoint,
            dataset_status_sender,
            all_dataset_chunk_sema,
            shutdown_suber,
            shutdown_rx,
        }
    }

    pub async fn upload(&mut self) -> Result<()> {

        info!("[DatasetUploader]:[upload_meta] dataset_meta_path: {:?}",self.dataset_meta_path);
        info!("[DatasetUploader]:[upload_meta] dataset_meta info:{:?}",self.dataset_meta);
        info!("[DatasetUploader]:[upload_meta] dataset_meta max size:{:?} TB",DatasetMeta::maxsize()?);

        self.upload_meta(self.dataset_id.clone(),self.dataset_version_id.clone(),self.dataset_meta_path.clone(),self.dataset_meta.clone(),self.upload_server_endpoint.clone()).await?;

        info!("[DatasetUploader]:[upload_blob] dataset_blob_path: {:?}",self.dataset_blob_path);

        self.upload_blob(self.dataset_id.clone(),self.dataset_version_id.clone(), self.dataset_blob_path.clone(),self.dataset_meta.clone(),self.upload_server_endpoint.clone()).await?;

        Ok(())
    }

    pub async fn upload_meta(&self,dataset_id:String, dataset_version_id:String, dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,server_endpoint: String) -> Result<()> {

        let mut meta_file = File::open(dataset_meta_path.as_path()).await?;
        let local_meta_file_info = meta_file.metadata().await?;
        debug!("[DatasetUploader]:[upload_meta], local meta file info {:?}. dataset_id:{}, dataset_version_id:{}", 
        local_meta_file_info,dataset_id,dataset_version_id);

        let digest = DatasetDigest::new("urfs".to_string(),dataset_meta.digest.clone());
        let meta_file_size = local_meta_file_info.len();
        let data_mode = DataMode::Meta.to_string();

        let meta_file_status = stat_file(server_endpoint.as_str(),data_mode.as_str(),
                                         dataset_id.as_str(),dataset_version_id.as_str(),
                                         &digest,meta_file_size).await?;

        //ToDo: process UnKnown File Status!!!
        if meta_file_status == UrchinFileStatus::UnKnown {
            debug!("[DatasetUploader]:[upload_meta] stat_file Meta file status is unknown, please check and stop upload process !!! dataset_id:{}, dataset_version_id:{}",
            dataset_id,dataset_version_id);
        }else if meta_file_status == UrchinFileStatus::Exist {
            debug!("[DatasetUploader]:[upload_meta] stat_file Meta file exist in backend, upload Finish !!! dataset_id:{}, dataset_version_id:{}",
            dataset_id,dataset_version_id);

            let dataset_status = DataSetStatus::Success;

            debug!("[DatasetUploader]:[upload_meta] send dataset_status:{:?} to [DatasetManager]. dataset_id:{}, dataset_version_id:{}", 
            dataset_status,dataset_id,dataset_version_id);

            self.dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),dataset_status,0)).await?;

        }else {

            let meta_file_name = dataset_meta_path.file_name().unwrap().to_str().unwrap().to_string();

            let mut contents: Vec<u8> = vec![];

            //meta file do not need to chunk upload
            meta_file.read_to_end(&mut contents).await?;

            let file_part = multipart::Part::bytes(contents)
                .file_name(meta_file_name)
                .mime_str("application/octet-stream")?;

            let form = multipart::Form::new()
                .part("file", file_part)
                .text("mode", data_mode)
                .text("dataset_id", dataset_id.clone())
                .text("dataset_version_id", dataset_version_id.clone())
                .text("digest", digest.to_string())
                .text("total_size", meta_file_size.to_string());

            let httpclient = get_http_client()?;

            let upload_meta_url = server_endpoint + "/api/v1/file/upload";

            debug!("[DatasetUploader]:[upload_meta], upload meta url {:?}. dataset_id:{}, dataset_version_id:{}", 
            upload_meta_url, dataset_id,dataset_version_id);

            let resp = httpclient
                .put(upload_meta_url)
                .multipart(form)
                .send().await?;

            debug!("[DatasetUploader]:[upload_meta] finish, resp:{:?}!!! dataset_id:{}, dataset_version_id:{}",
            resp, dataset_id,dataset_version_id);

            //ToDo: process meta file upload response!
            let _ = resp.text().await?;

        }

        Ok(())
    }

    async fn upload_blob(&mut self, dataset_id:String, dataset_version_id:String,dataset_blob_path: PathBuf,dataset_meta:DatasetMeta,server_endpoint: String) -> Result<()> {

        let digest = DatasetDigest::new("urfs".to_string(),dataset_meta.digest.clone());

        let blob_file_status = stat_file(server_endpoint.as_str(),DataMode::Blob.to_string().as_str(),
                                         dataset_id.as_str(),dataset_version_id.as_str(),
                                         &digest,dataset_meta.compressed_size).await?;

        //ToDo: process UnKnown File Status!!!
        if blob_file_status == UrchinFileStatus::UnKnown {
            debug!("[DatasetUploader]:[upload_blob] stat Blob file status is unknown, please check and stop upload process !!! dataset_id:{}, dataset_version_id:{}", 
            dataset_id,dataset_version_id);
        }else if blob_file_status == UrchinFileStatus::Exist{
            debug!("[DatasetUploader]:[upload_blob] stat Blob file exist in backend, upload Finish !!! dataset_id:{}, dataset_version_id:{}",
            dataset_id,dataset_version_id);
        }else{
            //may be NotFoundFile or Partial
            debug!("[DatasetUploader]:[upload_blob] stat not found blob file or partial upload, go to upload blob process. dataset_id:{}, dataset_version_id:{}",
            dataset_id,dataset_version_id);

            let all_dataset_chunk_sema = self.all_dataset_chunk_sema.clone();
            self.create_blob_chunks_manager(dataset_id,dataset_version_id,all_dataset_chunk_sema,dataset_meta,dataset_blob_path,server_endpoint).await?;
        }

        Ok(())
    }

    async fn create_blob_chunks_manager(&mut self,dataset_id:String, dataset_version_id:String, all_dataset_chunk_sema:Arc<Semaphore>,chunks_dataset:DatasetMeta,dataset_file_path: PathBuf,upload_endpoint: String) -> Result<()> {

        let dataset_status_sender = self.dataset_status_sender.clone();
        let chunks_manager_shutdown_suber = self.shutdown_suber.clone();

        let (chunk_pusher, chunk_getter) = mpsc::channel(100);

        let (chunk_result_sx,mut chunk_result_collector) = mpsc::channel(100);
       
        let mut chunks_manager = DatasetChunksManager::new(dataset_id.clone(),
                                                                                 dataset_version_id.clone(),
                                                                                 chunks_dataset,
                                                                                 all_dataset_chunk_sema,
                                                                                 upload_endpoint,
                                                                                 dataset_file_path,
                                                                                 chunk_pusher.clone(),
                                                                                 chunk_result_sx.clone(),
                                                                                 chunks_manager_shutdown_suber);

        //concurrent create data_chunk_consumer async task first! and wait data_chunk produce
        //if DatasetManager send shutdown cmd, data_chunk_consumer can stop first and cause data_chunk_producer stop!
        chunks_manager.create_data_chunk_consumer(chunk_getter).await;
        //concurrent create data_chunk_producer async task
        chunks_manager.create_data_chunk_producer(chunk_pusher,chunk_result_sx).await;

        let mut rest_upload_size = chunks_manager.upload_dataset.compressed_size;

        //wait data_chunk upload result or DatasetManager shutdown cmd
        loop{
            tokio::select! {
                //chunk_result_collector will not get None, cause DatasetChunksManager retain a no use chunk_result_sender
                //all chunk tasks end, will not let all chunk_result_senders are dropped, let chunk_result_collector get None
                Some(chunk_result) = chunk_result_collector.recv() => {
                    
                    debug!("[DatasetChunksManager]: received chunk_result: {:?}, dataset_id:{:?}, dataset_version_id:{:?}", 
                    chunk_result, dataset_id,dataset_version_id);

                    if chunk_result.upload_status.is_err(){
                        let dataset_status = DataSetStatus::Failed;

                        error!("[DatasetChunksManager][chunk_result_error]: send DataSetStatus: {:?} to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}, err:{:?}",
                        dataset_status,dataset_id,dataset_version_id,chunk_result);
                        
                        let send_result = dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),dataset_status,0)).await;

                        match send_result {
                            std::result::Result::Ok(_) => {
                                warn!("[DatasetChunksManager]: succeed to send DataSetStatus to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}", 
                                dataset_id,dataset_version_id);
                                //DataSetStatus::Failed, break the loop, End upload this dataset process!!!
                                break;
                            },
                            std::result::Result::Err(e) => {
                                //Can not handle this err normally, just log this err and break to End this dataset upload process!
                                //DatasetManager dataset_status_collector are dropped will touch this err!
                                error!("[DatasetChunksManager]: failed to send DataSetStatus to [DatasetManager], maybe [dataset_status_collector] are dropped!!! err:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                                e,dataset_id,dataset_version_id);
                                //break the loop, End this dataset upload process!!!
                                break;
                            }
                        }
                    }else{
                        rest_upload_size -= chunk_result.uploaded_size;
                        let uploaded_percent = 100.0 * (chunks_manager.upload_dataset.compressed_size - rest_upload_size) as f32 / chunks_manager.upload_dataset.compressed_size as f32;
                        let uploaded_percent_2d = (uploaded_percent * 100.0).round() / 100.0;
                        let dataset_status = DataSetStatus::Uploading(uploaded_percent_2d);
                        
                        debug!("[DatasetChunksManager]: send DataSetStatus: {:?} to [DatasetManager]. dataset_id:{:?}, dataset_version_id:{:?}", 
                        dataset_status, dataset_id,dataset_version_id);

                        let send_result = dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),dataset_status,0)).await;

                        match send_result {
                            std::result::Result::Ok(_) => {
                                debug!("[DatasetChunksManager]: succeed to send DataSetStatus to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}", 
                                dataset_id,dataset_version_id);
                            },
                            std::result::Result::Err(e) => {
                                //Can not handle this err normally, just log this err and break to End this dataset upload process!
                                //DatasetManager dataset_status_collector are dropped will touch this err!
                                error!("[DatasetChunksManager]: failed to send DataSetStatus to [DatasetManager], maybe [dataset_status_collector] are dropped!!! err:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                                e,dataset_id,dataset_version_id);
                                //break the loop, End this dataset upload process!!!
                                break;
                            }
                        }
                    }

                    if rest_upload_size == 0 {
                        info!("[DatasetChunksManager]: upload dataset blob success !!! dataset_id:{:?}, dataset_version_id:{:?}", 
                        dataset_id, dataset_version_id);

                        let merge_result = chunks_manager.merge_data_chunks().await;

                        info!("[DatasetChunksManager]: sent merge chunks cmd to server, result:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                        merge_result, dataset_id, dataset_version_id);
                        
                        match merge_result {
                            std::result::Result::Ok(_) => {
                                let dataset_status = DataSetStatus::AsyncProcessing;
                                info!("[DatasetChunksManager]: send DataSetStatus: {:?} to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}", 
                                dataset_status,dataset_id, dataset_version_id);
                                
                                let send_result = dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),dataset_status,0)).await;

                                match send_result {
                                    std::result::Result::Ok(_) => {
                                        info!("[DatasetChunksManager]: succeed to send DataSetStatus to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}", 
                                        dataset_id, dataset_version_id);
                                    },
                                    std::result::Result::Err(e) => {
                                        //Can not handle this err normally, just log this err and break to End this dataset upload process!
                                        //DatasetManager dataset_status_collector are dropped will touch this err!
                                        error!("[DatasetChunksManager]: failed to send DataSetStatus to [DatasetManager], maybe [dataset_status_collector] are dropped!!! err:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                                        e,dataset_id, dataset_version_id);
                                        //break the loop, End this dataset upload process!!!
                                        break;
                                    }
                                }
                            },
                            std::result::Result::Err(e)=> {

                                let dataset_status = DataSetStatus::Failed;

                                error!("[DatasetChunksManager]: send DataSetStatus: {:?} to [DatasetManager], Err:{},  dataset_id:{:?}, dataset_version_id:{:?}",
                                dataset_status,e, dataset_id, dataset_version_id);
                                
                                let send_result = dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),dataset_status,0)).await;

                                match send_result {
                                    std::result::Result::Ok(_) => {
                                        warn!("[DatasetChunksManager]: succeed to send DataSetStatus to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}",
                                        dataset_id, dataset_version_id);
                                        //DataSetStatus::Failed, break the loop, End this dataset upload process!!!
                                        break;
                                    },
                                    std::result::Result::Err(e) => {
                                        //Can not handle this err normally, just log this err and break to End this dataset upload process!
                                        //DatasetManager dataset_status_collector are dropped will touch this err!
                                        error!("[DatasetChunksManager]: failed to send DataSetStatus to [DatasetManager], maybe [dataset_status_collector] are dropped!!! err:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                                        e, dataset_id, dataset_version_id);
                                        //break the loop, End this dataset upload process!!!
                                        break;
                                    }
                                }
                            }
                        }
                    }
                },

                //Do not need to process shutdown_req err
                //Case if shutdown_sx is dropped, also need to shutdown
                _ = self.shutdown_rx.recv() => {
                    warn!("[DatasetChunksManager]: received shutdown cmd, stop [upload_chunks_manager] and send DataSetStatus::Stop to [DatasetManager] !!! binding dataset_id:{:?}, dataset_version_id:{:?}",
                    dataset_id, dataset_version_id);

                    let send_result = dataset_status_sender.send((dataset_id.clone(),dataset_version_id.clone(),DataSetStatus::Stop,0)).await;

                    match send_result {
                        std::result::Result::Ok(_) => {
                            warn!("[DatasetChunksManager]: succeed to send DataSetStatus to [DatasetManager], dataset_id:{:?}, dataset_version_id:{:?}", 
                            dataset_id,dataset_version_id);
                            //DataSetStatus::Stop, break the loop, End upload this dataset process!!!
                            break;
                        },
                        std::result::Result::Err(e) => {
                            //Can not handle this err normally, just log this err and break to End this dataset upload process!
                            //DatasetManager dataset_status_collector are dropped will touch this err!
                            error!("[DatasetChunksManager]: failed to send DataSetStatus to [DatasetManager], maybe [dataset_status_collector] are dropped!!! err:{:?}, dataset_id:{:?}, dataset_version_id:{:?}",
                            e,dataset_id,dataset_version_id);
                            //break the loop, End this dataset upload process!!!
                            break;
                        }
                    }
                }
            }
        }

        warn!("[DatasetChunksManager]: End !!! binding dataset_id:{:?}, dataset_version_id:{:?}",dataset_id, dataset_version_id);

        Ok(())
    }
}

struct DatasetChunksManager{
    dataset_id: String,
    dataset_version_id: String,
    //all dataset contain many chunks, concurrency from all dataset should be limited
    all_dataset_chunk_sema: Arc<Semaphore>,
    //one dataset also contain many chunks, concurrency form one dataset also should be limited
    one_dataset_chunk_sema: Arc<Semaphore>,
    upload_endpoint: String,
    upload_file_path: PathBuf,
    upload_dataset: DatasetMeta,
    //retain chunks_pusher, no all chunks_pusher are dropped to let receiver get None
    chunks_pusher: mpsc::Sender<DatasetChunk>,
    //retain chunk_result_sender, no all chunk_result_sender are dropped to let receiver get None
    chunk_result_sender:mpsc::Sender<DatasetChunkResult>,
    shutdown_cmd_suber: broadcast::Sender<()>,
}

impl Drop for DatasetChunksManager {
    fn drop(&mut self) {
        debug!("[DatasetChunksManager]: dataset chunks manager droped !!! dataset_id:{},dataset_version_id:{}",self.dataset_id,self.dataset_version_id);
    }
}

impl DatasetChunksManager {
    fn new(dataset_id:String,
           dataset_version_id:String,
           dataset_meta:DatasetMeta,
           all_dataset_chunk_sema:Arc<Semaphore>,
           endpoint:String,
           file_path:PathBuf,
           chunks_pusher:mpsc::Sender<DatasetChunk>,
           chunk_result_sender:mpsc::Sender<DatasetChunkResult>,
           shutdown_cmd_suber:broadcast::Sender<()>) -> Self {

        let one_dataset_chunk_sema = Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY+1));
        
        Self {
            dataset_id,
            dataset_version_id,
            all_dataset_chunk_sema,
            one_dataset_chunk_sema,
            upload_endpoint: endpoint,
            upload_file_path: file_path,
            upload_dataset: dataset_meta,
            chunks_pusher,
            chunk_result_sender,
            shutdown_cmd_suber,
        }
    }

    async fn create_data_chunk_producer(&self,chunks_pusher:mpsc::Sender<DatasetChunk>,chunk_result_sender:mpsc::Sender<DatasetChunkResult>) {

        let dataset_id = self.dataset_id.clone();
        let dataset_version_id = self.dataset_version_id.clone();
        let upload_dataset = self.upload_dataset.clone();
        let all_dataset_chunk_sema = self.all_dataset_chunk_sema.clone();
        let one_dataset_chunk_sema = self.one_dataset_chunk_sema.clone();
        let dataset_compressed_size = upload_dataset.compressed_size;
        let dataset_chunk_size= self.upload_dataset.chunk_size;
        let upload_endpoint= self.upload_endpoint.clone();
        let upload_file_path= self.upload_file_path.clone();

        tokio::spawn(async move {

            let mut chunk_seek_start  = 0u64;
            let mut chunk_num  = 0u64;

            //if while is end normally, chunks_pusher will dropped.
            //but not all dataset chunks_pushers are dropped, DatasetChunksManager retain a chunks_pusher!
            //dataset chunks_getter will not receive None datachunk!
            while chunk_seek_start <= dataset_compressed_size-1 {

                //Not all dataset chunk_result_sender are dropped, DatasetChunksManager retain a chunk_result_sender!
                let dc = DatasetChunk::new(dataset_id.clone(),
                                           dataset_version_id.clone(),
                                           upload_dataset.clone(),
                                           chunk_num,
                                           chunk_seek_start,
                                           upload_endpoint.clone(),
                                           upload_file_path.clone(),
                                           all_dataset_chunk_sema.clone(),
                  one_dataset_chunk_sema.clone(),
                                           chunk_result_sender.clone());

                if chunks_pusher.send(dc).await.is_err() {
                    //chunk_producer will shutdown after chunk_consumer_and_task_creator shutdown!
                    //normally will not occur this err, only activately send shutdown cmd!
                    warn!("[DatasetChunksManager]:[chunks_producer], failed to send datachunk to chan, maybe [chunks_getter] dropped by [chunk_consumer_and_task_creator] shutdown, stop [chunks_producer]!!! binding dataset_id:{:?},dataset_version_id:{:?}",
                    dataset_id,dataset_version_id);
                    //break to End this dataset upload process!
                    //ToDo: process this err?
                    break;
                }

                chunk_seek_start += dataset_chunk_size as u64;
                chunk_num += 1;
            }

            warn!("[DatasetChunksManager]:[chunks_producer] End !!! binding dataset_id:{:?},dataset_version_id:{:?}",dataset_id,dataset_version_id);
        });

    }

    async fn create_data_chunk_consumer(&mut self,mut chunks_chan_getter:mpsc::Receiver<DatasetChunk>) {

        let dataset_id = self.dataset_id.clone();
        let dataset_version_id = self.dataset_version_id.clone();
        let create_chunk_task_sema_by_one_dataset = self.one_dataset_chunk_sema.clone();
        let mut shutdown_cmd_rx = self.shutdown_cmd_suber.subscribe();

        tokio::spawn(async move {
            
            loop{
                tokio::select! {
                    //chunks_chan_getter will not get None, cause will not all chunks_pushers are dropped before
                    //DatasetChunksManager retain a no use chunks_pusher
                    //DatasetChunksManager will not dropped before this process!
                    Some(dc) = chunks_chan_getter.recv() => {

                        debug!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: request permit, one_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
                            create_chunk_task_sema_by_one_dataset.available_permits(),dataset_id,dataset_version_id,dc.chunk_num);

                        let permited_by_one_dataset_result = create_chunk_task_sema_by_one_dataset.acquire().await;

                        let chunk_result_sender = dc.upload_result_sender.clone();
                        let dataset_id = dc.dataset_id.clone();
                        let dataset_version_id = dc.dataset_version_id.clone();
                        let chunk_num = dc.chunk_num.clone();


                        //Do not create many upload_chunk task entity, and then let them acquire one_dataset sema!
                        match permited_by_one_dataset_result {
                            std::result::Result::Ok(permited_by_one_dataset) => {

                                debug!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: permitted by one_dataset! one_dataset_chunk_sema available permit rest:{:?},dataset_id:{:?},dataset_version_id:{:?},chunk_num:{}",
                                create_chunk_task_sema_by_one_dataset.available_permits(),dc.dataset_id,dc.dataset_version_id,chunk_num);

                                drop(permited_by_one_dataset);

                                debug!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: drop permitted by one_dataset! one_dataset_chunk_sema available permit rest:{:?},dataset_id:{:?},dataset_version_id:{:?},chunk_num:{}",
                                create_chunk_task_sema_by_one_dataset.available_permits(),dc.dataset_id,dc.dataset_version_id,chunk_num);

                                tokio::spawn(async move {

                                    let upload_result_sender = dc.upload_result_sender.clone();
                                    let dataset_id = dc.dataset_id.clone();
                                    let dataset_version_id = dc.dataset_version_id.clone();
                                    let chunk_num = dc.chunk_num.clone();

                                    let result = Self::create_upload_chunk_task(dc.clone()).await;

                                    debug!("[DatasetChunksManager]:[create_upload_chunk_task] return !!! one_dataset_chunk_sema available permit rest:{:?},all_dataset_chunk_sema available permit rest:{:?},result:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
                                    dc.one_dataset_chunk_sema.available_permits(),dc.all_dataset_chunk_sema.available_permits(),result,dataset_id,dataset_version_id,chunk_num);

                                    match result {
                                        std::result::Result::Ok(upload_chunk_result) => {
                                            debug!("[DatasetChunksManager]:[upload_chunk_task_return]: upload chunk success, chunk num:{:?}, dataset_id:{:?},dataset_version_id:{:?}",
                                            chunk_num.to_string(),dataset_id,dataset_version_id);

                                            let send_result = upload_result_sender.send(upload_chunk_result).await;
                                            
                                            match send_result {
                                                std::result::Result::Ok(_) => {
                                                    debug!("[DatasetChunksManager]:[upload_chunk_task_return]: success to send upload_chunk_result to [DtataChunksManager], chunk num:{:?}, dataset_id:{:?},dataset_version_id:{:?}",
                                                    chunk_num.to_string(),dataset_id,dataset_version_id);
                                                },
                                                std::result::Result::Err(e) => {
                                                    //Only-One-Reason to failed to send upload_chunk_result to [DtataChunksManager]
                                                    //is DtataChunksManager shutdown by [DtataManager] cmd!!!
                                                    //maybe this err is normally by [DtataManager] shutdown cmd
                                                    //so just warn log this err for process!!!
                                                    warn!("[DatasetChunksManager]:[upload_chunk_task_return]: failed to send upload_chunk_result to [DtataChunksManager], Only-One-Reason is [DtataChunksManager] shutdown by [DtataManager] cmd!!!  chunk num:{:?}, err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                                    chunk_num.to_string(),e,dataset_id,dataset_version_id);
                                                }
                                            }
                                        },
                                        std::result::Result::Err(e)=> {

                                            error!("[DatasetChunksManager]:[upload_chunk_task_return]: Operate-System-Error,send to [DataChunksManager] to process! chunk num:{:?},err:{}, dataset_id:{:?},dataset_version_id:{:?}",
                                            chunk_num.to_string(),e,dataset_id,dataset_version_id);

                                            //Err(upload_chunk_result) will set uploaded_size = 0
                                            let upload_chunk_result = DatasetChunkResult::new(
                                                0,
                                                Err(anyhow!("[DatasetChunksManager]:[upload_chunk_task_return]: Operate-System-Error,chunk num:{:?},err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                                chunk_num.to_string(),e,dataset_id,dataset_version_id))
                                            );

                                            let send_result = upload_result_sender.send(upload_chunk_result).await;

                                            match send_result {
                                                std::result::Result::Ok(_) => {
                                                    debug!("[DatasetChunksManager]:[upload_chunk_task_return]: success to send Err(upload_chunk_result) to [DtataChunksManager], chunk num:{:?}, dataset_id:{:?},dataset_version_id:{:?}",
                                                    chunk_num.to_string(),dataset_id,dataset_version_id);
                                                },
                                                std::result::Result::Err(e) => {
                                                    //Only-One-Reason to failed to send upload_chunk_result to [DtataChunksManager]
                                                    //is DtataChunksManager shutdown by [DtataManager] cmd!!!
                                                    //maybe this err is normally by [DtataManager] shutdown cmd
                                                    //so just warn log this err for process!!!
                                                    warn!("[DatasetChunksManager]:[upload_chunk_task_return]: failed to send upload_chunk_result to [DtataChunksManager],Only-One-Reason is [DtataChunksManager] shutdown by [DtataManager] cmd!!! chunk num:{:?},err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                                    chunk_num.to_string(),e,dataset_id,dataset_version_id);
                                                }
                                            }
                                        }
                                    }

                                    //All upload_chunk_task Result(Ok,Err) had processed! 
                                    Ok(())
                                });
                            },
                            std::result::Result::Err(e) => {
                                error!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: failed to permit create chunk task of one_dataset, stop this dataset upload process!!! chunk num:{:?},err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                chunk_num.to_string(),e,dataset_id,dataset_version_id);

                                //consume one datachunk of one dataset, should handle this err, send Err(upload_chunk_result) to DtataChunksManager!
                                //Err(upload_chunk_result) will set uploaded_size = 0
                                let upload_chunk_result = DatasetChunkResult::new(
                                    0,
                                    Err(anyhow!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: failed to permit create chunk task of one_dataset,chunk num:{:?},err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                    chunk_num.to_string(),e,dataset_id,dataset_version_id))
                                );

                                let send_result = chunk_result_sender.send(upload_chunk_result).await;

                                match send_result {
                                    std::result::Result::Ok(_) => {
                                        debug!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: success to send Err(upload_chunk_result) to [DtataChunksManager], chunk num:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                        chunk_num.to_string(),dataset_id,dataset_version_id);
                                    },
                                    std::result::Result::Err(e) => {
                                        //Only-One-Reason to failed to send upload_chunk_result to [DtataChunksManager]
                                        //is DtataChunksManager shutdown by [DtataManager] cmd!!!
                                        //maybe this err is normally by [DtataManager] shutdown cmd
                                        //so just warn log this err for process!!!
                                        warn!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: failed to send upload_chunk_result to [DtataChunksManager],Only-One-Reason is [DtataChunksManager] shutdown by [DtataManager] cmd!!! chunk num:{:?},err:{:?}. dataset_id:{:?},dataset_version_id:{:?}",
                                        chunk_num.to_string(),e,dataset_id,dataset_version_id);
                                    }
                                }
                            }
                        }
                    },
                    _ = shutdown_cmd_rx.recv() => {
                        warn!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: received [DatasetManager] shutdown cmd, stop this dataset upload process!!! binding dataset_id:{:?},dataset_version_id:{:?}",
                        dataset_id,dataset_version_id);
                        //Do not need to process shutdown_req err
                        //Case if shutdown_sx is dropped, also need to shutdown chunk_consumer_and_task_creator
                        //break to stop this dataset upload process!!!
                        break;
                    }
                }
            }

            warn!("[DatasetChunksManager]:[chunk_consumer_and_task_creator]: End !!! binding dataset_id:{:?},dataset_version_id:{:?}",
            dataset_id,dataset_version_id);

        });
    }
   
    async fn merge_data_chunks(&self) -> Result<()>{

        let digest = DatasetDigest::new("urfs".to_string(),self.upload_dataset.digest.clone());

        //ToDo: digester not ztd! shold be rename to urfs or else
        let form = multipart::Form::new()
            .text("mode",DataMode::ChunkEnd.to_string())
            .text("dataset_id",self.dataset_id.clone())
            .text("dataset_version_id",self.dataset_version_id.clone())
            .text("digest",digest.to_string())
            .text("total_size",self.upload_dataset.compressed_size.to_string())
            .text("chunk_size",self.upload_dataset.chunk_size.to_string());

        let httpclient = get_http_client()?;

        let upload_chunk_end_url = self.upload_endpoint.clone()+"/api/v1/file/upload";

        info!("[DatasetChunksManager]:[merge_chunks]: send merge chunks cmd to server url {:?}. dataset_id:{:?},dataset_version_id:{:?}",
                 upload_chunk_end_url,self.dataset_id,self.dataset_version_id);

        let resp = httpclient
            .put(upload_chunk_end_url)
            .multipart(form)
            .send().await?;

        let resp_txt = resp.text().await?;

        //ToDo: process response err？
        info!("[DatasetChunksManager]:[merge_chunks]: get normal response from server resp: {:?}. dataset_id:{:?},dataset_version_id:{:?}",
        resp_txt,self.dataset_id,self.dataset_version_id);

        Ok(())
        
    }

    pub async fn create_upload_chunk_task(data_chunk:DatasetChunk) -> Result<DatasetChunkResult> {

        debug!("[UploadChunkTask]: request permit, one_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
        data_chunk.one_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        let permit_by_one_dataset = data_chunk.one_dataset_chunk_sema.acquire().await?;

        debug!("[UploadChunkTask]: permitted by one_dataset! one_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
            data_chunk.one_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        debug!("[UploadChunkTask]: request permit, all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
            data_chunk.all_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        let permit_by_all_dataset = data_chunk.all_dataset_chunk_sema.acquire().await?;

        debug!("[UploadChunkTask]: permitted by all_dataset! all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
            data_chunk.all_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        let mut chunk_length = data_chunk.dataset.chunk_size as usize;
        let mut chunk_buffer = vec![0;chunk_length];
        let chunk_end = data_chunk.chunk_seek_start+data_chunk.dataset.chunk_size;
        if chunk_end > data_chunk.dataset.compressed_size {
            chunk_length = (data_chunk.dataset.compressed_size-data_chunk.chunk_seek_start) as usize;
            chunk_buffer = vec![0;chunk_length];
        }

        info!("[UploadChunkTask]: ready to upload chunk, chunk_num:{} chunk_start:{} chunk_end:{} total_size:{}, dataset_id:{},dataset_version_id:{}",
                data_chunk.chunk_num,data_chunk.chunk_seek_start,chunk_end,data_chunk.dataset.compressed_size, data_chunk.dataset_id,data_chunk.dataset_version_id);

        let digest = DatasetDigest::new("urfs".to_string(),data_chunk.dataset.digest.clone());
        let dataset_id = data_chunk.dataset_id.as_str();
        let dataset_version_id = data_chunk.dataset_version_id.as_str();
        let data_mode = DataMode::Chunk.to_string();
        let server_endpoint = data_chunk.upload_endpoint.as_str();
        let total_size = data_chunk.dataset.compressed_size;
        let chunk_file_size = chunk_length as u64;
        let chunk_num = data_chunk.chunk_num;
        let chunk_start = data_chunk.chunk_seek_start;

        let chunk_file_status = stat_chunk_file(server_endpoint,data_mode.as_str(),
                                                        dataset_id,dataset_version_id,
                                                  digest.clone(),total_size,
                                                chunk_file_size,chunk_start,chunk_num).await?;

        info!("[UploadChunkTask]:[stat_chunk_file]: check chunk file status :{:?},chunk num:{:?}. dataset_id:{},dataset_version_id:{}",
        chunk_file_status.to_string(),data_chunk.chunk_num.to_string(),data_chunk.dataset_id,data_chunk.dataset_version_id);

        let upload_chunk_result;
        //ToDo: retry upload add reliability?
        if chunk_file_status == UrchinFileStatus::UnKnown {
            warn!("[UploadChunkTask]: Upload-Chunk-Err,send to [DataChunksManager] to process! chunk file status: {:?},chunk num:{:?}. dataset_id:{},dataset_version_id:{}",
            chunk_file_status.to_string(),data_chunk.chunk_num.to_string(),data_chunk.dataset_id,data_chunk.dataset_version_id);

            //Err(upload_chunk_result) will set uploaded_size = 0
            upload_chunk_result = DatasetChunkResult::new(
                0,
                Err(anyhow!("[UploadChunkTask]: upload chunk err,chunk file status: {:?},chunk num:{:?}. dataset_id:{},dataset_version_id:{}",
                chunk_file_status.to_string(),data_chunk.chunk_num.to_string(),data_chunk.dataset_id,data_chunk.dataset_version_id))
            );

        }else if chunk_file_status == UrchinFileStatus::Exist {
            info!("[UploadChunkTask]: upload chunk file exist, finish immediately, chunk num:{:?}. dataset_id:{},dataset_version_id:{}",
            data_chunk.chunk_num.to_string(),data_chunk.dataset_id,data_chunk.dataset_version_id);

            upload_chunk_result = DatasetChunkResult::new(
                chunk_file_size,
                Ok(())
            );

        }else{
            //Chunk File NotFound will upload once
            //Chunk File Partial will upload overwrite
            info!("[UploadChunkTask]: chunk file NotFound or Partial, ready to upload chunk file to server, chunk num:{:?}. dataset_id:{},dataset_version_id:{}",
             data_chunk.chunk_num.to_string(),data_chunk.dataset_id,data_chunk.dataset_version_id);
            let mut dataset_file= File::open(data_chunk.upload_file_path.as_path()).await?;

            let dataset_file_name = data_chunk.upload_file_path.file_name().unwrap().to_str().unwrap().to_string();

            let _ = dataset_file.seek(SeekFrom::Start(data_chunk.chunk_seek_start)).await?;

            dataset_file.read(&mut chunk_buffer).await?;

            let file_part = multipart::Part::bytes(chunk_buffer)
                .file_name(dataset_file_name)
                .mime_str("application/octet-stream")?;

            let form = multipart::Form::new()
                .part("file", file_part)
                .text("mode",DataMode::Chunk.to_string())
                .text("dataset_id",data_chunk.dataset_id.clone())
                .text("dataset_version_id",data_chunk.dataset_version_id.clone())
                .text("digest",digest.to_string())
                .text("total_size",data_chunk.dataset.compressed_size.to_string())
                .text("chunk_size",chunk_file_size.to_string())
                .text("chunk_start",data_chunk.chunk_seek_start.to_string())
                .text("chunk_num",data_chunk.chunk_num.to_string());

            let httpclient = get_http_client()?;

            let upload_chunk_url = data_chunk.upload_endpoint+"/api/v1/file/upload";

            debug!("[UploadChunkTask]: upload chunk file to server, chunk num:{:?},url {:?}. dataset_id:{},dataset_version_id:{}", 
            data_chunk.chunk_num.to_string(),upload_chunk_url,data_chunk.dataset_id,data_chunk.dataset_version_id);

            let resp = httpclient
                .put(upload_chunk_url)
                .multipart(form)
                .send().await?;


            debug!("[UploadChunkTask]: upload chunk file to server finish, resp: {:?}. dataset_id:{},dataset_version_id:{},chunk num:{:?}",
            resp,data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num.to_string());

            //ToDo: upload to server finish is upload success?
            let resp_txt = resp.text().await?;

            upload_chunk_result = DatasetChunkResult::new(
                chunk_file_size,
                Ok(())
            );
        }

        drop(permit_by_one_dataset);

        debug!("[UploadChunkTask]: drop permitted by one_dataset! one_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
            data_chunk.one_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        drop(permit_by_all_dataset);

        debug!("[UploadChunkTask]: drop permitted by all_dataset! all_dataset_chunk_sema available permit rest:{:?}, dataset_id:{},dataset_version_id:{},chunk_num:{}",
            data_chunk.all_dataset_chunk_sema.available_permits(),data_chunk.dataset_id,data_chunk.dataset_version_id,data_chunk.chunk_num);

        Ok(upload_chunk_result)
    }
}

#[derive(Clone, Debug)]
struct DatasetChunk{
    dataset_id: String,
    dataset_version_id: String,
    dataset: DatasetMeta,
    chunk_num: u64,
    chunk_seek_start: u64,
    upload_endpoint: String,
    upload_file_path: PathBuf,
    upload_result_sender: mpsc::Sender<DatasetChunkResult>,
    //all dataset contain many chunks, concurrency from all dataset should be limited
    all_dataset_chunk_sema: Arc<Semaphore>,
    //one dataset also contain many chunks, concurrency form one dataset also should be limited
    one_dataset_chunk_sema: Arc<Semaphore>,
}

impl DatasetChunk {
    fn new(dataset_id:String,
           dataset_version_id:String,
           ds: DatasetMeta,
           num: u64,
           start:u64,
           endpoint:String,
           file_path: PathBuf,
           all_dataset_chunk_sema:Arc<Semaphore>,
           one_dataset_chunk_sema:Arc<Semaphore>,
           result_sender:mpsc::Sender<DatasetChunkResult>) -> Self {

        Self {
            dataset_id,
            dataset_version_id,
            dataset: ds,
            chunk_num: num,
            chunk_seek_start: start,
            upload_endpoint: endpoint,
            upload_file_path: file_path,
            upload_result_sender: result_sender,
            all_dataset_chunk_sema,
            one_dataset_chunk_sema,
        }
    }
}


#[derive(Debug)]
pub struct DatasetChunkResult{
    uploaded_size: u64,
    upload_status: Result<()>
}

impl DatasetChunkResult {
    pub fn new(uploaded_size:u64,upload_status: Result<()>) -> Self {

        Self {
            uploaded_size,
            upload_status
        }
    }
}

#[derive(Clone, Debug, Default,Serialize,Deserialize)]
pub struct DatasetMeta {
    /// A sha256 hex string digest as blob file
    #[serde(alias = "blob_id")]
    digest: String,
    /// Message digest algorithm to process the blob.
    digester: String,
    /// Size of the compressed blob file.
    compressed_size: u64,
    /// Compression algorithm to process the blob.
    compressor: String,
    /// Size of the uncompressed blob file, or the cache file.
    uncompressed_size: u64,
    /// Chunk size.
    chunk_size: u64,
    /// Number of chunks in blob file.
    /// A helper to distinguish bootstrap with extended blob table or not:
    /// Bootstrap with extended blob table always has non-zero `chunk_count`
    chunk_count: u64,
}

impl DatasetMeta{

    pub fn maxsize() -> Result<String> {

        let json = serde_json::to_string( &json!({
            "max_size:": (RAFS_MAX_CHUNK_SIZE >> 20) * (RAFS_MAX_CHUNKS_PER_BLOB >> 20) as u64,
        }))?;

        Ok(json)
    }
}