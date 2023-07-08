use std::collections::HashMap;
use std::fmt::{Debug};
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

use std::fmt;
use nydus_utils::digest;

use crate::upload_backend_type::UiResponse;
use crate::upload_backend_type::UiStartUploadDatasetRequest;
use crate::upload_backend_type::UiStopUploadDatasetRequest;

#[derive(Debug,Clone)]
pub enum DataSetStatus{
    Init,
    Uploading(f32),
    AsyncProcessing,
    Success,
    Failed,
    UnKnown,
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
struct Digest{
    algorithm: String,
    hash: String
}
impl Digest {
    pub fn new(algo:String, hash_str: String) -> Self {
        Self {
            algorithm: algo,
            hash: hash_str,
        }
    }
}

impl fmt::Display for Digest {
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

async fn stat_file(server_endpoint: &str,mode:&str,dataset_id:&str,dataset_version_id:&str,digest: &Digest,total_size:u64) -> Result<UrchinFileStatus> {

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

    println!("[stat_file] url {:?}",stat_url);

    let resp: StatFileResponse = httpclient
        .get(stat_url)
        .send()
        .await?
        .json()
        .await?;

    println!("[stat_file] result:{:?}",resp);

    let status_code = resp.status_code.into();

    match status_code {
        UrchinStatusCode::Exist | UrchinStatusCode::NotFound | UrchinStatusCode::PartialUploaded  => Ok(status_code.into()),
        _ => Ok(UrchinFileStatus::UnKnown)
    }

}

async fn stat_chunk_file(server_endpoint: &str,mode:&str,dataset_id:&str,dataset_version_id:&str,
                         digest: Digest,total_size:u64,
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

    println!("[stat_chunk_file]: stat_chunk_file url {:?}",stat_url);

    let resp: StatFileResponse = httpclient
        .get(stat_url)
        .send()
        .await?
        .json()
        .await?;

    println!("stat_chunk_file, result:{:?}!!!",resp);

    let status_code = resp.status_code.into();

    match status_code {
        UrchinStatusCode::Exist | UrchinStatusCode::NotFound | UrchinStatusCode::PartialUploaded  => Ok(status_code.into()),
        _ => Ok(UrchinFileStatus::UnKnown)
    }

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
    upload_dataset_history: HashMap<String,DataSetStatus>,
    all_dataset_chunk_sema :Arc<Semaphore>,
    dataset_status_sender: mpsc::Sender<(String,DataSetStatus)>,
    dataset_status_collector: mpsc::Receiver<(String,DataSetStatus)>,
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
            upload_dataset_history: HashMap::new(),
            all_dataset_chunk_sema: Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY)),
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
                Some(dataset_status) = self.dataset_status_collector.recv() => {
                    println!("[upload_manager]: received dataset_status: {:?}", dataset_status);
        
                    self.add_dataset_status("x".to_string(), dataset_status.1);
                },
                Some((cmd,req_json,resp_sender)) =  self.ui_cmd_collector.recv() => {
                    
                    match cmd.as_str() {
                        "start_upload" => {
                            println!("[DatasetManager]:upload cmd processor received request: {:?}", req_json);
                            
                            let result = self.start_dataset_uploader(req_json).await;
                            
                            match result {
                                std::result::Result::Ok(_) => {
                                   let resp = UiResponse{status_code: 0, status_msg:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        println!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                        error!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                   }
                                },
                                std::result::Result::Err(e)=> {
                                   let resp = UiResponse{status_code: 0, status_msg:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        println!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                        error!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                   }
                                }
                            }
                        },
                        "stop_upload" => {
                            println!("[DatasetManager]:upload cmd processor received request: {:?}", req_json);
                            let result =  self.stop_dataset_uploader(req_json).await;

                            match result {
                                std::result::Result::Ok(_) => {
                                   let resp = UiResponse{status_code: 0, status_msg:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        println!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                        error!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                   }
                                },
                                std::result::Result::Err(e)=> {
                                   let resp = UiResponse{status_code: 0, status_msg:"".to_string()};
                                   if resp_sender.send(resp).is_err(){
                                        //Do not need process next step, here is Err-Topest-Process Layer!
                                        println!("[DatasetManager]: ui {} cmd resp channel err: {}", cmd,e);
                                        error!("[DatasetManager]: ui {} cmd resp channel err: {}", cmd,e);
                                   }
                                }
                            }
                        },
                        "get_history" => {

                            self.get_history();

                            let resp = UiResponse{status_code: 0, status_msg:"".to_string()};
                            if resp_sender.send(resp).is_err() {
                                println!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                error!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                            }
                        },
                        _ => {
                            error!("[DatasetManager]: unknown cmd: {}", cmd);
                            let resp = UiResponse{status_code: -1, status_msg:"unknown cmd".to_string()};
                            if resp_sender.send(resp).is_err() {
                                println!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                                error!("[DatasetManager]: ui {} cmd resp channel err", cmd);
                            }
                        }
                    }
                },
            }
        }
    }

    fn add_dataset_status(&mut self,dataset_id:String,status:DataSetStatus) {
        self.upload_dataset_history.insert(dataset_id,status);
    }

    fn get_history(&self) {
       println!("get_history:{:?}",self.upload_dataset_history); 
    }

    async fn start_dataset_uploader(&mut self, req_json:String) -> Result<()> {

        let req =  serde_json::from_str::<UiStartUploadDatasetRequest>(&req_json)?;

        let dataset_image_path = Path::new("/Users/terrill/Documents/urchin/zhangshuiyong/urfs/tests/cifar-10-image");

        ensure_directory(dataset_image_path.clone())?;

        let dataset_meta_path = dataset_image_path.join("meta");
        let dataset_blob_path = dataset_image_path.join("blob");

        println!("dataset_image_dir:{:?},dataset_meta_path: {:?}",dataset_image_path,dataset_meta_path);

        let blobs_info_json =  inspect_blob_info(dataset_meta_path.as_path())?;

        let dataset_metas: Vec<DatasetMeta> =  serde_json::from_str(blobs_info_json.as_str())?;

        let dataset_meta = &dataset_metas[0];
        let upload_dataset_meta = dataset_meta.clone();

        let upload_server_endpoint = "http://0.0.0.0:65004".to_string();

        let (uploader_shutdown_cmd_sx,uploader_shutdown_cmd_rx) = broadcast::channel(1);
        
        let uploader_shutdown_cmd_suber = uploader_shutdown_cmd_sx.clone();

        self.dataset_uploader_shutdown_cmd_senders.insert("xx".to_string(),uploader_shutdown_cmd_sx);

        let dataset_status_sender = self.dataset_status_sender.clone();

        //Concurent upload futures tree
        tokio::spawn(async move {
            
            let mut uploader = DatasetUploader::new(dataset_status_sender,
                                                                    uploader_shutdown_cmd_suber,
                                                                    uploader_shutdown_cmd_rx);

            //ToDo: process upload result Error!!!
            uploader.upload(dataset_meta_path, upload_dataset_meta, dataset_blob_path, upload_server_endpoint).await?;

            Ok(())
        });

        Ok(())

    }

    async fn stop_dataset_uploader(&self, req_json:String) -> Result<()> {

        let req =  serde_json::from_str::<UiStopUploadDatasetRequest>(&req_json)?;
        
        let try_uploader_shutdown_sx = self.dataset_uploader_shutdown_cmd_senders.get("xx");
        
        match try_uploader_shutdown_sx {
            Some(shutdown_sx) => {
                let uploader_shutdown_sx = shutdown_sx.clone();

                println!("[stop_dataset_uploader]: send dataset shutdown cmd !!!");
                if uploader_shutdown_sx.send(()).is_err() {
                    //ToDo: process error log
                    println!("[stop_dataset_uploader]: send dataset shutdown cmd err");
                    return Err(anyhow!("[stop_dataset_uploader]: send dataset shutdown cmd err"));
                }
            },
            None => {
                println!("[stop_dataset_uploader]: dataset_uploader_cmd_senders not found dataset_id: {:?}", req.dataset_id);
                return Err(anyhow!("[stop_dataset_uploader]: dataset_uploader_cmd_senders not found dataset_id: {:?}", req.dataset_id));
            }
        }

        Ok(())
    }

}

//Support HTTP/HTTPS
pub struct DatasetUploader {
    all_dataset_chunk_sema :Arc<Semaphore>,
    dataset_status_sender: mpsc::Sender<(String,DataSetStatus)>,
    shutdown_suber: broadcast::Sender<()>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl Drop for DatasetUploader {
    fn drop(&mut self) {
        println!("[DatasetUploader]: dataset uploader droped !!!");
    }
}

impl DatasetUploader {
    pub fn new(dataset_status_sender: mpsc::Sender<(String,DataSetStatus)>,shutdown_suber: broadcast::Sender<()>,shutdown_rx: broadcast::Receiver<()>) -> Self {

        Self {
            dataset_status_sender,
            all_dataset_chunk_sema: Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY)),
            shutdown_suber,
            shutdown_rx,
        }
    }

    pub async fn upload(&mut self, dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,dataset_blob_path:PathBuf,server_endpoint: String) -> Result<()> {

        println!("dataset_meta_path: {:?}",dataset_meta_path);
        println!("dataset_blob_path: {:?}",dataset_blob_path);

        println!("dataset_meta info:{:?}",dataset_meta);
        println!("dataset_meta max size:{:?} TB",DatasetMeta::maxsize()?);

        let dataset_status = DataSetStatus::Init;

        println!("[upload] send dataset_status:{:?} to [DatasetManager]", dataset_status);

        self.dataset_status_sender.send((dataset_meta.id.clone(),dataset_status)).await?;

        //ToDo: diff Datasetid & Dataset digest

        self.upload_meta("xxx".to_string(),"default".to_string(),dataset_meta_path,dataset_meta.clone(),server_endpoint.clone()).await?;

        self.upload_blob("xxx".to_string(),"default".to_string(), dataset_blob_path,dataset_meta.clone(),server_endpoint.clone()).await?;

        Ok(())
    }

    pub async fn upload_meta(&self,dataset_id:String, dataset_version_id:String, dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,server_endpoint: String) -> Result<()> {

        let mut meta_file = File::open(dataset_meta_path.as_path()).await?;
        let local_meta_file_info = meta_file.metadata().await?;
        println!("[upload_meta]: local meta file info {:?}", local_meta_file_info);

        let digest = Digest::new("urfs".to_string(),dataset_meta.id.clone());
        let meta_file_size = local_meta_file_info.len();
        let data_mode = DataMode::Meta.to_string();

        let meta_file_status = stat_file(server_endpoint.as_str(),data_mode.as_str(),
                                         dataset_id.as_str(),dataset_version_id.as_str(),
                                         &digest,meta_file_size).await?;

        //ToDo: process UnKnown File Status!!!
        if meta_file_status == UrchinFileStatus::UnKnown {
            println!("[stat_file] Meta file status is unknown, please check and stop upload process !!! ");
        }else if meta_file_status == UrchinFileStatus::Exist {
            println!("[stat_file] Meta file exist in backend, upload Finish !!! ");

            let dataset_status = DataSetStatus::Success;

            println!("[upload] send dataset_status:{:?} to [DatasetManager]", dataset_status);

            //ToDo: process dif datasetid & dataset_meta.id!!!
            self.dataset_status_sender.send((dataset_meta.id.clone(),dataset_status)).await?;

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
                .text("dataset_id", dataset_id)
                .text("dataset_version_id", dataset_version_id)
                .text("digest", digest.to_string())
                .text("total_size", meta_file_size.to_string());

            let httpclient = get_http_client()?;

            let upload_meta_url = server_endpoint + "/api/v1/file/upload";

            println!("[upload_meta]: upload meta url {:?}", upload_meta_url);

            let resp = httpclient
                .put(upload_meta_url)
                .multipart(form)
                .send().await?;

            //ToDo: process meta file upload response!
            let result = resp.text().await?;

            println!("upload dataset_meta finish, result:{}!!!", result);
        }

        Ok(())
    }

    async fn upload_blob(&mut self, dataset_id:String, dataset_version_id:String,dataset_blob_path: PathBuf,dataset_meta:DatasetMeta,server_endpoint: String) -> Result<()> {

        let digest = Digest::new("urfs".to_string(),dataset_meta.id.clone());

        let blob_file_status = stat_file(server_endpoint.as_str(),DataMode::Blob.to_string().as_str(),
                                         dataset_id.as_str(),dataset_version_id.as_str(),
                                         &digest,dataset_meta.compressed_size).await?;

        //ToDo: process UnKnown File Status!!!
        if blob_file_status == UrchinFileStatus::UnKnown {
            println!("[stat_file] Blob file status is unknown, please check and stop upload process !!! ");
        }else if blob_file_status == UrchinFileStatus::Exist{
            println!("[stat_file] Blob file exist in backend, upload Finish !!! ");
        }else{
            //may be NotFoundFile or Partial
            println!("[stat_file] not found blob file or partial upload, go to upload blob process");
            let all_dataset_chunk_sema = self.all_dataset_chunk_sema.clone();
            self.create_blob_chunks_manager(dataset_id,dataset_version_id,all_dataset_chunk_sema,dataset_meta,dataset_blob_path,server_endpoint).await?;
        }

        Ok(())
    }

    async fn create_blob_chunks_manager(&mut self,dataset_id:String, dataset_version_id:String, all_dataset_chunk_sema:Arc<Semaphore>,chunks_dataset:DatasetMeta,dataset_file_path: PathBuf,upload_endpoint: String) -> Result<()> {

        let dataset_status_sender = self.dataset_status_sender.clone();
        let chunks_manager_shutdown_suber = self.shutdown_suber.clone();

        let mut chunks_manager = DatasetChunksManager::new(dataset_id,
                                                                                 dataset_version_id,
                                                                                 chunks_dataset,
                                                                                 all_dataset_chunk_sema,
                                                                                 upload_endpoint,
                                                                                 dataset_file_path,
                                                                                 chunks_manager_shutdown_suber);

        let (chunk_pusher, chunk_getter) = mpsc::channel(100);
        let (chunk_result_sx,mut chunk_result_collector) = mpsc::channel(100);
       
        chunks_manager.create_data_chunk_producer(chunk_pusher,chunk_result_sx).await;
        chunks_manager.create_data_chunk_consumer(chunk_getter).await;

        let mut rest_upload_size = chunks_manager.upload_dataset.compressed_size;

        loop{
            tokio::select! {
                try_chunk_result = chunk_result_collector.recv() => {
                    match try_chunk_result {
                        Some(chunk_result) => {
                            
                            println!("[upload_chunks_manager]: received chunk_result: {:?}", chunk_result);

                            if chunk_result.upload_status.is_ok(){
                                rest_upload_size -= chunk_result.uploaded_size;
                                //ToDo: Update Dataset Upload Status
                                let uploaded_percent = 100.0 * (chunks_manager.upload_dataset.compressed_size - rest_upload_size) as f32 / chunks_manager.upload_dataset.compressed_size as f32;
                                let uploaded_percent_2d = (uploaded_percent * 100.0).round() / 100.0;
                                let dataset_status = DataSetStatus::Uploading(uploaded_percent_2d);
                                
                                println!("[upload_chunks_manager]: send DataSetStatus: {:?} to [DatasetManager]", dataset_status);

                                dataset_status_sender.send((chunks_manager.dataset_id.clone(),dataset_status)).await?;

                            }

                            //ToDo: process upload_status is err

                            if rest_upload_size == 0 {
                                println!("[upload_chunks_manager]: upload dataset blob success !!!");

                                //ToDo: retry to merge chunks if failed!!!
                                let merge_result = chunks_manager.merge_data_chunks().await;

                                println!("[upload_chunks_manager]: dataset sent chunks merge cmd, result:{:?}!!!",merge_result);
                                
                                match merge_result {
                                    std::result::Result::Ok(_) => {
                                        let dataset_status = DataSetStatus::AsyncProcessing;
                                        println!("[upload_chunks_manager]: send DataSetStatus: {:?} to [DatasetManager]", dataset_status);
                                        dataset_status_sender.send((chunks_manager.dataset_id.clone(),dataset_status)).await?;
                                    },
                                    std::result::Result::Err(e)=> {
                                        let dataset_status = DataSetStatus::Failed;
                                        println!("[upload_chunks_manager]: send DataSetStatus: {:?} to [DatasetManager], Err:{}", dataset_status,e);
                                        dataset_status_sender.send((chunks_manager.dataset_id.clone(),dataset_status)).await?;
                                    }
                                }

                            }
                        },
                        None => {
                            println!("[upload_chunks_manager]: all chunk_result_sender in chunks task  are closed !!!");
                            warn!("[upload_chunks_manager]: all chunk_result_sender in chunks task  are closed !!!");
                            break;
                        }
                    }
                },
                _ = self.shutdown_rx.recv() => {
                    println!("[upload_chunks_manager]: received shutdown cmd, stop upload dataset blob !!!");
                    //Do not need to process shutdown_req err
                    //Case if shutdown_sx is dropped, also need to shutdown
                    break;
                }
            }
        }

        println!("[upload_chunks_manager]: End !!!");

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
    shutdown_cmd_suber: broadcast::Sender<()>,
}

impl Drop for DatasetChunksManager {
    fn drop(&mut self) {
        println!("[DatasetChunksManager]: dataset chunks manager droped !!!");
    }
}

impl DatasetChunksManager {
    fn new(dataset_id:String,
           dataset_version_id:String,
           dataset_meta:DatasetMeta,
           all_dataset_sema:Arc<Semaphore>,
           endpoint:String,
           file_path:PathBuf,
           shutdown_cmd_suber:broadcast::Sender<()>) -> Self {

        let one_dataset_sema = Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY+1));
        //ToDo: process upload status
        Self {
            dataset_id,
            dataset_version_id,
            all_dataset_chunk_sema: all_dataset_sema,
            one_dataset_chunk_sema: one_dataset_sema,
            upload_endpoint: endpoint,
            upload_file_path: file_path,
            upload_dataset: dataset_meta,
            shutdown_cmd_suber,
        }
    }

    async fn create_data_chunk_producer(&self,chunk_pusher:mpsc::Sender<DatasetChunk>,
                                              chunk_result_sender:mpsc::Sender<DatasetChunkResult>) {

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

            while chunk_seek_start <= dataset_compressed_size-1 {

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

                if chunk_pusher.send(dc).await.is_err() {
                    //chunk_producer will shutdown after chunk_consumer_and_task_creator shutdown!
                    error!("[chunks_producer]: chunk_getter closed by chunk_consumer_and_task_creator shutdown !!!");
                    return;
                }

                chunk_seek_start += dataset_chunk_size as u64;
                chunk_num += 1;
            }

            println!("[chunks_producer]: End !!!");

        });

    }

    async fn create_data_chunk_consumer(&mut self,mut chunks_chan_getter:mpsc::Receiver<DatasetChunk>) {
        
        let create_chunk_task_sema = self.one_dataset_chunk_sema.clone();
        let mut shutdown_cmd_rx = self.shutdown_cmd_suber.subscribe();

        tokio::spawn(async move {
            
            loop{
                tokio::select! {
                    Some(dc) = chunks_chan_getter.recv() => {

                        let try_create_chunk_task_permit = create_chunk_task_sema.acquire().await;

                        match try_create_chunk_task_permit {
                            std::result::Result::Ok(_) => {
                                println!("[chunk_consumer_and_task_creator]: create chunk task permit by one_dataset!");
                                tokio::spawn(async move {
                                    //ToDo: process upload chunk err result!
                                    Self::create_upload_chunk_task(dc).await?;
                                    Ok(())
                                });
                            },
                            std::result::Result::Err(e) => {
                                error!("[chunk_consumer_and_task_creator]: create chunk task err, permit by one_dataset err: {:?}", e);
                                break;
                            }
                        }
                       
                    },
                    _ = shutdown_cmd_rx.recv() => {
                        println!("[chunk_consumer_and_task_creator]: received shutdown cmd, stop upload dataset blob !!!");
                        //Do not need to process shutdown_req err
                        //Case if shutdown_sx is dropped, also need to shutdown chunk_consumer_and_task_creator
                        break;
                    }
                }
            }

            println!("[chunk_consumer_and_task_creator]: End !!!");

        });
    }
   
    async fn merge_data_chunks(&self) -> Result<()>{

        let digest = Digest::new("urfs".to_string(),self.upload_dataset.id.clone());

        //ToDo: digester not ztd! shold be rename to urfs or else
        let form = multipart::Form::new()
            .text("mode",DataMode::ChunkEnd.to_string())
            .text("dataset_id","xxx")
            .text("dataset_version_id","default")
            .text("digest",digest.to_string())
            .text("total_size",self.upload_dataset.compressed_size.to_string())
            .text("chunk_size",self.upload_dataset.chunk_size.to_string());

        let httpclient = get_http_client()?;

        let upload_chunk_end_url = self.upload_endpoint.clone()+"/api/v1/file/upload";

        println!("[upload_chunk_task]: upload DataChunk End merge chunks,dataset hash:{:?} ,url {:?}",
                 self.upload_dataset.id,
                 upload_chunk_end_url);

        let result = httpclient
            .put(upload_chunk_end_url)
            .multipart(form)
            .send().await;

        if let Result::Ok(resp) = result {

            let resp_txt = resp.text().await?;

            println!("[upload_chunk_end]: upload DataChunk End Ok, resp: {:?}",resp_txt);

            Ok(())

        }else if let Some(err) = result.err() {

            error!("[upload_chunk_end]: upload DataChunk End err, dataset.id:{:?} err:{}!!!",self.upload_dataset.id,err);
            anyhow::bail!("upload DataChunk End err,dataset.id:{:?} err:{}!!!",self.upload_dataset.id,err)

        }else{

            error!("[upload_chunk_end]: upload DataChunk End err, dataset.id:{:?}!!!",self.upload_dataset.id);
            anyhow::bail!("upload DataChunk End err,dataset.id:{:?}!!!",self.upload_dataset.id)

        }
    }

    pub async fn create_upload_chunk_task(data_chunk:DatasetChunk) -> Result<()> {

        let _run_permit_by_one_dataset = data_chunk.one_dataset_sema.acquire().await?;
        println!("[upload_chunk_task]: upload DataChunk permit by one_dataset!");
        let _run_permit_by_all_dataset = data_chunk.all_dataset_sema.acquire().await?;
        println!("[upload_chunk_task]: upload DataChunk permit by all_dataset!!");

        println!("[upload_chunk_task]: ready to upload DataChunk:{:?}", data_chunk.chunk_seek_start);

        let mut chunk_length = data_chunk.dataset.chunk_size as usize;
        let mut chunk_buffer = vec![0;chunk_length];
        let chunk_end = data_chunk.chunk_seek_start+data_chunk.dataset.chunk_size;
        if chunk_end > data_chunk.dataset.compressed_size {
            chunk_length = (data_chunk.dataset.compressed_size-data_chunk.chunk_seek_start) as usize;
            chunk_buffer = vec![0;chunk_length];
        }
        println!("chunk_num:{} chunk_start:{} chunk_end:{} total_size:{}",data_chunk.chunk_num,
                 data_chunk.chunk_seek_start,chunk_end,data_chunk.dataset.compressed_size);

        let digest = Digest::new("urfs".to_string(),data_chunk.dataset.id.clone());
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

        println!("[upload_chunk_task][stat_chunk_file]: check chunk file status :{:?},chunk num:{:?}!!!",chunk_file_status,data_chunk.chunk_num.to_string());

        //ToDo: retry upload add reliability?
        if chunk_file_status == UrchinFileStatus::UnKnown {
            error!("[upload_chunk_task]: upload DataChunk err, chunk num:{:?}!!!",data_chunk.chunk_num.to_string());

            data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                chunk_file_size,
                Err(anyhow!("upload ataChunk err,chunk num:{:?}!!!",data_chunk.chunk_num.to_string()))
            )).await?;

            anyhow::bail!("upload ataChunk err,chunk num:{:?}!!!",data_chunk.chunk_num.to_string())
        }else if chunk_file_status == UrchinFileStatus::Exist {
            println!("[upload_chunk_task]: upload DataChunk Exist, finish immediately！！！");

            data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                chunk_file_size,
                Ok(())
            )).await?;

            println!("[upload_chunk_task]: send upload result to chunk manager.");

             Ok(())
        }else{
            //Chunk File NotFound will upload once
            //Chunk File Partial will upload overwrite
            println!("[upload_chunk_task] chunk file NotFound or Partial, go to upload chunk num:{:?}!!!", data_chunk.chunk_num.to_string());
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

            println!("[upload_chunk_task]: upload DataChunk num:{:?} ,chunk_size:{:?} ,url {:?}",
                     data_chunk.chunk_num.to_string(),
                     data_chunk.dataset.chunk_size.to_string(),
                     upload_chunk_url);

            let result = httpclient
                .put(upload_chunk_url)
                .multipart(form)
                .send().await;

            if let Result::Ok(resp) = result {

                let resp_txt = resp.text().await?;

                println!("[upload_chunk_task]: upload DataChunk finish, resp: {:?}",resp_txt);

                data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                    chunk_file_size,
                    Ok(())
                )).await?;

                println!("[upload_chunk_task]: send upload result to chunk manager.");

                Ok(())

            }else if let Some(err) = result.err() {

                error!("[upload_chunk_task]: upload chunk num:{}, http access error:{}",data_chunk.chunk_num.to_string(),err.to_string());

                data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                    chunk_file_size,
                    Err(anyhow!("upload chunk num:{}, http network layer error:{}",data_chunk.chunk_num.to_string(),err.to_string()))
                )).await?;

                anyhow::bail!("upload DataChunk err,chunk num:{} err:{}!!!",data_chunk.chunk_num.to_string(),err.to_string())
            }else{
                //ToDo: get urchin logic error
                error!("[upload_chunk_task]: upload chunk num:{}, urchin logic layer error",data_chunk.chunk_num.to_string());
                data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                    chunk_file_size,
                    Err(anyhow!("upload chunk num:{}, urchin logic layer error",data_chunk.chunk_num.to_string()))
                )).await?;

                anyhow::bail!("upload DataChunk err,chunk num:{:?}!!!",data_chunk.chunk_num.to_string())
            }
        }
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
    all_dataset_sema: Arc<Semaphore>,
    //one dataset also contain many chunks, concurrency form one dataset also should be limited
    one_dataset_sema: Arc<Semaphore>,
}

impl DatasetChunk {
    fn new(dataset_id:String,
           dataset_version_id:String,
           ds: DatasetMeta,
           num: u64,
           start:u64,
           endpoint:String,
           file_path: PathBuf,
           sema_permit_by_all_dataset:Arc<Semaphore>,
           sema_permit_by_one_dataset:Arc<Semaphore>,
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
            all_dataset_sema: sema_permit_by_all_dataset,
            one_dataset_sema: sema_permit_by_one_dataset
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
    /// A sha256 hex string generally.
    #[serde(alias = "blob_id")]
    id: String,
    /// Size of the compressed blob file.
    compressed_size: u64,
    /// Size of the uncompressed blob file, or the cache file.
    uncompressed_size: u64,
    /// Chunk size.
    chunk_size: u64,
    /// Number of chunks in blob file.
    /// A helper to distinguish bootstrap with extended blob table or not:
    ///     Bootstrap with extended blob table always has non-zero `chunk_count`
    chunk_count: u64,
    /// Compression algorithm to process the blob.
    compressor: String,
    /// Message digest algorithm to process the blob.
    digester: String,
}

impl DatasetMeta{

    pub fn maxsize() -> Result<String> {

        let json = serde_json::to_string( &json!({
            "max_size:": (RAFS_MAX_CHUNK_SIZE >> 20) * (RAFS_MAX_CHUNKS_PER_BLOB >> 20) as u64,
        }))?;

        Ok(json)
    }
}