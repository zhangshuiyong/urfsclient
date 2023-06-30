// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

mod inspect;
mod upload;

use std::path::Path;

use tokio::{self, runtime::Runtime};
use tokio::sync::mpsc;

use crate::upload::{DatasetManager,DataSetStatus,DatasetUploader, DatasetMeta};

use anyhow::{ensure, Result, Context};
use std::fs::{self, metadata, DirEntry, File, OpenOptions};
use std::sync::Arc;
use nydus_api::ConfigV2;

#[tauri::command]
async fn upload(dataset_status_sender_car: tauri::State<'_,DatasetManagerCar>,name: String) -> Result<String,String> {
    
    let dataset_status_sender = dataset_status_sender_car.inner().dataset_status_sender.clone();

    if dataset_status_sender.send(("x".to_string(),DataSetStatus::Init)).await.is_err() {
        error!("[upload dataset_status_sender]: dataset_status chan closed!!!");
        return Err(format!("upload dataset_status_sender Err, {}", name));
    }

    let dataset_image_path = Path::new("/Users/terrill/Documents/urchin/zhangshuiyong/urfs/tests/cifar-10-image");

    let result = ensure_directory(dataset_image_path.clone());

    if result.is_err() {
        return Err(format!("ensure_directory Err, {}", name));
    }

    let dataset_meta_path = dataset_image_path.join("meta");
    let dataset_blob_path = dataset_image_path.join("blob");

    println!("dataset_image_dir:{:?},dataset_meta_path: {:?}",dataset_image_path,dataset_meta_path);

    let result = inspect_blob_info(dataset_meta_path.as_path());
    if result.is_err() {
        return Err(format!("inspect_blob_info Err, {}", name));
    }

    let blobs_info_json = result.unwrap();

    let result  = serde_json::from_str(blobs_info_json.as_str());
    if result.is_err() {
        return Err(format!("serde_json blobs_info_json Err, {}", name));
    }

    let dataset_metas: Vec<DatasetMeta> = result.unwrap();
    let dataset_meta = &dataset_metas[0];
    let upload_dataset_meta = dataset_meta.clone();

    let upload_server_endpoint = "http://192.168.23.209:65004".to_string();

    let mut uploader = DatasetUploader::new();

    let result = uploader.upload(dataset_meta_path, upload_dataset_meta, dataset_blob_path, upload_server_endpoint).await;
    if result.is_err() {
        println!("[main]: uploader upload Err, {:?}", result);
        return Err(format!("uploader Err, {}", name));
    }

    match name.as_str() {
        "" => Err("Name cannot be empty".to_string()),
        _ => Ok(format!("Hello, {}! You've been greeted from Rust!", name))
    }
}

#[tauri::command]
async fn get_history(dataset_status_sender_car: tauri::State<'_,DatasetManagerCar>,dataset_id:String) -> Result<String,String> {

    let dataset_manager = dataset_status_sender_car.inner().dataset_manager_lock.read().await;

    let history = dataset_manager.get_history();

    println!("get dataset_status history: {:?}", history);

    Ok("Ok!".to_string())
}

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

use tokio::sync::RwLock;
#[derive(Clone)]
struct DatasetManagerCar {
    dataset_manager_lock: Arc<RwLock<DatasetManager>>,
    dataset_status_sender: mpsc::Sender<(String,DataSetStatus)>
}

fn main() {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let dataset_manager_lock = Arc::new(RwLock::new(DatasetManager::new()));
    let dataset_manager_wlock = dataset_manager_lock.clone();
    let (dataset_status_sender,mut dataset_status_collector) = mpsc::channel(100);
    
   let dmc = DatasetManagerCar {
        dataset_manager_lock: dataset_manager_lock,
        dataset_status_sender: dataset_status_sender.clone()
    };
    
    tokio::spawn(async move {
        let mut upload_manager = dataset_manager_wlock.write().await;
        
        while let Some(dataset_status) = dataset_status_collector.recv().await {
            println!("[upload_manager]: received dataset_status: {:?}", dataset_status);

            upload_manager.add_dataset_status("x".to_string(), dataset_status.1);
        }
    });

    tauri::Builder::default()
        .manage(dmc)
        .invoke_handler(tauri::generate_handler![upload])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");

}