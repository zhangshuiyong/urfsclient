// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate anyhow;
//can log to file with using info!(),error!(),debug!(),trace!(),warn!()
#[macro_use]
extern crate log;

use tauri_plugin_log::LogTarget;
use log::LevelFilter;
use fern::colors::ColoredLevelConfig;
use tokio::sync::{mpsc,oneshot};

mod inspect;
mod upload_backend;
mod upload_backend_type;

use crate::upload_backend::DatasetManager;
use crate::upload_backend_type::UiError;
use crate::upload_backend_type::UiResponse;

#[tauri::command]
async fn start_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,UiError> {
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    cmd_sender.send(("start_upload".to_string(),req,sx)).await?;
    
    let resp = rx.await?;

    let resp_json = serde_json::to_string(&resp)?;

    Ok(resp_json)
}

#[tauri::command]
async fn stop_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,UiError> {
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    cmd_sender.send(("stop_upload".to_string(),req,sx)).await?;
    
    let resp = rx.await?;

    let resp_json = serde_json::to_string(&resp)?;

    Ok(resp_json)
}


#[tauri::command]
async fn terminate_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,String> {
    Ok("terminate upload Ok!".to_string())
}

#[tauri::command]
async fn get_history(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req:String) -> Result<String,UiError> {
    warn!("get dataset_status history: {:?}", req);
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    cmd_sender.send(("get_history".to_string(),"req".to_string(),sx)).await?;

    let resp = rx.await?;

    let resp_json = serde_json::to_string(&resp)?;

    Ok(resp_json)
}

#[tokio::main]
async fn main() {
    let (ex_cmd_sender,ex_cmd_collector) = mpsc::channel(100);
    
    tokio::spawn(async move {
        let mut dataset_manager = DatasetManager::new(ex_cmd_collector);
        info!("[dataset_manager]: start");
        dataset_manager.run().await;
        info!("[dataset_manager]: stop");
    });

    tauri::Builder::default()
        .plugin(
            tauri_plugin_log::Builder::default()
            .targets([
                LogTarget::Stdout,
                LogTarget::LogDir,
                LogTarget::Webview,
            ])
            .level(LevelFilter::Debug)
            .with_colors(ColoredLevelConfig::default())
            .build()
        )
        .manage(ex_cmd_sender)
        .invoke_handler(tauri::generate_handler![start_upload,stop_upload,terminate_upload,
                                                 get_history])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}