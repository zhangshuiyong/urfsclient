// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

//=========== dataset_image_cmd.rs ===========
// #![deny(warnings)]
#[macro_use(crate_authors)]
extern crate clap;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;

#[macro_use]
mod trace;
mod builder;
mod core;
mod inspect;
mod merge;
mod stat;
mod unpack;
mod validator;
//=========== dataset_image_cmd.rs ===========

mod dataset_image_cmd;
mod dataset_backend;
mod dataset_backend_type;

use tauri_plugin_log::LogTarget;
use log::LevelFilter;
use fern::colors::ColoredLevelConfig;
use tokio::sync::{mpsc,oneshot};

use crate::dataset_backend::DatasetManager;
use crate::dataset_backend_type::UiError;
use crate::dataset_backend_type::UiResponse;

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
async fn terminate_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,UiError> {
    info!("terminate_upload begin: {:?}", req);
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    cmd_sender.send(("terminate_upload".to_string(),req,sx)).await?;

    let resp = rx.await?;

    let resp_json = serde_json::to_string(&resp)?;

    info!("terminate_upload end: {:?}", resp_json);
    Ok(resp_json)
}

#[tauri::command]
async fn get_history(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req:String) -> Result<String,UiError> {
    warn!("get dataset_status history: {:?}", req);
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    cmd_sender.send(("get_history".to_string(),req,sx)).await?;

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

