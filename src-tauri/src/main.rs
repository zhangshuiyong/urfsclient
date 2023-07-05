// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

mod inspect;
mod upload_backend;
mod upload_backend_type;

use crate::upload_backend::DatasetManager;
use crate::upload_backend_type::UploadDatasetRequest;
use tokio::sync::{mpsc,oneshot};

#[tauri::command]
async fn upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<String>)>>,req: String) -> Result<String,String> {
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    let req_type =  serde_json::from_str::<UploadDatasetRequest>(&req);
    
    if req_type.is_err() {
        println!("[uploadcmd] request type Err: {:?}", req_type);
        return Err(format!("[uploadcmd] request type Err, {:?}", req_type));
    }

    println!("[uploadcmd] req: {:?}", req_type);

    let cmd_result = cmd_sender.send(("upload".to_string(),req,sx)).await;
    
    if cmd_result.is_err() {
        println!("[uploadcmd] cmd_sender  Err: {:?}", cmd_result);
        return Err(format!("[uploadcmd] cmd_sender Err, {:?}", cmd_result));
    }

    match rx.await {
        Ok(resp) => {
            println!("[uploadcmd] resp: {:?}", resp);
        },
        Err(e) => {
            println!("[uploadcmd] cmd channel Err: {:?}", e);
            return Err(format!("upload cmd channel Err, {}", e));
        }
    }

    // if dataset_status_sender.send(("x".to_string(),DataSetStatus::Init)).await.is_err() {
    //     error!("[upload dataset_status_sender]: dataset_status chan closed!!!");
    //     return Err(format!("upload dataset_status_sender Err, {}", name));
    // }

    Ok(format!("Hello! You've been greeted from Rust!"))
}

#[tauri::command]
async fn get_history(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<String>)>>,name:String) -> Result<String,String> {
    println!("get dataset_status history: {:?}", name);
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    let cmd_result = cmd_sender.send(("get_history".to_string(),"req".to_string(),sx)).await;

    if cmd_result.is_err() {
        println!("[ex_cmd_sender] get_history Err: {:?}", cmd_result);
        return Err(format!("[ex_cmd_sender] get_history Err, {:?}", cmd_result));
    }

    match rx.await {
        Ok(resp) => {
            println!("get history: {:?}", resp);
        },
        Err(e) => {
            println!("get history Err: {}", e);
            return Err(format!("get history Err, {}", e));
        }
    }
    
    Ok("get history Ok!".to_string())

}

#[tokio::main]
async fn main() {
    let (ex_cmd_sender,ex_cmd_collector) = mpsc::channel(100);
    
    tokio::spawn(async move {
        let mut dataset_manager = DatasetManager::new(ex_cmd_collector);
        println!("[dataset_manager]: start");
        dataset_manager.run().await;
        println!("[dataset_manager]: stop");
    });

    tauri::Builder::default()
        .manage(ex_cmd_sender)
        .invoke_handler(tauri::generate_handler![upload,get_history])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}