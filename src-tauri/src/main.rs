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
use crate::upload_backend_type::UiResponse;

use tokio::sync::{mpsc,oneshot};

#[tauri::command]
async fn start_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,String> {
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    let cmd_commit_result = cmd_sender.send(("start_upload".to_string(),req,sx)).await;
    
    if cmd_commit_result.is_err() {
        println!("[startuploadcmd] cmd_sender  Err: {:?}", cmd_commit_result);
        return Err(format!("[startuploadcmd] cmd_sender Err, {:?}", cmd_commit_result));
    }

    match rx.await {
        Ok(resp) => {
            println!("[startuploadcmd] resp: {:?}", resp);
        },
        Err(e) => {
            println!("[startuploadcmd] cmd channel Err: {:?}", e);
            return Err(format!("start upload cmd channel Err, {}", e));
        }
    }

    // if dataset_status_sender.send(("x".to_string(),DataSetStatus::Init)).await.is_err() {
    //     error!("[upload dataset_status_sender]: dataset_status chan closed!!!");
    //     return Err(format!("upload dataset_status_sender Err, {}", name));
    // }

    Ok(format!("Hello! You've been greeted from Rust!"))
}

#[tauri::command]
async fn stop_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,String> {
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    let cmd_commit_result = cmd_sender.send(("stop_upload".to_string(),req,sx)).await;
    
    if cmd_commit_result.is_err() {
        println!("[stopuploadcmd] cmd_sender  Err: {:?}", cmd_commit_result);
        return Err(format!("[stopuploadcmd] cmd_sender Err, {:?}", cmd_commit_result));
    }

    match rx.await {
        Ok(resp) => {
            println!("[stopuploadcmd] resp: {:?}", resp);

           let json_result = serde_json::to_string(&resp);
           
           match json_result {
                Ok(resp_json) => {
                    return Ok(resp_json);
                },
                Err(e) => {
                    error!("[stopuploadcmd] serialize resp json err: {}", e);

                    return Err(format!("[stopuploadcmd] serialize resp json err: {}", e));
                }
           }
        },
        Err(e) => {
            println!("[stopuploadcmd] cmd channel Err: {:?}", e);
            return Err(format!("[stopuploadcmd] upload cmd channel Err, {}", e));
        }
    }
}


#[tauri::command]
async fn terminate_upload(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req: String) -> Result<String,String> {
    Ok("terminate upload Ok!".to_string())
}

#[tauri::command]
async fn get_history(dataset_cmd_sender: tauri::State<'_,mpsc::Sender<(String,String,oneshot::Sender<UiResponse>)>>,req:String) -> Result<String,String> {
    println!("get dataset_status history: {:?}", req);
    
    let cmd_sender = dataset_cmd_sender.inner().clone();

    let (sx,rx) = oneshot::channel();

    let cmd_result = cmd_sender.send(("get_history".to_string(),"req".to_string(),sx)).await;

    if cmd_result.is_err() {
        //ToDo: process Error log
        println!("[ex_cmd_sender] get_history Err: {:?}", cmd_result);
        return Err(format!("[ex_cmd_sender] get_history Err, {:?}", cmd_result));
    }

    match rx.await {
        Ok(resp) => {
            println!("[UiCmd] get history resp: {:?}", resp);

           let json_result = serde_json::to_string(&resp);
           
           match json_result {
                Ok(resp_json) => {
                    return Ok(resp_json);
                },
                Err(e) => {

                    //ToDo: process Error log
                    println!("[UiCmd] serialize resp json err: {}", e);

                    return Err(format!("[UiCmd] serialize resp json err: {}", e));
                }
           }
        },
        Err(e) => {
            //ToDo: process Error log
            println!("[UiCmd] get history resp channel Err,{}", e);

            return Err(format!("[UiCmd] get history resp channel Err, {}", e));
        }
    }
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
        .invoke_handler(tauri::generate_handler![start_upload,stop_upload,terminate_upload,
                                                 get_history])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}