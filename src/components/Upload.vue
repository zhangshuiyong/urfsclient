<script setup lang="ts">
import { h,ref,reactive } from "vue";
import { invoke } from "@tauri-apps/api/tauri";
import { open } from '@tauri-apps/api/dialog';
import { info,error } from "tauri-plugin-log-api";
import { message } from 'ant-design-vue';
import { appCacheDir } from '@tauri-apps/api/path';
import { FileOutlined,FolderOutlined } from '@ant-design/icons-vue';

const uploadItemList:any = reactive([]);

async function select_upload_file() {

    const selected_file = await open({
        multiple: false,
    });

    if(typeof selected_file === 'string'){
        info("[ui] select upload file :"+selected_file);
        uploadItemList.push({name:selected_file, isDir:false});
    }
}

async function select_upload_fold() {

    const selected_folder = await open({
        multiple: false,
        directory: true,
    });

    if(typeof selected_folder === 'string'){
        info("[ui] select upload folder :"+selected_folder);
        uploadItemList.push({name:selected_folder,isDir:true});
    }
}

async function star_upload() {

    const appCacheDirPath = await appCacheDir();

    info("[ui] star_upload appCacheDirPath:"+appCacheDirPath);

    try{
        await invoke("start_upload", { req :JSON.stringify({
            dataset_id: 'xxx',
            dataset_version_id: 'default',
            dataset_cache_dir: appCacheDirPath,
            dataset_image_dir: '/Users/terrill/Documents/urchin/zhangshuiyong/urfs/tests/cifar-10-image',
            server_endpoint: 'http://0.0.0.0:65004'
        })})

        message.success('正在上传');
    }catch(err: any){
        message.error('上传出错：',err);
        error(`上传出错: ${err}`);
    }
}

async function stop_upload() {
    try{
        await invoke("stop_upload", { req :JSON.stringify({
            dataset_id: 'xxx',
            dataset_version_id: 'default',
        })})
        message.success("暂停上传成功");
    }catch(err: any){
        message.error("暂停上传出错：", err);
        error(`暂停上传出错: ${err}`);
    }
}

async function terminate_upload() {
    try{
        await invoke("terminate_upload", { req :JSON.stringify({
            dataset_id: 'xxx',
            dataset_version_id: 'default',
          })})
        message.success("终止上传成功");
    }catch(err: any){
        message.error("终止上传出错：", err);
        error(`终止上传出错: ${err}`);
    }
}

async function get_history() {
    try{
        info("[ui] click get_history btn")
        await invoke("get_history", {req: JSON.stringify({ req: "{}" })})
        message.success("获取文件上传历史成功");
    }catch(err: any){
        message.error("终止上传错误：", err);
    }
}

</script>

<template>
  <div class="card">
    <button type="button" @click="select_upload_file()">上传文件</button>
    <button type="button" @click="select_upload_fold()">上传文件夹</button>
    <button type="button" @click="get_history()">历史任务</button>
  </div>

  <a-list
    class="demo-upload-list"
    item-layout="horizontal"
    :data-source="uploadItemList"
  >
    
    <template #renderItem="{ item }">
      <a-list-item>
        <template #actions>
          <a key="star_upload" @click="star_upload()">开始上传</a>
          <a key="stop_upload" @click="stop_upload()">暂停上传</a>
          <a key="terminate_upload" @click="terminate_upload()">终止上传</a>
        </template>
       <div>
          <a-list-item-meta description="">
            <template #title>
              <span>{{ item.name }}</span>
            </template>
            <template #avatar>
                <a-avatar size="small" v-if="item.isDir===false">
                    <template #icon><FileOutlined /></template>
                </a-avatar>
                <a-avatar size="small" v-if="item.isDir===true">
                    <template #icon><FolderOutlined /></template>
                </a-avatar>
            </template>
          </a-list-item-meta>
        </div>
      </a-list-item>
    </template>
  </a-list>
</template>
