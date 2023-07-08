<script setup lang="ts">
import { ref } from "vue";
import { invoke } from "@tauri-apps/api/tauri";
import { info } from "tauri-plugin-log-api";
import { message } from 'ant-design-vue';

const greetMsg = ref("");
const name = ref("");

async function star_upload() {
    try{
        await invoke("start_upload", { req :JSON.stringify({dataset_id: 'xx',server_endpoint: 'xxx' })})
        message.success('正在上传');
    }catch(err: any){
        message.error('上传错误：',err);
    }
}

async function stop_upload() {
    try{
        await invoke("stop_upload", { req :JSON.stringify({dataset_id: 'xx' })})
        message.success("暂停上传成功");
    }catch(err: any){
        message.error("暂停上传错误：", err);
    }
}

async function terminate_upload() {
    try{
        await invoke("terminate_upload", { req :JSON.stringify({dataset_id: 'xx' })})
        message.success("终止上传成功");
    }catch(err: any){
        message.error("终止上传错误：", err);
    }
}

async function get_history() {
    try{
        info("[ui] click get_history btn")
        await invoke("get_history", {req: JSON.stringify({ name:name.value })})
        message.success("获取文件上传历史成功");
    }catch(err: any){
        message.error("终止上传错误：", err);
    }
}

</script>

<template>
  <div class="card">
    <button type="button" @click="star_upload()">上传文件</button>
  </div>

  <p>{{ greetMsg }}</p>
</template>
