<template>
  <div class="uploadWrapper">
    <el-upload class="upload-demo" drag :on-change="showText" action="#" :limit="1" :auto-upload="false">
      <el-icon class="el-icon--upload"><upload-filled /></el-icon>
      <div class="el-upload__text">请将文件拖拽到此处</div>
      <template #tip>
        <div class="el-upload__tip"></div>
      </template>
    </el-upload>
    <el-row class="row-bg" justify="center">   
      <el-col :span="4"><el-button type="primary" @click="upload()">点击上传</el-button></el-col>
    </el-row>
  </div>
</template>
<script lang="ts" setup>
window.addEventListener("drop", (e) => e.preventDefault(), false);
window.addEventListener("dragover", (e) => e.preventDefault(), false);
import { ref } from "vue";
import { invoke } from "@tauri-apps/api/tauri";
import { ElMessage } from 'element-plus'
let name = ref('');
const showText=(file: { name: string; })=>{
  name.value=file.name
}
async function upload() {
  if(name.value==''){
    ElMessage({
    message: '文件不能为空',
    type: 'warning',
  })
  }else{
    ElMessage({
    message: '开始上传',
    type: 'success',
  })

 try{
  await invoke("upload", { name:name.value })
  ElMessage({
    message: '文件上传成功',
    type: 'success'}
  )
 }catch(err: any){
  ElMessage({
    message: err,
    type: 'error',
  })
 }
  
}}
</script>
<style scoped>
.uploadWrapper {
  width: 60%;
  margin: 0 auto;
  margin-top: 70px;
}
.fileName{font-size: 14px;}

</style>
