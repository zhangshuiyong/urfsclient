<script setup lang="ts">
import { ref, reactive, onMounted } from "vue";
import { invoke } from "@tauri-apps/api/tauri";
import { open } from "@tauri-apps/api/dialog";
import { error } from "tauri-plugin-log-api";
import { message } from "ant-design-vue";
import { FileOutlined, FolderOutlined } from "@ant-design/icons-vue";
import { useStore } from "vuex";
import config from "../util/config"
import { defineEmits } from 'vue'
const emit = defineEmits(['close'])
const store = useStore();
const uploadItemList: any = reactive([]);
const allow = ref<boolean>(true);
const version = ref<string>("");
onMounted(() => {
  version.value = store.state.dataSetVersion
});
async function select_upload_fold() {
  const selected_folder = await open({
    multiple: false,
    directory: true,
  });
  if (typeof selected_folder === "string") {
    // info("[ui] select upload folder :" + selected_folder);
    uploadItemList[0] = { name: selected_folder, isDir: true }
  }
}
async function star_upload(source: string) {

  // info(`[ui] star_upload source path:${source}`);
  if (allow.value === true) {
    try {
      allow.value = false;
      var resp: any = await invoke("start_upload", {
        req: JSON.stringify({
          dataset_id: store.state.dataSetId,
          dataset_version_id: version.value,
          dataset_source: source,
          server_endpoint: config.baseURL
        })
      });
      let Data = JSON.parse(resp)
      if (Data.status_code == 0) {
        emit('close')
        message.success('上传请求已发送');
      }
      else {
        message.error('上传出错');
      }
      allow.value = true
      // info(`上传请求返回: ${resp}`);

    } catch (err: any) {
      message.error('上传出错：', err);
      error(`上传出错: ${err}`);
      allow.value = true
    }
  } else { message.warning("请求发送中") }
}
</script>

<template>
  <div class="card">
    <a-button type="primary" @click="select_upload_fold()">上传数据集</a-button>
  </div>

  <a-list class="demo-upload-list" item-layout="horizontal" :data-source="uploadItemList">
    <template #renderItem="{ item }">
      <a-list-item>
        <template #actions>
          <a key="star_upload" @click="star_upload(item.name)">开始上传</a>
        </template>
        <div>
          <a-list-item-meta description="">
            <template #title>
              <span>{{ item.name }}</span>
            </template>
            <template #avatar>
              <a-avatar size="small" v-if="item.isDir === false">
                <template #icon>
                  <FileOutlined />
                </template>
              </a-avatar>
              <a-avatar size="small" v-if="item.isDir === true">
                <template #icon>
                  <FolderOutlined />
                </template>
              </a-avatar>
            </template>
          </a-list-item-meta>
        </div>
      </a-list-item>
    </template>
  </a-list>
</template>
<style scoped>
.card {
  margin-top: 40px;
  text-align: center;
}

:deep(.ant-list-item-meta-content) {
  width: 200px !important;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

:deep(.ant-list-item-action) {
  margin-left: 0px !important
}
</style>
