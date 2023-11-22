<template>
  <a-layout>
    <a-layout-header>
      <a-row justify="space-between">
        <a-col :span="8">
          <a-row>
            <a-col :span="2" class="logo"></a-col>
            <a-col :span="14">
              <a-tabs v-model:activeKey="activeKey" @tabClick="showList()">
                <a-tab-pane key="1" tab="数据集"></a-tab-pane> </a-tabs></a-col> </a-row></a-col>
        <a-col :span="5" class="min-width">
          <a-row justify="end">
            <a-col :span="2">
              <div class="upload" @click="openTaskManager()"></div>
            </a-col>
            <a-col :span="3" class="pt-05">
              <a-avatar :size="50">
                <template #icon>
                  <UserOutlined />
                </template>
              </a-avatar>
            </a-col>
            <a-col :span="2">
              <a-dropdown>
                <template #overlay>
                  <a-menu @click="handleMenuClick">
                    <a-menu-item key="1"> 创建数据集 </a-menu-item>
                    <a-menu-item key="2" disabled> 系统设置 </a-menu-item>
                    <a-menu-item key="3"> 退出 </a-menu-item>
                  </a-menu>
                </template>
                <div class="arrowDown"></div>
              </a-dropdown>
            </a-col>
          </a-row>
        </a-col>
      </a-row>
    </a-layout-header>
    <a-layout class="main_layout">
      <a-layout class="main_content_layout">
        <a-layout-content class="main_content">
          <slot />
        </a-layout-content>
      </a-layout>
    </a-layout>
    <operateDialg v-if="open1" @closeDatasetDialg="closeDatasetDialg" title="创建数据集" />
    <settingDialg v-if="open2" @closeSettingDialg="closeSettingDialg" />
    <taskManagerDialg v-if="open3" @closeTaskManagerDialg="closeTaskManagerDialg" :allTaskData="allTaskData" />
  </a-layout>
</template>
<script lang="ts" setup>
import { ref, reactive } from "vue";
const activeKey = ref("1");
import { UserOutlined } from "@ant-design/icons-vue";
import type { MenuProps } from "ant-design-vue";
import { useRouter } from "vue-router";
import { message } from "ant-design-vue";
import operateDialg from "../components/OperateDatasetDialg.vue";
import settingDialg from "../components/SystemSettingDialg.vue";
import taskManagerDialg from "../components/TaskManagerDialg.vue";
import { useStore } from "vuex";
import { invoke } from "@tauri-apps/api/tauri";
import config from "../util/config"
import { info } from "tauri-plugin-log-api";
import { formatSize } from "../util/index"
import moment from "moment";
import { http } from "@tauri-apps/api";
interface dataType {
  id: string,
  state: string | Number | any,
  size: string | Number,
  path: string,
  createDate: string,
  version: string

}
interface taskType {
  id: string,
  state: string | Number | any,
  name: string | null,
  size: string | Number,
  path: string,
  createDate: string,
  version: string,
  versionName: string
}
const data: dataType[] = reactive([]);
const allTaskData: taskType[] = reactive([]);
const router = useRouter();
const store = useStore();
const open1 = ref<boolean>(false);
const open2 = ref<boolean>(false);
const open3 = ref<boolean>(false);
const handleButtonClick = (e: Event) => {
  console.log("click left button", e);
};
const handleMenuClick: MenuProps["onClick"] = (e) => {
  if (e.key == 3) {
    router.push({
      path: "/",
    });
    message.success("退出成功");
  } else if (e.key == 1) {
    open1.value = true;
  } else {
    open2.value = true;
  }
};
const closeDatasetDialg = (val: boolean) => {
  open1.value = val;
};
const closeSettingDialg = (val: boolean) => {
  open2.value = val;
};
const closeTaskManagerDialg = (val: boolean) => {
  open3.value = val;
};
const openTaskManager = () => {
  get_history()

};
const showList = () => {
  store.commit("changeDataPage", "list");
};
async function get_history() {
  try {
    info("[ui] click get_history btn");
    let res: any = await invoke("get_history", { req: JSON.stringify({ req: "{}" }) });
    let Data = JSON.parse(res)
   if(Data.status_code==0&&Data["payload_json"]){
    Data = JSON.parse(Data["payload_json"])
    if (Data.length !== 0) {
      data.length = 0
      Data.forEach((item: { dataset_id: string, local_dataset_path: string, create_timestamp: string, dataset_status: any, dataset_version_id: string, local_dataset_size: string }) => {
        if (typeof item.dataset_status === "string") {
          data.push({ id: item.dataset_id, state: item.dataset_status, size: formatSize(item.local_dataset_size.toString()), path: item.local_dataset_path, createDate: moment(parseInt(item.create_timestamp) * 1000).format('YYYY-MM-DD-HH:mm:ss'), version: item.dataset_version_id })
        } else {
          data.push({ id: item.dataset_id, state: parseInt(item.dataset_status.Uploading), size: formatSize(item.local_dataset_size.toString()), path: item.local_dataset_path, createDate: moment(parseInt(item.create_timestamp) * 1000).format('YYYY-MM-DD-HH:mm:ss'), version: item.dataset_version_id })
        }
      })
      getTaskList(data)
    } else {
      allTaskData.length = 0
      open3.value = true;
    }
   }else{
     allTaskData.length = 0
      open3.value = true;
   }
    
  } catch (err: any) {
    message.error("获取文件上传历史错误：", err);
    console.log(err, "err")
  }
}
const getTaskList = async (data: any) => {
  if (!data) {
    data = []
  }
 await Promise.all(
  data.map(async (item: {
    id: string,
    name: string,
    size: string | Number,
    path: string,
    createDate: string,
    version: string,
    state: string | Number | any,
    versionName: string
  }) => {
    const datasetId = item.id
    const dataseVersion = item.version
    try {
      const res: any = await http.fetch(config.baseURL + '/api/v1/dataset/' + datasetId, {
        method: 'GET',
        timeout: config.timeout
      })
      if (res && res.data && res.data["status_msg"] && res.data["status_msg"] == "succeed" && res.data.dataset.name !== "") {
        item.name = res.data.dataset.name


      } else { item.name = "已删除" }
      if (dataseVersion == "") { item.versionName = "已删除" }
      else {
        const res2: any = await http.fetch(config.baseURL + '/api/v1/dataset/' + datasetId + "/version/" + dataseVersion, {
          method: 'GET',
          timeout: config.timeout
        })

        if (res2.status === 422) {
          item.versionName = "已删除"
        } else { item.versionName = item.version }

      }

    } catch (err: any) {
      message.error("err", err);
    }
  })
 )
  allTaskData.length = 0
  data.forEach((item: any) => {
    allTaskData.push(item)
  })
  open3.value = true
};
</script>
<style scoped>
.ant-layout-header {
  height: 70px;
  overflow: hidden;
  padding-left: 10px;
}

.main_layout {
  width: 100%;
  /* height: 100vh; */
  background-color: #f0f0f0;
}

.logo {
  background: url("../static/logo.svg");
  background-size: 100% 100%;
  background-repeat: no-repeat;
  padding: 5px;
  height: 70px;
  min-width: 120px;
}

:deep(.ant-tabs-nav) {
  height: 70px;
}

:deep(.ant-tabs-tab-btn) {
  font-size: 20px;
  font-weight: 800;
}

:deep(.ant-tabs-ink-bar) {
  display: none;
}

.arrowDown {
  background: url("../static/arrowDown.svg");
  background-size: 100% 100%;
  background-repeat: no-repeat;
  min-width: 20px;
  min-height: 20px;
  margin-top: 30px;
}

.pt-05 {
  margin-top: 5px;
}

:deep(.ant-avatar) {
  font-size: 40px !important;
}

:deep(.ant-badge-count:hover) {
  cursor: pointer;
}

.upload {
  background: url("../static/upload.svg");
  background-repeat: no-repeat;
  background-repeat: no-repeat;
  background-size: 100%;
  width: 30px;
  height: 30px;
  margin-top: 25px;
}

.upload:hover {
  cursor: pointer;
}

.min-width {
  min-width: 350px;
}
</style>
