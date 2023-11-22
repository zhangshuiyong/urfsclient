<template>
  <div style="background-color: #ececec; padding: 20px">
    <a-row :gutter="16">
      <a-col :span="8" v-for="(item, index) in cardList" :key="index">
        <a-card :title="item.name" :bordered="false" class="cardStyle" @click="showDetail(item.id)">
          <a-tag v-for="(Item, index) in item.tags" :key="index" :color="getLabel(Item).color" class="tagClass">{{
            getLabel(Item).content }}</a-tag>
          <p class="mt-1">{{ item.desc }}</p>
          <p>
            <!-- <span class="m-1">{{ item.updateTime }}</span
            ><span class="m-1" v-if="show">{{ item.author }}</span> -->
          </p>
        </a-card>
      </a-col>
    </a-row>
    <!-- <a-row justify="end" class="bg">
      <a-col :span="10">
        <a-pagination v-model:current="current" :total="50" show-less-items @change="handleTableChange" /></a-col>
    </a-row> -->
  </div>
</template>
<script lang="ts" setup>
import { reactive, ref, watch, onMounted } from "vue";
import { useStore } from "vuex";
import {getLabel} from "../util/index"
import { message } from "ant-design-vue";
import config from "../util/config"
import { http } from "@tauri-apps/api";
const store = useStore();
const show = ref<boolean>(false);
onMounted(() => {
  if (store.state.dataType === "persional") {
    show.value = false;
    getList()
  } else {
    show.value = true;
    getList()
  }
});
watch(
  () => store.state.dataType,
  (val) => {
    if (val === "persional") {
      show.value = false;
    } else {
      show.value = true;
    }
  },
);
watch(
  () => store.state.dataSetNumber,
  (val) => {
    getList()
  },
);
interface cardType {
  name: string;
  desc: string;
  tags: [];
  id: string;
  replica: Number;

}
const cardList: cardType[] = reactive([]);
const current = ref(1);
const handleTableChange = (e: any) => {
  console.log(e);
};
const showDetail = (id: String) => {
  store.commit("changeDataPage", "detail");
  store.commit("changeDatasetId", id);
};
async function getList() {
  try {
    const res: any = await http.fetch(config.baseURL + '/api/v1/datasets', {
      method: 'GET',
      timeout: config.timeout,
    })
    if (res.data["status_msg"] === "succeed") {
      if (!res.data.datasets) {
        res.data.datasets = []
      }
      cardList.length = 0
      res.data.datasets.forEach((item: { id: string; name: string; desc: string, replica: Number, tags: [] }) => {
        cardList.push({ id: item.id, name: item.name, replica: item.replica, desc: item.desc, tags: item.tags })
      })
    } else {
      message.warning("获取数据集列表失败")
    }
  } catch (err: any) {
    message.error("err", err);
  }
}
</script>
<style scoped>
.cardStyle {
  margin-bottom: 25px;
}

.ant-card {
  height: 220px;
}

.bg {
  background-color: #fff;
  padding: 10px 0 10px 0;
}

:deep(.ant-card-head-title) {
  color: #646cff;
}

:deep(.ant-card-head-title:hover) {
  cursor: pointer;
}

.m-1 {
  margin: 20px;
}

.mt-1 {
  margin-top: 10px;
}

:deep(.ant-pagination) {
  text-align: right;
}

.tagClass {
  margin-bottom: 10px;
}
</style>
