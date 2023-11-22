<template>
  <div>
    <a-table :columns="type ? columns1 : columns2" :data-source="data" :pagination="false">
      <template #bodyCell="{ column, record }">
        <template v-if="column.key === 'name'">
          <a @click="showDetail(record.id)">
            {{ record.name }}
          </a>
        </template>
        <template v-else-if="column.key === 'tags'">
          <span>
            <a-tag v-for="tag in record.tags" :key="tag" :color="getLabel(tag).color">
              {{ getLabel(tag).content }}
            </a-tag>
          </span>
        </template>
      </template>
    </a-table>
    <!-- <a-row justify="end" class="bg">
      <a-col :span="10">
        <a-pagination
          v-model:current="current"
          :total="50"
          show-less-items
          @change="handleTableChange"
      /></a-col>
    </a-row> -->
  </div>
</template>
<script lang="ts" setup>
import { reactive, ref, watch, onMounted } from "vue";
import { useStore } from "vuex";
import { useRouter } from "vue-router";
import config from "../util/config"
import { http } from "@tauri-apps/api";
import {getLabel} from "../util/index"
import { message } from "ant-design-vue";
const store = useStore();
const type = ref(false);
onMounted(() => {
  if (store.state.dataType === "persional") {
    type.value = false;
    getList()
  } else {
    type.value = true;
    getList()
  }
});
watch(
  () => store.state.dataType,
  (val) => {
    if (val === "persional") {
      type.value = false;
    } else {
      type.value = true;
    }
  }
);
watch(
  () => store.state.dataSetNumber,
  (val) => {
    getList()
  },
);
const columns1 = [
  {
    title: "名称",
    dataIndex: "name",
    key: "name",
    align:"center"
  },
  {
    title: "Tags",
    key: "tags",
    dataIndex: "tags",
    align:"center"
  },
  {
    title: "描述",
    dataIndex: "desc",
    key: "desc",
    align:"center"
  },
  {
    title: "更新时间",
    dataIndex: "updateTime",
    key: "updateTime",
    align:"center"
  },
  {
    title: "作者",
    dataIndex: "author",
    key: "author",
    align:"center"
  },
];
const columns2 = [
  {
    title: "名称",
    dataIndex: "name",
    key: "name",
    align:"center"
  },
  {
    title: "标签",
    key: "tags",
    dataIndex: "tags",
    align:"center"
  },
  {
    title: "描述",
    dataIndex: "desc",
    key: "desc",
    align:"center"
  },
  // {
  //   title: "更新时间",
  //   dataIndex: "updateTime",
  //   key: "updateTime",
  // },
];
interface dataType {
  name: string;
  desc: string;
  tags: label[];
  id: string;
  replica: Number;

}
interface label {
  color: string;
  content: string;
}
const data: dataType[] = reactive([]);
const showDetail = (id: String) => {
  store.commit("changeDataPage", "detail");
  store.commit("changeDatasetId", id);
};

const current = ref(1);
const handleTableChange = (e: any) => {
  console.log(e);
};
async function getList() {
  try {
    const res:any = await http.fetch(config.baseURL + '/api/v1/datasets', {
      method: 'GET',
      timeout: config.timeout
    })
    if (res.data.status_msg === "succeed") {
      if(!res.data.datasets){
        res.data.datasets=[]
      }
      data.length = 0
      res.data.datasets.forEach((item: { id: string; name: string;desc:string,replica:Number,tags:[] }) => {
        data.push({ id: item.id, name: item.name, replica: item.replica, desc: item.desc, tags: item.tags })
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
.bg {
  background-color: #fff;
  padding: 10px 0 10px 0;
}

:deep(.ant-pagination) {
  text-align: right;
}
</style>
