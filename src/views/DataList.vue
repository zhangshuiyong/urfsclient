<template>
  <a-layout>
    <a-layout-header :style="headerStyle">
      <div>
        <a-row justify="space-between">
          <a-col :span="20">
            <a-row justify="center">
              <a-col :span="8" :push="4"><a-input v-model:value="value" placeholder="请输入搜索内容" disabled /></a-col>
            </a-row>
          </a-col>
          <a-col :span="4" style="text-align: right">
            <AppstoreOutlined :style="{ fontSize: '30px', color: '#000' }" @click="changeForm('card')"
              v-if="type === 'table'" />
            <OrderedListOutlined :style="{ fontSize: '30px', color: '#000' }" @click="changeForm('table')"
              v-if="type === 'card'" />
          </a-col>
        </a-row>
      </div>
    </a-layout-header>
    <a-layout>
      <a-layout-sider :style="siderStyle" :width="'400px'">
        <a-row justify="center">
          <a-col :span="24">
            <div class="buttonGroup">
              <a-radio-group v-model:value="datasetType" button-style="solid" @change="handleChange()">
                <a-radio-button value="persional">个人数据集</a-radio-button>
                <a-radio-button value="community" disabled>公共数据集</a-radio-button>
              </a-radio-group>
            </div>
          </a-col>
        </a-row>
        <div>
          <a-tag color="pink" class="tagClass">中文分词</a-tag>
          <a-tag color="red" class="tagClass">图像分类</a-tag>
          <a-tag color="orange" class="tagClass">数据增强</a-tag>
          <a-tag color="grey" class="tagClass">文本分类</a-tag>
          <a-tag color="cyan" class="tagClass">目标检测</a-tag>
          <a-tag color="blue" class="tagClass">目标跟踪</a-tag>
          <a-tag color="yellow" class="tagClass">语义分割</a-tag>
          <a-tag color="purple" class="tagClass">音频分类</a-tag>
          <a-tag color="green" class="tagClass">其他</a-tag>
        </div>
      </a-layout-sider>
      <a-layout-content :style="contentStyle">
        <DatasetCard v-if="type == 'card'" />
        <DatasetTable v-if="type == 'table'" />
      </a-layout-content>
    </a-layout>
  </a-layout>
</template>
<script lang="ts" setup>
import type { CSSProperties } from "vue";
import { watch, ref } from "vue";
import { OrderedListOutlined, AppstoreOutlined } from "@ant-design/icons-vue";
import DatasetCard from "../components/DatasetCard.vue";
import DatasetTable from "../components/DatasetTable.vue";
import { useStore } from "vuex";
const store = useStore();
const value = ref<string>("");
const type = ref<string>("card");
const datasetType = ref("persional");
const changeForm = (val: string) => {
  type.value = val;
};
const handleChange = () => {
  store.commit("changeDataType", datasetType.value);
};
watch(value, () => {
  console.log(datasetType.value);
});
const headerStyle: CSSProperties = {
  textAlign: "center",
  color: "#fff",
  height: 64,
  paddingInline: 50,
  lineHeight: "64px",
  backgroundColor: "#fff",
};

const contentStyle: CSSProperties = {
  textAlign: "center",
  minHeight: 120,
  lineHeight: "120px",
  color: "#fff",
  backgroundColor: "#fff",
};

const siderStyle: CSSProperties = {
  color: "#fff",
  backgroundColor: "#fff",
  textAlign: "center",
};
</script>
<style scoped>
.mb-2 {
  margin-bottom: 40px;
}

.tagClass {
  width: 120px;
  height: 50px;
  text-align: center;
  padding-top: 15px;
  margin: 0px 20px 60px 0px;
  font-size: 14px;
  font-weight: 800;
}

.tagClass:hover {
  cursor: pointer;
}

.buttonGroup {
  margin: 20px 20px 50px 20px;
}
</style>
