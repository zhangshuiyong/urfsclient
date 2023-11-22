<template>
  <div>
    <a-modal v-model:open="open" :title="title" @ok="handleOk" @cancel="handleCancel" :centered="true" width="800px">
      <a-form ref="formRef" :model="formState" :rules="rules" :label-col="labelCol" :wrapper-col="wrapperCol">
        <a-form-item ref="name" label="数据集名称" name="name">
          <a-input v-model:value="formState.name" />
        </a-form-item>
        <a-form-item label="数据集描述" name="desc">
          <a-textarea v-model:value="formState.desc" />
        </a-form-item>
        <a-form-item label="标签类型" name="type">
          <a-select v-model:value="formState.tags" :showSearch=false style="width: 100%" :options="options"
            @change="handleChange()"></a-select>
        </a-form-item>
        <a-form-item label="副本数">
          <a-form-item name="copy" no-style>
            <a-input-number v-model:value="formState['replica']" :min="0" :max="10" />
          </a-form-item>
          <span class="ant-form-text">
            <a-alert message="副本数最小为0" type="warning" show-icon /></span>
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>
<script lang="ts" setup>
import { useStore } from "vuex";
import { reactive, ref, toRaw, onMounted } from "vue";
import type { UnwrapRef } from "vue";
import type { Rule } from "ant-design-vue/es/form";
import { message } from "ant-design-vue";
import { ArrowUpOutlined, ArrowDownOutlined } from "@ant-design/icons-vue";
import { http } from "@tauri-apps/api";
import config from "../util/config"
const id = ref<string>("")
const store = useStore();
onMounted(() => {
  if (props.title == "编辑数据集") {
    id.value = store.state.dataSetId
    getDetail(id.value)
  }
});
interface FormState {
  name: string;
  desc: string;
  tags: "";
  replica: number;
}
const props = defineProps({
  title: {
    default: "",
    type: String,
  },
});
const open = ref<boolean>(true);
const handleOk = (e: MouseEvent) => {
  onSubmit();
};
const handleCancel = (e: MouseEvent) => {
  reqclick();
};
const emit = defineEmits<{
  (event: "closeDatasetDialg", val: boolean): void;
}>();
//const emit = defineEmits(['chilFun']) // 自定义chilFun事件
const reqclick = () => {
  emit("closeDatasetDialg", false); //:传个父组件的数据
};
const formRef = ref();
const labelCol = { span: 5 };
const wrapperCol = { span: 10 };
const formState: UnwrapRef<FormState> = reactive({
  name: "",
  desc: "",
  tags: "",
  replica: 0,
});
const rules: Record<string, Rule[]> = {
  name: [
    {
      required: true,
      message: "请输入数据集名称",
      trigger: "change",
    },
  ],
};

async function createDataset() {
  const body = http.Body.form({
    name: formState.name,
    desc: formState.desc,
    replica: formState.replica.toString(),
    tags: formState.tags
  });
  try {
    const res:any = await http.fetch(config.baseURL + '/api/v1/dataset', {
      method: 'POST',
      body: body,
      timeout: config.timeout,

    })

    if (res.data.status_msg == "succeed") {
      message.success("创建数据集成功"); store.commit("changedataSetNumber"); reqclick();
    } else { message.warning("创建数据集失败") }
  } catch (err: any) {
    message.error("err", err);
  }

}
async function editeDataset(id: String) {
  const body = http.Body.form({
    name: formState.name,
    desc: formState.desc,
    replica: formState.replica.toString(),
    tags: formState.tags
  });
  try {
    const res:any = await http.fetch(config.baseURL + '/api/v1/dataset/' + id, {
      method: 'PATCH',
      body: body,
      timeout: config.timeout
    })
    if (res.data.status_msg == "succeed") {
      message.success("编辑数据集成功");
      store.commit("changedataSetNumber");
      reqclick();

    }
    else { message.warning("编辑数据集失败") }
  } catch (err: any) {
    message.error("err", err);
  }
}
async function getDetail(id: String) {
  try {
    const res:any = await http.fetch(config.baseURL + '/api/v1/dataset/' + id, {
      method: 'GET',
      timeout: config.timeout
    })
    if (res.data.status_msg == "succeed") {
      formState.name = res.data.dataset.name
      formState.desc = res.data.dataset.desc
      formState.replica = res.data.dataset.replica
      formState.tags = res.data.dataset.tags[0]
    } else { message.warning("获取数据集详情失败") }
  } catch (err: any) {
    message.error("err", err);
  }
}
const onSubmit = () => {
  formRef.value
    .validate()
    .then(() => {
      if (props.title === "创建数据集") {
        createDataset()
      }
      else { editeDataset(id.value) }


    })
    .catch((error: any) => {
      console.log("error", error);
    });
};
const resetForm = () => {
  formRef.value.resetFields();
};
const options = [
  { label: "中文分词", value: "中文分词" },
  { label: "图像分类", value: "图像分类" },
  { label: "数据增强", value: "数据增强" },
  { label: "文本分类", value: "文本分类" },
  { label: "目标检测", value: "目标检测" },
  { label: "目标跟踪", value: "目标跟踪" },
  { label: "语义分割", value: "语义分割" },
  { label: "音频分类", value: "音频分类" },
];
const handleChange = () => { };
</script>
<style scoped>
:deep(.ant-alert-warning) {
  background-color: #fff;
  border: 0px;
  height: 20px;
  position: relative;
  top: 5px
}
</style>
