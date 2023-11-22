<template>
  <div>
    <a-modal
      v-model:open="open"
      title="系统设置"
      @ok="handleOk"
      @cancel="handleCancel"
      :centered="true"
    >
      <a-form
        ref="formRef"
        :model="formState"
        :label-col="labelCol"
        :wrapper-col="wrapperCol"
      >
        <a-form-item label="最大并行任务数" name="taskNum">
          <a-input-number
            id="inputNumber"
            v-model:value="formState.taskNum"
            :min="3"
            addon-after="个"
          >
            <template #upIcon>
              <ArrowUpOutlined />
            </template>
            <template #downIcon>
              <ArrowDownOutlined />
            </template>
          </a-input-number>
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>
<script lang="ts" setup>
import { Dayjs } from "dayjs";
import { reactive, ref, toRaw } from "vue";
import type { UnwrapRef } from "vue";
import type { Rule } from "ant-design-vue/es/form";
import { message } from "ant-design-vue";
import { ArrowUpOutlined, ArrowDownOutlined } from "@ant-design/icons-vue";
interface FormState {
  taskNum: number;
}
const open = ref<boolean>(true);
const handleOk = (e: MouseEvent) => {
  onSubmit();
};
const handleCancel = (e: MouseEvent) => {
  reqclick();
};
const emit = defineEmits<{
  (event: "closeSettingDialg", val: boolean): void;
}>();
//const emit = defineEmits(['chilFun']) // 自定义chilFun事件
const reqclick = () => {
  emit("closeSettingDialg", false); //1212:传个父组件的数据
};
const formRef = ref();
const labelCol = { span: 6 };
const wrapperCol = { span: 10 };
const formState: UnwrapRef<FormState> = reactive({
  taskNum: 3,
});
const onSubmit = () => {
  formRef.value
    .validate()
    .then(() => {
      reqclick();
      message.success("系统设置成功");
    })
    .catch((error: any) => {
      console.log("error", error);
    });
};
const resetForm = () => {
  formRef.value.resetFields();
};
</script>
