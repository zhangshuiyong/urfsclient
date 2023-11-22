<template>
  <div>
    <a-modal v-model:open="open" width="1000px" title="任务管理" @ok="handleOk" @cancel="handleCancel" :centered="true">
      <p>
        <a-tabs v-model:activeKey="activeKey" destroyInactiveTabPane>
          <a-tab-pane key="1" tab="上传中">
            <taskTable state="Uploading" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="2" tab="等待">
            <taskTable state="Wait&ReadyUpload" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="3" tab="初始化">
            <taskTable state="Init" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="4" tab="已暂停">
            <taskTable state="Stop" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="5" tab="成功">
            <taskTable state="Success" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="6" tab="失败">
            <taskTable state="Failed" :TaskData="allTaskData" />
          </a-tab-pane>
          <a-tab-pane key="7" tab="副本同步中">
            <taskTable state="AsyncProcessing" :TaskData="allTaskData" />
          </a-tab-pane>
        </a-tabs>
      </p>
    </a-modal>
  </div>
</template>
<script lang="ts" setup>
import { ref } from "vue";
import taskTable from "./TaskTable.vue";
const open = ref<boolean>(true);
const activeKey = ref("1");
const emit = defineEmits<{
  (event: "closeTaskManagerDialg", val: boolean): void;
}>();
const handleOk = (e: MouseEvent) => {
  emit("closeTaskManagerDialg", false);
};
const handleCancel = (e: MouseEvent) => {
  emit("closeTaskManagerDialg", false);
};
const props = defineProps({
  allTaskData: {
    type: Array,
    default: []
  },
});
</script>
