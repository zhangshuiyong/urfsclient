<script setup lang="ts">
import AppMain from "../layout/AppMain.vue";
import DataList from "./DataList.vue";
import DataDetail from "./DatasetDetail.vue";
import { useStore } from "vuex";
import { watch, ref } from "vue";
const store = useStore();
const show = ref<boolean>(true);
watch(
  () => store.state.dataPage,
  (val) => {
    if (val === "list") {
      show.value = true;
    }
    if (val === "detail") {
      show.value = false;
    }
  }
);
</script>
<template>
  <AppMain>
    <div class="dataset_content">
      <DataList v-if="show"/>
      <DataDetail v-if="!show" />
    </div>
  </AppMain>
</template>

<style scoped>
.dataset_content {
  margin: 0;
  background-color: #fff;
  height:100vh
}
</style>
