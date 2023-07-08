<script setup lang="ts">

import Upload from "./components/Upload.vue";

import zhCN from 'ant-design-vue/es/locale/zh_CN';

const locale = zhCN;

</script>

<template>
  <a-config-provider :locale="locale">
    <div class="container">
        <h1>Welcome to Tauri!</h1>

        <Upload />
    </div>
  </a-config-provider>
</template>

<style scoped>

.container {
  text-align: center;
}

</style>
