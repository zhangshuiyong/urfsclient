<script setup lang="ts">

import { useStore } from 'vuex';

const globalShareStore = useStore();

import Upload from "./components/Upload.vue";

import zhCN from 'ant-design-vue/es/locale/zh_CN';


const localeLang = zhCN;

</script>

<template>
  <!-- vue-router 4.x 路由出口 -->
  <!-- vue-router 4.x 路由匹配到的组件将渲染在这里 -->
  <a-config-provider :locale="localeLang">
    <router-view />
  </a-config-provider>
</template>

<style scoped>

</style>
