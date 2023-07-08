<script setup lang="ts">

import { h,ref } from 'vue';
import { MailOutlined, CalendarOutlined } from '@ant-design/icons-vue';

import Upload from "./components/Upload.vue";

import zhCN from 'ant-design-vue/es/locale/zh_CN';

const locale = zhCN;

const selectedHomeKeys = ref([]);
const selectedSiderKeys = ref([]);
const openSiderKeys = ref(['2']);
const items = ref([
  {
    key: '1',
    icon: () => h(MailOutlined),
    label: '海胆数据集',
    title: '海胆数据集',
  },
  {
    key: '2',
    icon: () => h(CalendarOutlined),
    label: '任务管理',
    title: '任务管理',
  },
]);

const handleClick = (menuInfo:any) => {
  console.log('click ', menuInfo);
};

</script>

<template>
    <a-layout>
      <a-layout-header class="header">
        <div class="logo" />
        <a-menu
          v-model:selectedKeys="selectedHomeKeys"
          theme="dark"
          mode="horizontal"
          :style="{ lineHeight: '64px' }"
        >
          <a-menu-item key="1">搜索</a-menu-item>
          <a-menu-item key="2">帮助</a-menu-item>
        </a-menu>
      </a-layout-header>
      <a-layout>
        
        <a-layout-sider minHight="500px" width="200px" style="background: #fff">
            <a-menu
                v-model:openKeys="openSiderKeys"
                v-model:selectedKeys="selectedSiderKeys"
                mode="inline"
                :style="{ height: '100%', borderRight: 0 }"
                :items="items"
                @click="handleClick"
            />
        </a-layout-sider>
        <a-layout style="padding: 0 24px 24px">
          <a-layout-content
            :style="{ background: '#fff', margin: 0, minHeight: '500px' }"
          >
            <a-config-provider :locale="locale">
                <div class="container">
                    <Upload />
                </div>
            </a-config-provider>
          </a-layout-content>
          <a-layout-footer style="text-align: center">
            Urchin ©2023 Created by Openi
          </a-layout-footer>
        </a-layout>
      </a-layout>
    </a-layout>
  </template>

  <style scoped>
   .logo {
    float: left;
    width: 50px;
    height: 31px;
    margin: 10px 20px 16px 0;
    background-image: url('./assets/logo.png');
    background-repeat: no-repeat;
    background-size: contain;
  }
  
  .ant-row-rtl .logo {
    float: right;
    margin: 16px 0 16px 24px;
  }
  
  </style>
