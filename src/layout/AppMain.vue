
<script setup lang="ts">
import { h,ref } from 'vue';
import { QuestionCircleOutlined } from '@ant-design/icons-vue';

const selectedHomeKeys = ref([]);

const handleClick = (menuInfo:any) => {
  console.log('click ', menuInfo);
};


</script>
<template>
  <a-layout>
    <a-layout-header>

    <a-row justify="space-between">
      <a-col :span="8">
        <div class="header_logo" />
        <a-menu
          v-model:selectedKeys="selectedHomeKeys"
          theme="dark"
          mode="horizontal"
          :style="{ lineHeight: '64px' }"
        >
          <a-menu-item key="1">数据集</a-menu-item>
          <a-menu-item key="2" @click="handleClick">任务管理</a-menu-item>
        </a-menu></a-col>
      <a-col :span="4">
        <div class="header_right">
                <a
                target="_blank"
                href="https://pro.ant.design/docs/getting-started"
                class="question_action"
                >
                    <QuestionCircleOutlined :style="{ color: '#fff' }"  />
                </a>
        </div>
      </a-col>
    </a-row>
        
        

    </a-layout-header>

    <a-layout class="main_layout">
      <a-layout class="main_content_layout">
        <a-layout-content class="main_content">
          <slot />
        </a-layout-content>
      </a-layout>
    </a-layout>
  </a-layout>
</template>
<style scoped>
.main_layout{
    width: 100%;
    height: 97vh;
    background-color: #F0F0F0;
}

.header_logo {
    float: left;
    width: 50px;
    height: 31px;
    margin: 10px 20px 16px 0;
    background-image: url('../assets/logo.png');
    background-repeat: no-repeat;
    background-size: contain;
  }

.header_right {
    float: right;
    margin-right: 1px;
    height: 100%;
}

.question_action {
    padding: 20px 12px;
}

.question_action:hover {
    background-color: #252a3d;
}

.main_content_layout{
    padding: 0 0 0 24px;
}

.main_content{
    margin: 0;
    min-height: 280px;
}
</style>