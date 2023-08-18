<script setup lang="ts">
import AppMain from '../layout/AppMain.vue'
import Upload from '../components/Upload.vue'
</script>

<template>
  <AppMain>
    <div class="dataset_content">
        <Upload />
    </div>
  </AppMain>
</template>

<style scoped>
.dataset_content{
    margin: 0;
    background-color: #fff;
    padding: 0 0 0 24px;
}
</style>