<template>
  <el-row class="row-bg" :gutter="20">
    <el-col :span="6">
      <el-input v-model="input" placeholder="文件名"
    /></el-col>
    <el-col :span="6"
      ><div class="grid-content ep-bg-purple-light" />
      <el-button type="primary">搜索</el-button></el-col
    >
  </el-row>
  <el-table :data="tableData" style="width: 100%">
    <el-table-column prop="name" label="文件名" />
    <el-table-column prop="date" label="上传时间" />
    <el-table-column label="状态">
      <template #default="scope">
        <el-progress
          :percentage="scope.row.rate"
          v-if="scope.row.rate != 100"
        />
        <el-progress :percentage="scope.row.rate" status="success" v-else />
      </template>
    </el-table-column>
    <el-table-column label="操作">
      <template #default="scope">
        <el-button
          link
          type="primary"
          size="small"
          @click="handleEdit(scope.$index, scope.row)"
          >停止</el-button
        >
         <el-button
          link
          type="primary"
          size="small"
          @click="handleDelete(scope.$index, scope.row)"
          >删除</el-button
        >
      </template>
    </el-table-column>
  </el-table>
  <el-row class="row-bg" justify="end">
    <el-col :span="6"
      ><el-pagination small layout="prev, pager, next" :total="50"
    /></el-col>
  </el-row>
</template>

<script lang="ts" setup>
import { ref } from "vue";
const input = ref("");
const tableData = [
  {
    date: "2023-05-03",
    name: "test-uzip-fail3",
    rate: "40",
  },
  {
    date: "2023-04-02",
    name: "test-swap",
    rate: "10",
  },
  {
    date: "2023-03-04",
    name: "mnist_test",
    rate: "100",
  },
  {
    date: "2023-05-18",
    name: "hymenoptera_data",
    rate: "5",
  },
  {
    date: "2023-05-18",
    name: "dataset1",
    rate: "0",
  },
  {
    date: "2023-05-18",
    name: "MNIST",
    rate: "0",
  },
  {
    date: "2023-05-18",
    name: "CIFAR10",
    rate: "0",
  },
  {
    date: "2023-05-18",
    name: "MNISTData-npu",
    rate: "100",
  },
  {
    date: "2023-05-18",
    name: "MSCOCO2017",
    rate: "20",
  },
];
const handleEdit = (index: number, row: User) => {
  console.log(index, row);
};
const handleDelete = (index: number, row: User) => {
  console.log(index, row);
};
</script>
<style scoped>
.row-bg {
  margin-top: 10px;
}
</style>
