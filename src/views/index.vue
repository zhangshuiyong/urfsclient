<template>
  <el-row class="header" justify="end">
    <el-col :span="4">
      <div class="login">
        <div class="demo-basic--circle">
          <el-avatar
            src="https://cube.elemecdn.com/3/7c/3ea6beec64369c2642b92c6726f1epng.png"
            :size="22"
          />
        </div>
        <el-dropdown @command="handleCommand">
          <span class="el-dropdown-link">
            <div class="user">user</div>
            <el-icon class="el-icon--right icon">
              <arrow-down />
            </el-icon>
          </span>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item>退出</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </el-col>
  </el-row>
  <el-tabs :tab-position="'left'" style="height: 90vh" class="demo-tabs">
    <el-tab-pane>
      <template #label>
        <span class="custom-tabs-label">
          <el-icon><UploadFilled /></el-icon>
          <span class="sidebar">文件上传</span>
        </span>
      </template>
      <upload />
    </el-tab-pane>
    <!-- <el-tab-pane>
      <template #label>
        <span class="custom-tabs-label">
          <el-icon><Document /></el-icon>
          <span class="sidebar">历史记录</span>
        </span>
      </template>
      <history />
    </el-tab-pane>
    <el-tab-pane>
      <template #label>
        <span class="custom-tabs-label">
          <el-icon><Setting /></el-icon>
          <span class="sidebar">上传配置</span>
        </span>
      </template>
      <setting />
    </el-tab-pane> -->
  </el-tabs>
</template>
<script >
import upload from "../components/upload.vue";
// import history from "../components/history.vue";
// import setting from "../components/setting.vue";
import { ArrowDown } from "@element-plus/icons-vue";
export default {
  components: {
    upload,
    // history,
    // setting,
  },
};
</script>
<script setup>
import { useRouter } from "vue-router";
const router = useRouter();
const handleCommand = () => {
  router.push("/login");
};
</script>
<style scoped>
.example-showcase .el-dropdown-link {
  cursor: pointer;
  color: var(--el-color-primary);
  display: flex;
  align-items: center;
}
</style>
<style>
.demo-tabs > .el-tabs__content {
  padding: 32px;
  color: #6b778c;
  font-size: 32px;
  font-weight: 600;
}

.el-tabs--right .el-tabs__content,
.el-tabs--left .el-tabs__content {
  height: 100%;
}
.el-tabs__item {
  font-size: 18px;
  line-height: 18px;
  font-weight: 600;
  height:60px
}
.example-showcase .el-dropdown-link {
  cursor: pointer;
  color: var(--el-color-primary);
  display: flex;
  align-items: center;
}
.user {
  margin: 4px 0 0 10px;
  font-weight: 800;
  color: #fff;
  display: inline-block;
}
.demo-basic--circle {
  display: inline-block;
}
.sidebar {
  position: relative;
  top: -2px;
}
.header {
  background-color: #409eff;
  height: 50px;
  margin-bottom: 5px;
  padding-top: 10px;
}
.icon {
  color: #fff;
  position: relative;
  top: 3px;
}
.login {
  float: right;
  margin-right: 20px;
}
.el-icon{margin-right:4px;}
</style>

