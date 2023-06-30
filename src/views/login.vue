<template>
  <div class="wrapper">
    <el-row class="mt1">
      <el-col :span="24" class="row-bg" :offset="6">
        <el-row justify="center">
          <el-col :span="12" :offset="4">
            <div class="title">欢迎使用</div>
          </el-col>
        </el-row>
        <el-row justify="center" :offset="6">
          <el-col :span="8">
            <div class="grid-content ep-bg-purple-light" />
            <el-form :model="form" ref="ruleFormRef" :rules="rules">
              <el-form-item label="" prop="username">
                <el-input v-model="form.username" placeholder="请输入用户名" />
              </el-form-item>
              <el-form-item label="" prop="password">
                <el-input
                  v-model="form.password"
                  placeholder="请输入密码"
                  type="password"
                  show-password
                />
              </el-form-item>
              <el-form-item label="记住密码">
                <el-checkbox
                  label=""
                  name="type"
                  @change="isMemember"
                  v-model="remember"
                />
              </el-form-item>
              <el-form-item style="text-align: center">
                <el-button type="primary" @click="onSubmit(ruleFormRef)"
                  >登录</el-button
                >
              </el-form-item>
            </el-form>
          </el-col>
        </el-row>
      </el-col>
    </el-row>
  </div>
</template>

<script lang="ts" setup>
import { reactive, ref } from "vue";
import { ElMessage } from "element-plus";
import { invoke } from "@tauri-apps/api/tauri";
import { useRouter } from "vue-router";
import type { FormInstance, FormRules } from "element-plus";
import { getCurrentInstance } from "vue";
import { onMounted } from "vue";
const router = useRouter();
const ruleFormRef = ref<FormInstance>();
const internalInstance = getCurrentInstance();
const internalData = internalInstance.appContext.config.globalProperties;
const form = reactive({
  username: "",
  password: "",
});
const remember = ref(false);
const rules = reactive<FormRules>({
  username: [{ required: true, message: "请输入用户名", trigger: "blur" }],
  password: [{ required: true, message: "请输入密码", trigger: "blur" }],
});
onMounted(() => {
    (form.username = internalData.$cookies.get("usernmae")),
    (form.password = internalData.$cookies.get("password"));
});
const onSubmit = async (formEl: FormInstance | undefined) => {
  if (!formEl) return;
  await formEl.validate((valid, fields) => {
    if (valid) {   
        ElMessage({
            message: "登录成功!",
            type: "success",
          });
        router.push("/index")
    } else {
      console.log("error submit!", fields);
    }
  });
};
const setCook = () => {
    internalData.$cookies.set("usernmae", form.username),
    internalData.$cookies.set("password", form.password);
};
const isMemember = () => {
  if (remember) {
    setCook();
  } else {
  }
};
</script>
<style scoped>
.el-button {
  width: 100%;
}

.row-bg {
  margin-top: 10%;
}

.title {
  font-size: 22px;
  color: aliceblue;
  margin-bottom: 20px;
}

.wrapper {
  background-image: url(../assets/background.jpeg);
  background-repeat: no-repeat;
  background-size: 100% 100%;
  height: 100vh;
  overflow: hidden;
  min-width: 800px;
}

.mt1 {
  width: 900px;
  margin: 0 auto;
  padding-top: 15%;
}
</style>
