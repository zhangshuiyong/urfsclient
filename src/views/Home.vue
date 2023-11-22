<template>
  <div class="wrapper">
    <a-row justify="center">
      <a-col :span="10">
        <a-row>
          <a-col :span="8"></a-col>
          <a-col :span="16">
            <a-form
              :model="formState"
              name="basic"
              :label-col="{ span: 8 }"
              :wrapper-col="{ span: 16 }"
              autocomplete="off"
              @finish="onFinish"
              @finishFailed="onFinishFailed"
              labelAlign="right"
            >
              <a-form-item
                label="用户名"
                name="username"
                :rules="[{ required: true, message: '用户名不能为空!' }]"
              >
                <a-input v-model:value="formState.username" />
              </a-form-item>

              <a-form-item
                label="密码"
                name="password"
                :rules="[{ required: true, message: '密码不能为空!' }]"
              >
                <a-input v-model:value="formState.password" />
              </a-form-item>

              <a-form-item
                name="remember"
                :wrapper-col="{ offset: 8, span: 16 }"
              >
                <a-checkbox v-model:checked="formState.remember"
                  >记住账号密码</a-checkbox
                >
              </a-form-item>

              <a-form-item :wrapper-col="{ offset: 14, span: 16 }">
                <a-button type="primary" html-type="submit">登录</a-button>
              </a-form-item>
            </a-form></a-col
          >
        </a-row>
      </a-col>
    </a-row>
  </div>
</template>
<script setup lang="ts">
import { reactive } from "vue";
import { message } from "ant-design-vue";
import { useRouter } from "vue-router";
interface FormState {
  username: string;
  password: string;
  remember: boolean;
}

const formState = reactive<FormState>({
  username: "",
  password: "",
  remember: true,
});
const onFinish = (values: any) => {
  success();
};

const onFinishFailed = (errorInfo: any) => {
  console.log("Failed:", errorInfo);
};
const success = () => {
  message.success("登录成功");
  router.push({
    path: "/dataSet",
  });
};
const router = useRouter();
</script>
<style scoped>
.wrapper {
  padding-top: 20%;
  background: url("../static/background.jpeg");
  background-size: 100% 100%;
  height: 100vh;
  background-repeat: no-repeat;
  min-width: 1200px;
}
:deep(.ant-form-item-required) {
  color: #fff !important;
  font-weight: 600;
  font-size: 20px !important;
}
:deep(.ant-checkbox-wrapper) {
  color: #fff !important;
}
</style>
