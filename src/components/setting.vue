<template>
  <div class="formWrapper">
    <el-form
      ref="ruleFormRef"
      :model="ruleForm"
      :rules="rules"
      label-width="120px"
      class="demo-ruleForm"
      :size="formSize"
      status-icon
    >
      <el-form-item label="AccessKey" prop="accessKey">
        <el-input v-model="ruleForm.accessKey" />
      </el-form-item>
      <el-form-item label="SecretKey" prop="secretKey">
        <el-input v-model="ruleForm.secretKey" />
      </el-form-item>
      <el-form-item label="Bucket" prop="bucket">
        <el-input v-model="ruleForm.bucket" />
      </el-form-item>
      <el-form-item label="域名" prop="domain">
        <el-input v-model="ruleForm.domain" />
      </el-form-item>
      <el-form-item label="存储区域" prop="region">
        <el-input v-model="ruleForm.region" />
      </el-form-item>
      <el-form-item label="存储路径" prop="path">
        <el-input v-model="ruleForm.path" />
      </el-form-item>
      <el-form-item>
        <div class="wrapper">
          <el-button type="primary" @click="submitForm(ruleFormRef)">
            保存
          </el-button>
        </div>
      </el-form-item>
    </el-form>
  </div>
</template>

<script lang="ts" setup>
import { reactive, ref } from "vue";
import type { FormInstance, FormRules } from "element-plus";

const formSize = ref("default");
const ruleFormRef = ref<FormInstance>();
const ruleForm = reactive({
  accessKey: "",
  secretKey: "",
  Bucket: "",
  domain: "",
  region: "",
  path: "",
});

const rules = reactive<FormRules>({
  accessKey: [{ required: true, message: "请输入accessKey", trigger: "blur" }],
  secretKey: [{ required: true, message: "请输入secretKey", trigger: "blur" }],
  bucket: [{ required: true, message: "请输入secretKey", trigger: "blur" }],
  domain: [{ required: true, message: "请输入domain", trigger: "blur" }],
  region: [{ required: true, message: "请输入region", trigger: "blur" }],
  path: [{ required: true, message: "请输入path", trigger: "blur" }],
});

const submitForm = async (formEl: FormInstance | undefined) => {
  if (!formEl) return;
  await formEl.validate((valid, fields) => {
    if (valid) {
      console.log("submit!");
    } else {
      console.log("error submit!", fields);
    }
  });
};
</script>
<style scoped>
.wrapper {
  margin: 0 auto;
}
.formWrapper {
  width: 70%;
  margin: 0 auto;
}
</style>
