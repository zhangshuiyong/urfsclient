import { createApp } from "vue";

import Antd from 'ant-design-vue';
import 'ant-design-vue/dist/reset.css';

import App from "./App.vue";
import "./styles.css";

import { attachConsole } from "tauri-plugin-log-api";
attachConsole();

const app = createApp(App);

app.use(Antd).mount("#app");