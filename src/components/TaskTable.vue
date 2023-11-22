<template>
    <a-table :columns="columns" :data-source="taskList" :pagination="false" :loading=show>
        <template #bodyCell="{ column, record }">
            <template v-if="column.key === 'id'">
                <span style="width:180px!important">{{ record.name }}</span>
            </template>
            <template v-if="column.key === 'versionName'">
                <span>
                    {{ record.versionName }}
                </span>
            </template>
            <template v-if="column.key === 'size'">
                <span>
                    {{ record.size === "0" ? "计算中" : record.size }}
                </span>
            </template>
            <template v-if="column.key === 'path'">
                <span>
                    {{ record.path }}
                </span>
            </template>
            <template v-if="column.key === 'createDate'">
                <span>
                    {{ record.createDate }}
                </span>
            </template>
            <template v-if="column.key === 'status'">
                <a-progress :stroke-color="{
                    '0%': '#108ee9',
                    '100%': '#87d068',
                }" :percent="parseInt(record.state)" v-if="typeof (record.state) == 'number'" />
                <span v-if="typeof (record.state) == 'string'">
                    {{ record.state }}
                </span>
            </template>
            <template v-else-if="column.key === 'operate'">
                <span class="m-l" v-if="props.state == 'Uploading'&&(record.name!=='已删除'&&record.versionName!=='已删除')">
                    <a @click="stop_upload(record)">暂停</a>
                </span>
                <span @click="terminate_upload(record)" class="m-l"
                    v-if="props.state !== 'Uploading' && props.state !== 'Init' && props.state !== 'AsyncProcessing'&&(record.name!=='已删除'&&record.versionName!=='已删除')">
                    <a>删除</a>
                </span>
                <span @click="terminate_uploading(record)" class="m-l" v-if="props.state === 'Uploading'&&(record.name!=='已删除'&&record.versionName!=='已删除')">
                    <a>删除</a>
                </span>
                <span @click="restart_upload(record)" class="m-l" v-if="props.state === 'Stop' || props.state === 'Failed'&&(record.name!=='已删除'||record.versionName!=='已删除')">
                    <a>继续上传</a>
                </span>
            </template>
        </template>
    </a-table>
</template>
<script lang="ts" setup>
import { invoke } from "@tauri-apps/api/tauri";
import { message } from "ant-design-vue";
import { info, error } from "tauri-plugin-log-api";
import { onMounted, reactive, onUnmounted, ref, getCurrentInstance } from 'vue'
import config from "../util/config";
import { formatSize } from "../util/index";
import moment from "moment";
const timer = ref()
const instance = getCurrentInstance();
const allow = ref<boolean>(true);
onMounted(() => {
    if (props.TaskData.length !== 0) {
        allTaskData.length = 0
        props.TaskData.forEach((item: any) => {
            allTaskData.push(item)
        })
        updateState()
        timer.value = setInterval(updateState, 2000);
    } else {
        show.value = false,
            clearTimeout(timer.value)
        timer.value = ""
    }
})
onUnmounted(() => {
    clearTimeout(timer.value)
    timer.value = ""
})
const props = defineProps({
    state: {
        type: String,
        default: ""
    },
    TaskData: {
        type: Array,
        default: []
    },
});
const columns = [
    {
        title: "名称",
        dataIndex: "name",
        key: "id",
        width: "150px",
        align: "center",
        resizable: false,
    },
    {
        title: "版本",
        dataIndex: "versionName",
        key: "versionName",
        width: "120px",
        align: "center",
        resizable: false,
    },
    {
        title: "大小",
        dataIndex: "size",
        key: "size",
        width: "150px",
        align: "center",
        resizable: false,
    },
    {
        title: "路径",
        key: "path",
        align: "center",
        width: "120px",
        dataIndex: "path",
        resizable: false,
    },
    {
        title: "创建时间",
        key: "createDate",
        align: "center",
        width: "220px",
        dataIndex: "createDate",
        resizable: false,
    },
    {
        title: "状态",
        dataIndex: "status",
        key: "status",
        align: "center",
        width: "180px",
        resizable: false,
    },
    {
        title: "操作",
        key: "operate",
        align: "center",
        width: "220px",
        resizable: false,
    },
];
interface dataType {
    id: string,
    state: string | Number | any,
    size: string | Number,
    path: string,
    createDate: string,
    version: string

}
interface taskType {
    id: string,
    state: string | Number | any,
    name: string | null,
    size: string | Number,
    path: string,
    createDate: string,
    version: string,
    versionName: string

}
// const data: dataType[] = reactive([]);
const allTaskData: taskType[] = reactive([]);
let taskData: taskType[] = reactive([]);
const taskList: taskType[] = reactive([]);
const show = ref<boolean>(true);
async function restart_upload(record: dataType) {
    if (allow.value === true) {
        try {
            allow.value = false
            var res: any = await invoke("start_upload", {
                req: JSON.stringify({
                    dataset_id: record.id,
                    dataset_version_id: record.version,
                    dataset_source: record.path,
                    server_endpoint: config.baseURL
                })
            });
            let Data = JSON.parse(res)
            if (Data.status_code == 0) {
                message.success('上传请求已发送');
                allow.value = true
                info(`上传请求返回: ${res}`);
                clearTimeout(timer.value)
                timer.value = ""
                updateState()
                timer.value = setInterval(updateState, 2000);
            }
            else {
                message.warning("上传请求失败")
            }

        } catch (err: any) {
            message.error('上传出错：', err);
            error(`上传出错: ${err}`);
            allow.value = true
        }
    } else {
        message.warning("请求发送中")
    }
}
async function stop_upload(record: dataType) {
    if (allow.value === true) {
        try {
            allow.value = false
            const res: any = await invoke("stop_upload", {
                req: JSON.stringify({
                    dataset_id: record.id,
                    dataset_version_id: record.version,
                }),
            });
            let Data = JSON.parse(res)
            if (Data.status_code == 0) {
                message.success("暂停任务成功");
                allow.value = true;
                clearTimeout(timer.value)
                timer.value = ""
                updateState()
                timer.value = setInterval(updateState, 2000);
            } else {
                message.warning(res)
            }
        } catch (err: any) {
            message.error("暂停出错：", err);
            allow.value = true
            // error(`暂停上传出错: ${err}`);
        }
    } else { message.warning("请求发送中") }
}
async function terminate_uploading(record: dataType) {
    if (allow.value === true) {
        try {
            allow.value = false;
            const res: any = await invoke("terminate_upload", {
                req: JSON.stringify({
                    dataset_id: record.id,
                    dataset_version_id: record.version,
                }),
            });
            let Data = JSON.parse(res)
            if (Data.status_code == 0) {
                message.success("删除任务成功");
                allow.value = true
                clearTimeout(timer.value)
                timer.value = ""
                updateState()
                timer.value = setInterval(updateState, 2000);
            } else {
                message.warning(res)
            }
        } catch (err: any) {
            message.error("删除任务出错：", err);
            allow.value = true
            // error(`终止上传出错: ${err}`);
        }
    } else {
        message.warning("请求发送中")
    }
}
async function terminate_upload(record: dataType) {
    if (allow.value === true) {
        try {
            allow.value = false
            const res: any = await invoke("delete_history_task", {
                req: JSON.stringify({
                    dataset_id: record.id,
                    dataset_version_id: record.version,
                }),
            });
            let Data = JSON.parse(res)
            if (Data.status_code == 0) {
                message.success("删除任务成功");
                allow.value = true
                clearTimeout(timer.value)
                timer.value = ""
                updateState()
                timer.value = setInterval(updateState, 2000);
            } else {
                message.warning(res)
            }
        } catch (err: any) {
            message.error("删除出错：", err);
            allow.value = true
            // error(`暂停上传出错: ${err}`);
        }
    } else {
        message.warning("请求发送中")
    }
}
async function updateState() {
    taskData.length = 0
    allTaskData.forEach((item: any) => {
        if (typeof item.state == "string") {
            taskData.push(item)
        } else {
            if (typeof item.state == "number") {
                taskData.push(item)
            }
        }
    }
    )
    try {
        info("[ui] click get_history btn");
        let res: any = await invoke("get_history", { req: JSON.stringify({ req: "{}" }) });
        if (typeof res === "string") {
            let Data = JSON.parse(res)
            Data = JSON.parse(Data["payload_json"])
            if (Data.length !== 0) {
                interface infoData {
                    id: string,
                    state: string | Object,
                    version: string,
                    size: string,
                    path: string,
                    createDate: string,
                }
                let infoData: infoData[] = reactive([]);
                Data.forEach((item: { dataset_id: string, dataset_status: any, dataset_version_id: string, local_dataset_size: string, local_dataset_path: string, create_timestamp: string }) => {
                    if (typeof item.dataset_status === "string") {
                        if (typeof item.dataset_status === "string") {
                            infoData.push({ id: item.dataset_id, state: item.dataset_status, version: item.dataset_version_id, size: formatSize(item.local_dataset_size.toString()), path: item.local_dataset_path, createDate: moment(parseInt(item.create_timestamp) * 1000).format('YYYY-MM-DD-HH:mm:ss') })
                        }
                    } else if (typeof item.dataset_status === "object" && props.state == "Uploading") {
                        infoData.push({ id: item.dataset_id, state: parseInt(item.dataset_status.Uploading), version: item.dataset_version_id, size: formatSize(item.local_dataset_size.toString()), path: item.local_dataset_path, createDate: moment(parseInt(item.create_timestamp) * 1000).format('YYYY-MM-DD-HH:mm:ss') })
                    }
                })
                taskData.forEach((item: { id: string; state: string | Object; version: string; size: string | Number, path: string, createDate: string }) => {
                    infoData.forEach(Item => {
                        if (item.id === Item.id && item.version === Item.version) {
                            item.state = Item.state
                            item.size = Item.size
                            item.path = Item.path
                            item.createDate = Item.createDate
                        }
                    })
                })
                let idArray: string[] = []
                infoData.forEach(
                    item => {
                        idArray.push(item.id + item.version)
                    }
                )
                taskData = taskData.filter(item => {
                    if (idArray.indexOf(item.id + item.version) !== -1) {
                        return item
                    }
                })
                taskData = taskData.filter(item => {
                    if (typeof item.state == "string" && props.state.indexOf(item.state) != -1) {
                        return item
                    } else if (typeof item.state == "number" && props.state === "Uploading") {
                        return item
                    }
                })
                taskList.length = 0
                taskData.forEach(item => {
                    taskList.push(item)
                })
                show.value = false
                if (instance && instance.proxy) {
                    instance.proxy.$forceUpdate()
                }
            } else {
                taskList.length = 0
                show.value = false
            }

        }
    } catch (err: any) {
        message.error("获取文件上传历史错误：", err);
        console.log(err, "err")
    }
}
</script>
<style scoped>
.m-l {
    margin-right: 20px;
}

:deep(.ant-table) {
    min-height: 352px !important;
}

:deep(.ant-empty-normal) {
    margin: 10px 0 !important;
}

:deep(.ant-table-placeholder) {
    height: 100px;

}

:deep(.ant-empty) {
    display: none;
}
</style>
