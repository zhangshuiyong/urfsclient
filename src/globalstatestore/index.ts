import { createStore } from 'vuex'

//Vuex 的状态存储是响应式的。
//作用是给界面/组件层使用，读取、修改这些共享状态
export default createStore({
  state: {
    lang: 'zh_CN',
    loading: false,
    dataType: "persional",
    dataPage:"list",
    dataSetId:"",
    dataSetVersion:"default",
    dataSetNumber:true
  },
  getters: {
    //以state为原料，加工返回一个新的state
  },
  mutations: {
    //纯粹简单地同步代码更改state
    changeLang2En(state: any) {
      state.lang = 'en_US';
    },
    changeLang2Zh(state: any) {
      state.lang = 'zh_CN';
    },
    setLoading(state: any, value: boolean) {
      state.loading = value;
    },
    changeDataType(state: any, data: string) {
      state.dataType = data
    },
    changeDataPage(state: any, data: string) {
      state.dataPage = data
    },
    changeDatasetId(state: any, data: string) {
      state.dataSetId = data
    },
    changeDataSetVersion(state: any, data: string) {
      state.dataSetVersion = data
    },
    changedataSetNumber(state: any) {
      state.dataSetNumber = !state.dataSetNumber
    },
  },
  actions: {
    //同步/异步复杂逻辑 + 更改state
  }
});