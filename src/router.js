import { createRouter,createWebHistory} from "vue-router";

const routes = [
    { path: "/", redirect: "/login" },
    {
        path: "/index",
        name: "index",
        component: () => import("./views/index.vue")
    },
    {
        path: "/login",
        name: "login",
        component: () => import("./views/login.vue")
    }
]

const router = createRouter({
    history: createWebHistory(),
    routes
});

export default router;

