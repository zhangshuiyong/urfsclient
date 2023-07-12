# urfsclient

Frontend of Uchin2, will be installed in user multi-platform devices

# How to debug in UI layer

1. Install code package 

> npm i
>
> npm run tauri dev

2. After occur the app window

> ***Right-click*** in the app window

3. find the WebBrowser dev window

> Click the ***Inspect Element***

# How to dev in UI layer

### Log:
import ***info***, ***warn***, ***error***, and ***debug*** functions,for example: 
> import { info } from "tauri-plugin-log-api";
>
> info("[ui] click btn");

# How to use nydus-image cmd to create urchin image from a dir

> meta文件名固定为‘mata’，blob文件名自动生成。默认生成最新的v6版本镜像，以下为命令:
>
> ./nydus-image create -t dir-rafs -B ${生成镜像文件目录}/meta -D ${生成镜像文件目录} ${源文件夹}
