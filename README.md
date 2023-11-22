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

### Urchinclient app Path:


https://tauri.app/v1/api/js/path/#localdatadir


# How to use nydus-image cmd to create urchin image from a dir

> meta文件名固定为‘mata’，blob文件名自动生成。默认生成最新的v6版本镜像，以下为命令:
>
> ./nydus-image create -t dir-rafs -B ${生成镜像文件目录}/meta -D ${生成镜像文件目录} ${源文件夹}

# How to use nydus-image cmd to create urchin image from a file or dir
> 第一步：将文件或者文件夹转为targz压缩包(如果是targz文件直接跳到第二步)
> 第二步：
> ./nydus-image create -t targz-rafs -B ${生成镜像文件目录}/meta -D ${生成镜像文件目录} ${targz源文件路径}

## Reason for input same dir to create targz with dif hash
https://blog.csdn.net/aiyanzielf/article/details/108379400

## Reason for input same dir to create image with dif mode(dir-rafs vs targz-rafs) output dif image id
https://github.com/dragonflyoss/image-service/issues/1258

# How to use nydus-image cmd to unpack urchin image files
> ./nydus-image unpack -b ${blob文件路径} --output ./a.tar ${meta文件路径}

# What is xattr(com.apple.quarantine) in macos

https://www.cnblogs.com/Flat-White/p/17153264.html

