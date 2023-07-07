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
