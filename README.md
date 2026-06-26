# pipeshare

基于 Web 管道流技术的零无感快传工具，不占服务器磁盘，文件、文本秒级跨端互传
![pipeshare](https://raw.githubusercontent.com/zxecsm/pipeshare/main/pipeshare.png)

```
services:
  pipeshare:
    image: 'ghcr.io/zxecsm/pipeshare:latest'
    container_name: pipeshare
    restart: unless-stopped
    ports:
      - '3000:3000'
```

```
docker run -d \
  --name pipeshare \
  --restart unless-stopped \
  -p 3000:3000 \
  ghcr.io/zxecsm/pipeshare:latest
```

如果使用 `Nginx`，需要关闭代理缓冲区，允许数据直接穿透（Stream）：

```
# 关闭请求和响应缓冲区，开启真正的公网流式中转
proxy_buffering off;
proxy_request_buffering off;

# 顺便调大允许上传的最大文件限制，否则传大文件会报 413 Payload Too Large
client_max_body_size 0;
```
