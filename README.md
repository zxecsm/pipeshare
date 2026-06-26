# pipeshare

基于 Web 管道流技术的零无感快传工具，不占服务器磁盘，文件、文本秒级跨端互传

```
services:
  pipeshare:
    image: 'ghcr.io/zxecsm/pipeshare:latest'
    # image: 'zxecsm/pipeshare:latest'
    container_name: pipeshare
    restart: unless-stopped
    ports:
      - '3000:3000'
```

```
sudo docker run -d \
  --name pipeshare \
  --restart unless-stopped \
  -p 3000:3000 \
  ghcr.io/zxecsm/pipeshare:latest
  # zxecsm/pipeshare:latest
```
