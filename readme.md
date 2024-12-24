### 部署相关暂存. 


### 1. 创建network

```shell
docker network create kf_network --driver bridge
```


### 2. 启动依赖
```shell
docker compose up -d 
```