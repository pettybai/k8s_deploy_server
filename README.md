# deploy-server

## 用途

用于远程执行k8s客户端api

## 构建和运行

```
docker build -t  ccr.ccs.tencentyun.com/deploy/kubernet.deploy:$COMMIT_ID .

docker pull ccr.ccs.tencentyun.com/deploy/kubernet.deploy:latest

docker run -it -d ccr.ccs.tencentyun.com/deploy/kubernet.deploy:latest
```
