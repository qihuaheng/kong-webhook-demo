# 可靠级 Webhook 异步投递系统

## 核心定位

构建一套具备「至少一次投递、指数级容错重试及全链路可观测」能力的业务事件推送引擎。

## 业务背景

在 SaaS 基础设施中，平台需将支付成功或监控告警等核心事件实时推送到客户回调接口。面对网络抖动或对端服务宕机，系统必须在确保请求来源可信的前提下，通过科学的重试机制收敛投递状态。

## 基础要求（MVP——系统闭环）

- **实现完整的事件流转**：事件异步入队 --> 分布式 Worker 消费 --> 触发 POST 指令。
- **容错算法**：针对超时或非 2xx 异常，严格执行指数退避 + 随机抖动（Jitter）重试策略。
- **终态管控**：耗尽重试配额后自动归档至死信队列（DLQ），防止系统积压。
- **安全加固**：请求封装标准 HMAC 签名请求头，确保上游身份的不可抵赖性。

## 本地开发

本地构建的 `webhookd-*` 镜像，下次 `up` 可以直接复用。

以后跑集成测试，需要先拉起依赖：

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis minio
docker compose -f deploy/docker-compose.yml --profile test up -d redis-cluster
docker compose -f deploy/docker-compose.yml --profile ha up -d pg-a pg-b
```

第二条用于 Redis Cluster 测试，第三条用于 PG 故障切换测试，不跑这两类测试可以不启动。

docker-compose 环境包括 Redis Cluster 和 PG HA 两组测试容器。不用时可以停掉：

```bash
docker compose -f deploy/docker-compose.yml --profile test --profile ha down -v
```
