# kafka-practice

用 Golang 操作 Kafka。

## Get started

本地运行 ZK 和 Kafka 容器：

```bash
$ docker-compose up
```

启动消费者：

```bash
# 可以开多个终端运行多个实例
$ go run pkg/consumer/consumer.go
$ go run pkg/consumer/consumer.go
```

启动生产者：

```bash
$ go run pkg/producer/producer.go
```
