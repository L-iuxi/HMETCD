# HMETCD

一个用 Go 实现的 etcd-like 分布式 KV 存储，支持 Raft 共识、MVCC、TTL 和 Watch。

## 架构

```
Client ──gRPC──→ KvServer ──→ Raft ──→ ApplyLoop ──→ BadgerDB
                         │                       
                         └── LeaseManager ──→ TTL Expire Worker
                         └── WatcherManager ──→ Watch Stream
```

## 功能

- **Raft 共识** — 领导者选举、日志复制、快照安装、WAL 持久化
- **MVCC** — 版本化读写（`key/rev`）、乐观锁（WRONG_VERSION）、历史版本查询
- **TTL / Lease** — 按 key 绑定 lease，到期自动删除
- **Watch** — 基于 gRPC 流实时监听 key 变更
- **幂等** — 基于 ClientId + RequestId 去重

## 快速开始

```bash
go run cmd/main.go
```

启动 5 节点集群（默认地址 `127.0.0.1:50051~50055`）。

## 依赖

- [BadgerDB v4](https://github.com/dgraph-io/badger) — 底层 KV 存储
- gRPC + Protocol Buffers — 节点间通信

## 项目结构

```
clerk/          客户端 SDK
cmd/            启动入口
server/         gRPC 服务注册
proto/          Protobuf 定义 + 生成代码
internal/
  db/           BadgerDB 封装
  kv/           KV Server + ApplyLoop
  raft/         Raft 共识实现
  lease/        Lease/TTL 管理
  watch/        Watch 管理器
  wal/          预写日志
  labgob/       Go binary 编解码
  persister/    状态持久化
  type/         数据结构
```
