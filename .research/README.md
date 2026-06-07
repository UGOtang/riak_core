# Riak Core 分布式系统设计全景图

> 📖 基于《数据密集型应用系统设计》(DDIA) 框架的深度分析

---

## 📚 文档导航

本系列文档按照 DDIA 的分类框架，从五个维度深入分析 Riak Core 的设计：

| 序号 | 主题 | 对应 DDIA 章节 | 核心问题 |
|------|------|----------------|----------|
| [01](./01_数据模型与一致性.md) | 数据模型与一致性 | 第5章《复制》 | 如何处理并发写入冲突？ |
| [02](./02_分区与复制.md) | 分区与复制 | 第6章《分区》 | 如何分布数据到多节点？ |
| [03](./03_分布式协调.md) | 分布式协调 | 第9章《一致性与共识》 | 如何让节点达成一致？ |
| [04](./04_故障检测与恢复.md) | 故障检测与恢复 | 第8章《分布式系统的麻烦》 | 如何处理节点故障？ |
| [05](./05_VNode架构.md) | VNode 架构 | 第6章《分区》 | 如何组织数据处理？ |

---

## 🎯 Riak Core 的设计哲学

### CAP 定理的权衡

```
              CAP 定理
                 │
          ┌──────┼──────┐
          │      │      │
        一致性  可用性  分区容错
          │      │      │
          └──┬───┴──┬───┘
             │      │
         Riak 选择: AP
         (牺牲强一致性，换取高可用)
```

### 核心设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 一致性模型 | 最终一致性 | 高可用，低延迟 |
| 冲突解决 | 向量时钟 + 应用层 | 不丢数据，灵活处理 |
| 分区策略 | 一致性哈希 | 最小化迁移 |
| 协调机制 | Gossip + Claimant | 简单，无单点 |
| 故障恢复 | 多层修复 | 全面覆盖 |

---

## 🗺️ 架构全景图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Riak Core 架构层级                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ 应用层 (如 riak_kv)                                          │   │
│  │  - Bucket/Key 操作                                           │   │
│  │  - MapReduce                                                 │   │
│  │  - 二级索引                                                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Riak Core 服务层                                              │   │
│  │                                                               │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │   │
│  │  │ VNode Master │ │ Coverage FSM │ │ Node Watcher │        │   │
│  │  │  请求路由     │ │  覆盖查询     │ │  节点监控     │        │   │
│  │  └──────────────┘ └──────────────┘ └──────────────┘        │   │
│  │                                                               │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │   │
│  │  │ Handoff Mgr  │ │   Gossip     │ │  Claimant    │        │   │
│  │  │  数据迁移     │ │  状态传播     │ │  集群协调     │        │   │
│  │  └──────────────┘ └──────────────┘ └──────────────┘        │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ VNode 层                                                      │   │
│  │                                                               │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐               │   │
│  │  │VNode 0 │ │VNode 1 │ │VNode 2 │ │VNode N │  ...          │   │
│  │  │WorkerPool│ │WorkerPool│ │WorkerPool│ │WorkerPool│             │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘               │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ 数据结构层                                                    │   │
│  │                                                               │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │   │
│  │  │ Vector Clock │ │    DVVSet    │ │  Hash Tree   │        │   │
│  │  │  因果追踪     │ │  冲突解决     │ │  数据比对     │        │   │
│  │  └──────────────┘ └──────────────┘ └──────────────┘        │   │
│  │                                                               │   │
│  │  ┌──────────────┐ ┌──────────────┐                          │   │
│  │  │   chash      │ │  chashbin    │                          │   │
│  │  │  一致性哈希   │ │  高效环存储   │                          │   │
│  │  └──────────────┘ └──────────────┘                          │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Ring (集群状态)                                               │   │
│  │                                                               │   │
│  │  - 分区所有权映射                                              │   │
│  │  - 成员状态                                                   │   │
│  │  - 元数据                                                     │   │
│  │  - 版本向量                                                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 核心概念速查

### 数据一致性

```
向量时钟 (vclock.erl):
  记录每个节点的修改次数，判断因果关系
  
  示例:
  [alice:2, bob:1] 表示 alice 修改了2次，bob 修改了1次
  
  规则:
  - descends(A, B) = true → A 比 B 新
  - 互不 descends → 并发冲突

DVVSet (dvvset.erl):
  向量时钟的改进版，精确追踪每个值
  
  优势:
  - 区分"相同值"和"并发写入相同值"
  - 支持多值保留 (siblings)
```

### 数据分布

```
一致性哈希 (chash.erl):
  SHA-1 哈希 → 环位置 → 顺时针找分区
  
  特点:
  - 固定分区数
  - 加节点只迁移部分数据
  - O(log n) 查找

优先列表 (riak_core_apl.erl):
  数据位置 → 后续 N 个节点
  
  包含:
  - 主副本 (primary)
  - 降级副本 (fallback)
```

### 分布式协调

```
Gossip 协议 (riak_core_gossip.erl):
  随机选择节点交换状态
  
  特点:
  - O(n) 消息复杂度
  - 最终一致性
  - 容错性好

Claimant (riak_core_claimant.erl):
  唯一的决策者
  
  职责:
  - 处理成员变更
  - 计算分区分配
  - 推进状态机
```

### 故障恢复

```
Hinted Handoff:
  节点宕机 → 其他节点临时托管
  
  流程:
  写入 → 检测宕机 → 保存 hint → 节点恢复 → 归还数据

Read Repair:
  读取时发现不一致 → 立即修复

AAE (Active Anti-Entropy):
  后台定期比对 Hash Tree → 发现差异 → 修复

Handoff:
  数据迁移机制
  - hinted: 故障恢复
  - ownership: 所有权转移
  - repair: 手动修复
```

### VNode 架构

```
VNode (riak_core_vnode.erl):
  分区的抽象，独立的状态机
  
  特点:
  - 每个分区一个 VNode
  - 独立处理请求
  - 支持 Handoff

Worker Pool:
  并行处理请求
  
  优势:
  - 慢请求不阻塞
  - 利用多核

Coverage FSM:
  覆盖查询状态机
  
  功能:
  - 访问所有分区
  - 收集合并结果
```

---

## 📊 关键代码文件索引

### 数据结构层

| 文件 | 功能 | 核心函数 |
|------|------|----------|
| `vclock.erl` | 向量时钟 | `descends/2`, `merge/1` |
| `dvvset.erl` | 点版本向量 | `sync/1`, `update/2` |
| `chash.erl` | 一致性哈希 | `key_of/1`, `successors/3` |
| `chashbin.erl` | 高效环存储 | `lookup/2` |

### 服务层

| 文件 | 功能 | 核心函数 |
|------|------|----------|
| `riak_core_ring.erl` | 环状态管理 | `reconcile/2`, `preflist/2` |
| `riak_core_gossip.erl` | Gossip 协议 | `random_gossip/1` |
| `riak_core_claimant.erl` | 集群协调 | `ring_changed/2` |
| `riak_core_node_watcher.erl` | 节点监控 | `service_up/2` |
| `riak_core_handoff_manager.erl` | Handoff 管理 | `add_outbound/6` |

### VNode 层

| 文件 | 功能 | 核心函数 |
|------|------|----------|
| `riak_core_vnode.erl` | VNode 行为 | `handle_command/3` |
| `riak_core_vnode_master.erl` | 请求路由 | `command/4` |
| `riak_core_coverage_fsm.erl` | 覆盖查询 | `waiting_results/2` |
| `riak_core_worker_pool.erl` | Worker Pool | `run/3` |

### 修复层

| 文件 | 功能 | 核心函数 |
|------|------|----------|
| `hashtree.erl` | Merkle Tree | `compare/4` |
| `riak_core_handoff_sender.erl` | 数据发送 | `send_data/2` |
| `riak_core_handoff_receiver.erl` | 数据接收 | `receive_loop/2` |

---

## 🎓 学习路径建议

### 初学者路径

```
第1步: 理解核心概念
  ├─ 阅读本文档 (README.md)
  └─ 了解 Riak Core 的整体架构

第2步: 数据模型
  ├─ 阅读 01_数据模型与一致性.md
  ├─ 研究 vclock.erl 的测试用例
  └─ 理解向量时钟的工作原理

第3步: 数据分布
  ├─ 阅读 02_分区与复制.md
  ├─ 研究 chash.erl
  └─ 理解一致性哈希

第4步: 分布式协调
  ├─ 阅读 03_分布式协调.md
  └─ 理解 Gossip 和 Claimant

第5步: 故障恢复
  ├─ 阅读 04_故障检测与恢复.md
  └─ 理解 Hinted Handoff 和 AAE

第6步: VNode 架构
  ├─ 阅读 05_VNode架构.md
  └─ 实现一个简单的 VNode
```

### 进阶路径

```
深入源码:
  ├─ 追踪一个完整的读写请求
  ├─ 研究 Handoff 的完整流程
  ├─ 分析 Ring 变更的传播路径
  └─ 理解 AAE 的 Hash Tree 比对

性能优化:
  ├─ 调整 Worker Pool 大小
  ├─ 优化 Gossip 频率
  ├─ 配置 Handoff 并发
  └─ 监控关键指标

实战应用:
  ├─ 基于 Riak Core 构建应用
  ├─ 处理集群扩缩容
  ├─ 处理故障场景
  └─ 性能调优
```

---

## 🔬 实验环境

### 运行测试

```bash
# 运行所有测试
make test

# 运行单个模块测试
rebar3 eunit --module=vclock
rebar3 eunit --module=chash
rebar3 eunit --module=riak_core_gossip
rebar3 eunit --module=riak_core_vnode

# 运行覆盖率测试
rebar3 cover
```

### 关键配置

```erlang
%% app.config 示例
{riak_core, [
    %% Ring 配置
    {ring_creation_size, 64},
    {default_bucket_props, [{n_val, 3}]},
    
    %% Gossip 配置
    {gossip_interval, 60000},
    {gossip_limit, {45, 10000}},
    
    %% Handoff 配置
    {handoff_port, 8099},
    {handoff_concurrency, 2},
    
    %% AAE 配置
    {anti_entropy, {on, []}},
    {anti_entropy_tick, 60000},
    
    %% VNode 配置
    {vnode_mailbox_limit, 1000}
]}.
```

---

## 📖 参考资料

### 官方文档

- [Riak Core Documentation](https://docs.riak.com/)
- [Riak KV Source Code](https://github.com/basho/riak_kv)

### 学术论文

1. **一致性哈希**: Karger et al., "Consistent Hashing and Random Trees" (1997)
2. **向量时钟**: Lamport, "Time, Clocks, and the Ordering of Events" (1978)
3. **DVVSet**: Almeida et al., "Dotted Version Vectors" (2014)
4. **Gossip 协议**: Demers et al., "Epidemic Algorithms for Replicated Database Maintenance" (1987)

### 相关书籍

- **《数据密集型应用系统设计》** - Martin Kleppmann
- **《分布式系统原理与范型》** - Andrew S. Tanenbaum
- **《Erlang/OTP 并发编程实战》** - Martin Logan

---

## 🤝 贡献

本系列文档基于 Riak Core 源码分析，欢迎补充和修正。

### 文档改进建议

1. 添加更多实际案例
2. 补充性能调优建议
3. 增加故障排查指南
4. 添加与其他系统的对比分析

---

## 📝 版本信息

- **Riak Core 版本**: 基于 `develop` 分支分析
- **分析日期**: 2026年6月7日
- **DDIA 参考**: 中文版第二版

---

*"简单性是可靠性的前提" - Edsger W. Dijkstra*

Riak Core 通过简单而优雅的设计，实现了高可用、可扩展的分布式系统。理解其设计哲学，对于构建可靠的分布式应用具有重要参考价值。
