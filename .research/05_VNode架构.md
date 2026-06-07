# VNode 架构 - Riak Core 的数据处理单元

> 📖 **对应DDIA章节**: 第六章《分区》中的"分区与二级索引"与第三章《存储与检索》

## 🎯 核心问题：如何在分布式系统中高效处理数据？

想象你在设计一个分布式数据库：
- 每个节点需要处理多个分区
- 请求可能来自任何地方
- 如何组织代码使其可扩展、可维护？

### 传统方案的问题

| 方案 | 问题 |
|------|------|
| 每个分区一个进程 | 进程数爆炸，资源消耗大 |
| 共享进程池 | 锁竞争严重 |
| 单线程处理 | 无法利用多核 |

### Riak Core 的答案：**VNode 行为模式 + Worker Pool**

---

## 🧪 实验一：VNode 行为模式

### 核心思想：每个分区是一个独立的状态机

```
传统架构:
┌─────────────────────────────────────────┐
│ Node1                                    │
│  ┌─────────────────────────────────┐    │
│  │     单一进程处理所有请求          │    │
│  │  (分区A, 分区B, 分区C...)        │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘

VNode 架构:
┌─────────────────────────────────────────┐
│ Node1                                    │
│  ┌────────┐ ┌────────┐ ┌────────┐      │
│  │VNode A │ │VNode B │ │VNode C │      │
│  │ 独立   │ │ 独立   │ │ 独立   │      │
│  │ 状态机 │ │ 状态机 │ │ 状态机 │      │
│  └────────┘ └────────┘ └────────┘      │
└─────────────────────────────────────────┘
```

### VNode 行为定义

```erlang
%% 文件: src/riak_core_vnode.erl

%% VNode 行为回调
-callback init([partition()]) ->
    {ok, ModState::term()} |
    {ok, ModState::term(), [vnode_opt()]} |
    {error, Reason::term()}.

-callback handle_command(Request::term(), Sender::sender(), ModState::term()) ->
    continue |                              %% 继续处理下一个命令
    {reply, Reply::term(), NewModState::term()} |  %% 同步回复
    {noreply, NewModState::term()} |        %% 不回复
    {async, Work::function(), From::sender(), NewModState::term()} |  %% 异步处理
    {stop, Reason::term(), NewModState::term()}.

-callback handle_coverage(Request::term(), keyspaces(), Sender::sender(), ModState::term()) ->
    ...  %% 处理覆盖查询

-callback handle_exit(pid(), Reason::term(), ModState::term()) ->
    ...  %% 处理链接进程退出

-callback handoff_starting(handoff_dest(), ModState::term()) ->
    {boolean(), NewModState::term()}.  %% 开始 handoff

-callback handoff_finished(handoff_dest(), ModState::term()) ->
    {ok, NewModState::term()}.  %% handoff 完成

-callback handle_handoff_data(binary(), ModState::term()) ->
    {reply, ok | {error, Reason::term()}, NewModState::term()}.

-callback encode_handoff_item(Key::term(), Value::term()) ->
    binary().

-callback is_empty(ModState::term()) ->
    {boolean(), NewModState::term()} |
    {false, Size::pos_integer(), NewModState::term()}.

-callback terminate(Reason::term(), ModState::term()) ->
    ok.

-callback delete(ModState::term()) -> 
    {ok, NewModState::term()}.
```

### VNode 状态机

```erlang
%% VNode 使用 gen_fsm 实现状态机
%% 状态转换图：

         ┌──────────┐
         │  started │ ← 新创建的 VNode
         └────┬─────┘
              │ init 成功
              ↓
         ┌──────────┐
    ┌───>│  active  │ ←─┐
    │    └────┬─────┘   │
    │         │         │
    │  handoff│         │handoff
    │  取消    │         │完成
    │         ↓         │
    │    ┌──────────┐   │
    │    │ handoff  │───┘
    │    │ (sending)│
    │    └──────────┘
    │         │
    │         │ delete
    │         ↓
    │    ┌──────────┐
    └────│ deleting │
         └──────────┘
              │
              ↓
         终止
```

### VNode 实现

```erlang
%% 文件: src/riak_core_vnode.erl

%% VNode 状态
-record(state, {
    mod,              %% 实现模块
    modstate,         %% 模块状态
    partition,        %% 分区 ID
    index,            %% 分区索引
    pool,             %% Worker Pool
    handoff_pid,      %% Handoff 进程
    forward,          %% 转发目标
    delete_after      %% 删除标记
}).

%% 初始化
init([Mod, Partition, Index]) ->
    case Mod:init([Partition]) of
        {ok, ModState} ->
            {ok, started, #state{mod=Mod, modstate=ModState, 
                                 partition=Partition, index=Index}};
        {ok, ModState, Opts} ->
            State = process_opts(Opts, #state{...}),
            {ok, started, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% started 状态：等待激活
started({activate, ManagerPid}, State) ->
    %% 通知 Manager 已就绪
    ManagerPid ! {vnode_started, self()},
    {next_state, active, State}.

%% active 状态：处理命令
active(#riak_vnode_req{request=Req, sender=Sender}=Msg, State) ->
    case State#state.forward of
        undefined ->
            %% 正常处理
            handle_command(Req, Sender, State);
        ForwardTo ->
            %% 转发到其他节点
            forward_request(ForwardTo, Msg),
            {next_state, active, State}
    end.
```

---

## 🧪 实验二：命令处理

### 命令结构

```erlang
%% 命令请求记录
-record(riak_vnode_req, {
    request :: term(),      %% 具体请求内容
    sender :: sender(),     %% 发送者信息
    keyspaces :: keyspaces()  %% 覆盖查询的键空间
}).

%% 发送者信息
-type sender() :: {pid(), reference()} | ignore.
```

### 同步命令

```erlang
%% 发送同步命令
-spec send_command(pid(), term()) -> term().
send_command(VNode, Request) ->
    Ref = make_ref(),
    VNode ! #riak_vnode_req{request=Request, sender={self(), Ref}},
    receive
        {Ref, Reply} -> Reply
    after 5000 ->
        {error, timeout}
    end.

%% 处理同步命令
handle_command(Req, {Pid, Ref}, State) ->
    case State#state.mod:handle_command(Req, {Pid, Ref}, State#state.modstate) of
        {reply, Reply, NewModState} ->
            Pid ! {Ref, Reply},
            {next_state, active, State#state{modstate=NewModState}};
        {noreply, NewModState} ->
            {next_state, active, State#state{modstate=NewModState}};
        {async, Work, From, NewModState} ->
            %% 分配给 Worker Pool
            spawn_async_work(Work, From, State),
            {next_state, active, State#state{modstate=NewModState}};
        {stop, Reason, NewModState} ->
            {stop, Reason, State#state{modstate=NewModState}}
    end.
```

### 异步命令

```erlang
%% 异步命令处理
handle_async({async_work_done, From, Result}, State) ->
    %% Worker 完成工作
    case From of
        {Pid, Ref} ->
            Pid ! {Ref, Result};
        ignore ->
            ok
    end,
    {next_state, active, State}.

%% 分配异步工作
spawn_async_work(Work, From, State) ->
    case State#state.pool of
        undefined ->
            %% 没有池，直接 spawn
            spawn(fun() -> 
                Result = Work(),
                State#state.mod ! {async_work_done, From, Result}
            end);
        Pool ->
            %% 使用 Worker Pool
            riak_core_worker_pool:run(Pool, Work, From)
    end.
```

---

## 🧪 实验三：覆盖查询 (Coverage Query)

### 什么是覆盖查询？

```
普通查询: 查询单个 key
  key → hash → partition → vnode → 结果

覆盖查询: 查询所有数据
  需要访问所有分区！
  
例如：
- 列出所有用户
- 统计数据总量
- 批量导出
```

### 覆盖查询策略

```erlang
%% 文件: src/riak_core_coverage_fsm.erl

%% 覆盖查询参数
%% VNodeSelector:
%%   - all: 必须获得完整覆盖
%%   - allup: 尽力覆盖，允许部分不可用

%% PrimaryVNodeCoverage:
%%   - all: 覆盖所有主 vnode
%%   - N: 只覆盖 N 个副本中的部分

%% 最小覆盖计算
%% 对于 N=3 的集群：
%% - PrimaryVNodeCoverage = all: 需要访问所有分区
%% - PrimaryVNodeCoverage = 1: 只需要访问 1/3 的分区
```

### 覆盖查询 FSM

```erlang
%% 文件: src/riak_core_coverage_fsm.erl

%% 覆盖查询状态机
init([Request, VNodeSelector, NVal, PrimaryVNodeCoverage, 
      NodeCheckService, VNodeMaster, Timeout, ModState]) ->
    %% 1. 计算覆盖计划
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Plan = riak_core_coverage_plan:create_plan(
        VNodeSelector, NVal, PrimaryVNodeCoverage, Ring
    ),
    
    %% 2. 向选中的 VNode 发送请求
    [send_coverage_req(VNode, Request) || VNode <- Plan],
    
    %% 3. 等待结果
    {ok, waiting_results, #state{
        plan=Plan,
        results=[],
        timeout=Timeout,
        modstate=ModState
    }}.

%% 等待结果状态
waiting_results({result, VNode, Result}, State) ->
    %% 收到一个结果
    Results = [{VNode, Result} | State#state.results],
    
    %% 检查是否全部完成
    case all_results_received(Results, State#state.plan) of
        true ->
            %% 完成，调用处理函数
            FinalResult = process_results(Results, State),
            {stop, normal, FinalResult};
        false ->
            {next_state, waiting_results, State#state{results=Results}}
    end.
```

### 覆盖计划生成

```erlang
%% 文件: src/riak_core_coverage_plan.erl

%% 生成最小覆盖计划
create_plan(all, NVal, PrimaryVNodeCoverage, Ring, UpNodes) ->
    %% 获取所有分区
    AllPartitions = riak_core_ring:all_owners(Ring),
    
    case PrimaryVNodeCoverage of
        all ->
            %% 需要覆盖所有分区
            filter_up_partitions(AllPartitions, UpNodes);
        N when N < NVal ->
            %% 只需要覆盖部分分区
            select_minimal_cover(AllPartitions, N, NVal, UpNodes)
    end.

%% 选择最小覆盖集
select_minimal_cover(Partitions, N, NVal, UpNodes) ->
    %% 对于每个 key，它会在 NVal 个分区中有副本
    %% 我们只需要选择其中的 N 个
    %%
    %% 策略：按分组选择
    %% 例如 NVal=3, N=1：
    %% 分区 [P1, P2, P3] 是一组（存储相同 key 的不同副本）
    %% 只需要选择其中一个
    
    Groups = group_partitions(Partitions, NVal),
    [select_one_from_group(G, UpNodes) || G <- Groups].
```

### 覆盖查询流程图

```
┌─────────────────────────────────────────────────────────────┐
│                   覆盖查询流程                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  客户端 ──> 发起覆盖查询                                     │
│              │                                               │
│              ↓                                               │
│  ┌────────────────────────────────────────┐                 │
│  │ Coverage FSM                           │                 │
│  │ 1. 计算覆盖计划                         │                 │
│  │    - 确定需要访问的 VNode              │                 │
│  │    - 最小化访问数量                    │                 │
│  └────────────────────────────────────────┘                 │
│              │                                               │
│              ↓                                               │
│  ┌────────────────────────────────────────┐                 │
│  │ 并发向选中 VNode 发送请求               │                 │
│  │                                        │                 │
│  │   VNode1 ←──┐                         │                 │
│  │   VNode2 ←──┼── Coverage FSM          │                 │
│  │   VNode3 ←──┘                         │                 │
│  └────────────────────────────────────────┘                 │
│              │                                               │
│              ↓                                               │
│  ┌────────────────────────────────────────┐                 │
│  │ 收集结果                               │                 │
│  │ - 等待所有 VNode 响应                  │                 │
│  │ - 处理超时                             │                 │
│  └────────────────────────────────────────┘                 │
│              │                                               │
│              ↓                                               │
│  ┌────────────────────────────────────────┐                 │
│  │ 合并结果                               │                 │
│  │ - 去重                                 │                 │
│  │ - 排序                                 │                 │
│  │ - 返回给客户端                         │                 │
│  └────────────────────────────────────────┘                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🧪 实验四：Worker Pool

### 为什么需要 Worker Pool？

```
问题：
  VNode 是单进程，处理慢请求会阻塞后续请求

解决方案：
  使用 Worker Pool 并行处理

架构：
  ┌─────────────────────────────────────────┐
  │           VNode                          │
  │  ┌─────────────────────────────────┐    │
  │  │         Worker Pool              │    │
  │  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐   │    │
  │  │  │ W1 │ │ W2 │ │ W3 │ │ W4 │   │    │
  │  │  └────┘ └────┘ └────┘ └────┘   │    │
  │  └─────────────────────────────────┘    │
  └─────────────────────────────────────────┘

请求队列:
  请求1 ──> Worker1 (处理中)
  请求2 ──> Worker2 (处理中)
  请求3 ──> 等待空闲 Worker
  请求4 ──> 等待空闲 Worker
```

### Worker Pool 实现

```erlang
%% 文件: src/riak_core_worker_pool.erl

-record(state, {
    workers,       %% Worker 进程列表
    waiting,       %% 等待队列
    queue_max      %% 最大队列长度
}).

%% 初始化 Worker Pool
init([PoolSize, WorkerMod, WorkerArgs]) ->
    %% 创建 Worker 进程
    Workers = [start_worker(WorkerMod, WorkerArgs) 
               || _ <- lists:seq(1, PoolSize)],
    {ok, #state{workers=Workers, waiting=[]}}.

%% 运行任务
run(Pool, Work, From) ->
    gen_server:cast(Pool, {run, Work, From}).

handle_cast({run, Work, From}, State) ->
    case find_free_worker(State#state.workers) of
        {ok, Worker} ->
            %% 分配给空闲 Worker
            Worker ! {work, Work, From},
            {noreply, State};
        none ->
            %% 加入等待队列
            Waiting = [{Work, From} | State#state.waiting],
            {noreply, State#state{waiting=Waiting}}
    end.

%% Worker 完成工作
handle_info({work_done, WorkerPid}, State) ->
    case State#state.waiting of
        [] ->
            %% 没有等待的任务
            {noreply, State};
        [{Work, From} | Rest] ->
            %% 分配下一个任务
            WorkerPid ! {work, Work, From},
            {noreply, State#state{waiting=Rest}}
    end.
```

### Worker 实现

```erlang
%% 文件: src/riak_core_vnode_worker.erl

%% Worker 进程
init([VNode, Mod, ModState]) ->
    {ok, #state{vnode=VNode, mod=Mod, modstate=ModState}}.

loop(State) ->
    receive
        {work, Work, From} ->
            %% 执行工作
            Result = Work(),
            %% 通知 VNode 完成
            State#state.vnode ! {async_work_done, From, Result},
            %% 通知 Pool 空闲
            worker_pool ! {work_done, self()},
            loop(State)
    end.
```

---

## 🧪 实验五：VNode Master - 请求路由

### VNode Master 的职责

```erlang
%% 文件: src/riak_core_vnode_master.erl

%% VNode Master 是 VNode 的管理者和路由器

%% 主要职责：
%% 1. 创建/销毁 VNode
%% 2. 路由请求到正确的 VNode
%% 3. 管理 VNode 状态
%% 4. 协调 Handoff
```

### 请求路由

```erlang
%% 文件: src/riak_core_vnode_master.erl

%% 发送命令到 VNode
command(Partition, Request, Sender, Mod) ->
    %% 1. 找到负责该分区的 VNode
    VNode = get_vnode(Partition, Mod),
    %% 2. 发送请求
    VNode ! #riak_vnode_req{request=Request, sender=Sender},
    ok.

%% 获取或创建 VNode
get_vnode(Partition, Mod) ->
    case ets:lookup(vnode_registry, {Mod, Partition}) of
        [{_, VNode}] ->
            VNode;
        [] ->
            %% 创建新 VNode
            {ok, VNode} = riak_core_vnode_sup:start_vnode(Mod, Partition),
            ets:insert(vnode_registry, {{Mod, Partition}, VNode}),
            VNode
    end.
```

### 命令协议

```erlang
%% 同步命令
sync_command(Index, Request, Mod, Timeout) ->
    VNode = get_vnode(Index, Mod),
    riak_core_vnode:send_command(VNode, Request).

%% 异步命令
async_command(Index, Request, Mod, Sender) ->
    VNode = get_vnode(Index, Mod),
    VNode ! #riak_vnode_req{request=Request, sender=Sender},
    ok.

%% 覆盖命令
coverage_command(Request, Mod, NVal, Timeout) ->
    %% 启动 Coverage FSM
    riak_core_coverage_fsm:start_link(Mod, Request, NVal, Timeout).
```

### 请求流程图

```
┌─────────────────────────────────────────────────────────────┐
│                   请求处理流程                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  客户端                                                      │
│    │                                                         │
│    │ 1. 发送请求                                             │
│    │    {Index, Request}                                     │
│    ↓                                                         │
│  VNode Master                                                │
│    │                                                         │
│    │ 2. 查找/创建 VNode                                      │
│    │    get_vnode(Index, Mod)                               │
│    ↓                                                         │
│  VNode Registry (ETS)                                        │
│    │                                                         │
│    │ 3. 返回 VNode Pid                                       │
│    ↓                                                         │
│  VNode Master                                                │
│    │                                                         │
│    │ 4. 转发请求                                             │
│    ↓                                                         │
│  VNode                                                       │
│    │                                                         │
│    │ 5. 处理请求                                             │
│    │    - 同步：直接处理                                     │
│    │    - 异步：分配给 Worker                               │
│    ↓                                                         │
│  返回结果给客户端                                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎨 实际应用示例

### 实现自定义 VNode

```erlang
%% 示例：实现一个简单的 KV 存储

-module(my_kv_vnode).
-behaviour(riak_core_vnode).

-export([init/1, handle_command/3, is_empty/1, 
         terminate/2, delete/1, handle_handoff_data/2,
         encode_handoff_item/2, handoff_starting/2,
         handoff_cancelled/1, handoff_finished/2]).

-record(state, {partition, data}).

init([Partition]) ->
    {ok, #state{partition=Partition, data=dict:new()}}.

handle_command({put, Key, Value}, _Sender, State) ->
    NewData = dict:store(Key, Value, State#state.data),
    {reply, ok, State#state{data=NewData}};

handle_command({get, Key}, _Sender, State) ->
    Result = dict:find(Key, State#state.data),
    {reply, Result, State};

handle_command({delete, Key}, _Sender, State) ->
    NewData = dict:erase(Key, State#state.data),
    {reply, ok, State#state{data=NewData}}.

is_empty(State) ->
    {dict:size(State#state.data) == 0, State}.

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State#state{data=dict:new()}}.

handle_handoff_data(Bin, State) ->
    {Key, Value} = binary_to_term(Bin),
    NewData = dict:store(Key, Value, State#state.data),
    {reply, ok, State#state{data=NewData}}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

handoff_starting(_Target, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_Target, State) ->
    {ok, State}.
```

### 使用自定义 VNode

```erlang
%% 写入数据
write(Key, Value) ->
    %% 计算 key 所属分区
    Index = chash:key_of(Key),
    %% 发送命令
    riak_core_vnode_master:sync_command(Index, {put, Key, Value}, my_kv_vnode).

%% 读取数据
read(Key) ->
    Index = chash:key_of(Key),
    riak_core_vnode_master:sync_command(Index, {get, Key}, my_kv_vnode).

%% 删除数据
delete(Key) ->
    Index = chash:key_of(Key),
    riak_core_vnode_master:sync_command(Index, {delete, Key}, my_kv_vnode).

%% 列出所有 keys (覆盖查询)
list_keys() ->
    riak_core_vnode_master:coverage_command({list_keys}, my_kv_vnode, 3, 5000).
```

---

## 📊 VNode 架构优势

### 1. 隔离性

```
每个 VNode 是独立的状态机：
- 一个 VNode 崩溃不影响其他
- 数据天然分区，无锁竞争
- 独立的 Handoff 和修复
```

### 2. 可扩展性

```
Worker Pool 提供并行处理能力：
- 根据负载调整 Pool 大小
- 慢请求不阻塞其他请求
- 充分利用多核 CPU
```

### 3. 灵活性

```
覆盖查询支持多种场景：
- 全量扫描
- 范围查询
- 二级索引查询
```

---

## 🔬 深度分析：为什么 VNode 是核心抽象？

### 设计理念

```
VNode = Virtual Node (虚拟节点)

理念：
1. 物理节点 → 多个 VNode
2. 每个 VNode → 一个分区
3. VNode 是数据和状态的最小单位

好处：
1. 数据迁移单位 = VNode
2. 故障恢复单位 = VNode
3. 负载均衡单位 = VNode
```

### 与其他系统对比

| 系统 | 分区单位 | 迁移单位 |
|------|----------|----------|
| HBase | Region | Region |
| Cassandra | Token Range | Token Range |
| Riak | VNode | VNode |
| MongoDB | Shard | Chunk |

Riak 的独特之处：
- VNode 是进程级别的隔离
- 每个物理节点可以有多个 VNode
- VNode 可以在不同节点间迁移

---

## 📚 与 DDIA 的对照

| DDIA 概念 | Riak Core 实现 | 代码位置 |
|-----------|----------------|----------|
| 分区 | VNode | `riak_core_vnode.erl` |
| 请求路由 | VNode Master | `riak_core_vnode_master.erl` |
| 并行处理 | Worker Pool | `riak_core_worker_pool.erl` |
| 覆盖查询 | Coverage FSM | `riak_core_coverage_fsm.erl` |
| 状态机 | gen_fsm | `riak_core_vnode.erl` |

---

## 🔗 相关文件索引

| 文件 | 功能 | 关键函数 |
|------|------|----------|
| [riak_core_vnode.erl](../src/riak_core_vnode.erl) | VNode 行为 | `init/1`, `handle_command/3` |
| [riak_core_vnode_master.erl](../src/riak_core_vnode_master.erl) | 请求路由 | `command/4`, `get_vnode/2` |
| [riak_core_coverage_fsm.erl](../src/riak_core_coverage_fsm.erl) | 覆盖查询 | `init/1`, `waiting_results/2` |
| [riak_core_worker_pool.erl](../src/riak_core_worker_pool.erl) | Worker Pool | `run/3`, `handle_cast/2` |
| [riak_core_vnode_sup.erl](../src/riak_core_vnode_sup.erl) | VNode 监督 | `start_vnode/2` |

---

## 🎓 学习建议

### 理解顺序

1. **理解 VNode 行为**: 它是分区的抽象
2. **理解命令处理**: 同步 vs 异步
3. **理解覆盖查询**: 如何访问所有分区
4. **理解 Worker Pool**: 如何并行处理

### 推荐实验

```bash
# 运行 VNode 测试
rebar3 eunit --module=riak_core_vnode

# 运行 Coverage 测试
rebar3 eunit --module=riak_core_coverage_fsm

# 运行 Worker Pool 测试
rebar3 eunit --module=riak_core_worker_pool
```

### 实践建议

1. 实现一个简单的 VNode
2. 测试 Handoff 功能
3. 实现一个覆盖查询
4. 调整 Worker Pool 大小，观察性能变化
