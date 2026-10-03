# Resolve 冲突消解的持久化原子操作 - 产品需求文档

## Overview
- **Summary**: 将 `POST /api/repo/{uuid}/resolve` 的一次冲突消解（按优先级顺序扫描多个父版本、在多个数据实例上删除冲突、提交扩展版本、执行最终 merge）冻结为一个带稳定标识、可持久化恢复的单一操作记录。操作分阶段推进（删除 → 提交 → 合并 → 完成），任一步骤失败或进程重启后，客户端重发相同请求即可从断点续跑；全部成功才返回最终合并版本。
- **Purpose**: 现有实现中，先处理的实例可能已创建未提交的扩展版本并写入删除标记，后续实例的扫描/删除或最终 commit、merge 失败后，重试会叠加新的临时子节点（或因 `conflict-{uuid}` 分支名冲突直接失败），进程退出会留下与最终 merge 无法对应的半成品，且 `DeleteConflicts` 内部 goroutine 吞掉单键删除错误仍向上层报告成功。需要保证删除结果与操作一一对应、不重复删除、不重复建节点、不把部分结果当成功。
- **Target Users**: 通过 REST API 对多分支 DVID repo 做冲突消解的研究人员与自动化脚本。

## Goals
- 一次 resolve 的父版本顺序、目标数据实例集合、冲突处理结果冻结为同一持久操作。
- 各数据实例可独立重试；所有删除结果必须归属该操作；全部实例删除完成前不得提交任何扩展版本、不得生成最终 merge 节点。
- 扫描、删除、提交、最终 merge 任一失败后，重发请求或进程重启后重发请求均从未完成阶段继续，且不重复删除、不重复创建子节点。
- 重复提交相同操作复用既有进度与最终 child；等待期间父版本、目标实例或相关 repo 状态变化时拒绝陈旧计划。
- 只有全部冲突处理与元数据持久化成功才返回一个可正常继续使用（可分支、可提交、读取按优先级消解）的合并版本。

## Non-Goals
- 不改变普通 `POST /api/repo/{uuid}/merge`（conflict-free）的行为。
- 不扫描、不触碰未在 `data` 字段中选中的数据实例。
- 不改变既有只读查询行为，包括 merge 后读取时的惰性冲突报错。
- 不新增操作状态轮询、取消或回滚 API。
- 不在服务器启动时自动后台执行删除/合并（仅持久化进度，由客户端重发触发续跑，已与用户确认）。
- 不提供跨多个元数据键/存储后端的分布式事务。

## Background & Context
- 处理器：[repoResolveHandler](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2400-L2483)，当前流程：解析 parents → 对每个实例调用 `DeleteConflicts` → 提交所有扩展节点 → `Merge`。
- 冲突删除：[DeleteConflicts](file:///Users/ding/Documents/swe/09271/project-06/datastore/datastore.go#L681-L779) 通过 `RawRangeQuery(keysOnly=true)` 扫描实例全量键，按 TKey 分组后用 [FindConflicts](file:///Users/ding/Documents/swe/09271/project-06/datastore/datastore.go#L608-L635) 按父版本优先级找出待删键；扩展节点在 [deleteConflict](file:///Users/ding/Documents/swe/09271/project-06/datastore/datastore.go#L644-L677) 中惰性创建，分支名为 `conflict-{oldUUID}`。
- 缺陷证据：goroutine 内 `FindConflicts`/`deleteConflict` 出错仅 `dvid.Errorf` 后 continue，外层仍返回 nil；扩展节点创建后经 `r.save()` 已持久化为未提交节点；commit/merge 失败后无任何恢复路径；重发时分支名唯一检查（[newVersion](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1897-L1996)）报错或叠加新子节点。
- 元数据持久化：每个 repo 以 gob 整体序列化到元数据存储单键（[saveToStore](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2869-L2887)），DAG 节点与 repo 记录在同一次 `r.save()` 落盘；gob 已有可选尾部字段先例（passcode 的容错解码，[GobDecode](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2667-L2706)）。
- 存储删除在各后端天然幂等（badger 写 tombstone 键，重删仅重写 tombstone）；扩展节点是父版本的子节点，不会被父版本视角的 `findMatch` 祖先遍历触及，因此重扫对冲突判定无副作用。

## Functional Requirements
- **FR-1 计划冻结与标识**: resolve 请求校验通过后、任何扩展节点/删除产生前，创建并持久化操作记录，内容包括：repo 根、有序父版本 UUID 全字、目标实例名集合、note、创建时间、每父版本的冻结子节点集合、每实例的身份（InstanceID、DataUUID）、各阶段状态。操作标识由 repo 根 + 有序父版本 + 规范化实例名集合的哈希确定；note 以首次提交为准，相同标识下重发的不同 note 不改变操作。
- **FR-2 分阶段门控**: 操作严格按 ① 删除（逐实例扫描+写 tombstone）→ ② 提交（逐个锁定扩展节点）→ ③ 合并（生成最终 child）→ ④ 完成 推进。任一实例未完成删除时，不得提交任何扩展版本，不得创建最终 merge 节点；阶段切换必须先持久化。
- **FR-3 实例级进度与错误上抛**: 每个实例独立记录 待处理/已完成；扫描、键版本解析、tombstone 写入的任何错误必须使该实例保持未完成并作为操作错误返回（替换当前 log-and-continue 吞错行为），HTTP 响应为 5xx；同请求重发只重做未完成实例，已完成实例跳过。
- **FR-4 扩展节点唯一且可恢复**: 每个父版本至多一个扩展节点，跨所有实例共享；扩展 UUID 预生成并先记入计划，创建使用 assign 方式，分支名含操作标识以避免与其他操作冲突。若记录中扩展已登记但 DAG 节点缺失（崩溃于登记与落盘之间），用同一 UUID 补建；节点已存在则复用。无冲突的父版本不创建扩展。
- **FR-5 提交与合并幂等**: 提交阶段发现扩展节点已锁定时，确认归属本操作后直接视为已提交。最终 child UUID 在 merge 前预生成并持久化；merge 内部支持 assigned UUID：缓存已登记但 DAG 节点缺失时用既有 VersionID 直接构建节点，DAG 节点已存在且父集合匹配时直接认领，绝不产生第二个 merge 节点。
- **FR-6 重发/重启续跑**: 客户端用完全相同请求体重发时，按 FR-2 阶段状态从断点继续；进程重启（元数据重新加载）后重发同样可继续。续跑不产生重复 tombstone 语义（重写 tombstone 合法）、不产生额外扩展或 merge 节点、不报告成功除非全部阶段完成。
- **FR-7 重复提交复用**: 操作完成后相同标识的提交直接返回既有 child，不创建任何节点；同一操作正在执行时的并发相同提交在进程内排队，首个执行完成后复用同一结果。
- **FR-8 陈旧计划检测与拒绝**: 每次续跑前校验：各父版本仍存在且已锁定、其当前子节点集合 = 冻结集合 + 本操作已登记的扩展；各目标实例仍存在且 InstanceID、DataUUID 与冻结一致。任一不满足（如父版本新增其他子节点、实例被改名/删除）时，操作转为不可恢复的 stale 终态并持久化，本次及此后相同提交返回 409；用户须提交全新计划。
- **FR-9 并发互斥**: 同一 repo 同时只允许一个处于活动状态（删除/提交/合并阶段）的 resolve 操作；对不同计划的并发提交返回 409。
- **FR-10 HTTP 语义**: 成功 200 返回 `{"child":"..."}`；请求格式/参数错误返回 400；陈旧计划与冲突提交返回 409；扫描/删除/提交/merge 的可重试失败返回 5xx。请求体与响应字段格式与现状兼容。
- **FR-11 非回归**: conflict-free `/merge`、未选中实例、只读查询（含惰性冲突报错）行为不变；操作记录不出现在现有 JSON 元数据 API 中；旧 repo gob（无操作记录字段）可正常加载。

## Non-Functional Requirements
- **NFR-1 崩溃窗口完备性**: 每两次元数据写入之间的崩溃窗口都必须可恢复；实现需列出崩溃窗口矩阵并用逐阶段中断测试覆盖。
- **NFR-2 并发安全**: 同标识并发与不同计划并发下无数据竞争，`go test -race` 对 datastore 与相关 datatype 包通过。
- **NFR-3 向后兼容**: repo gob 仅在末尾追加字段并容错解码；不提升 repo 格式版本号；不影响 full/visibility 两种 JSON 序列化。
- **NFR-4 性能不退化**: 无失败的正常 resolve 与现状相比不增加额外全量扫描次数；续跑未完成实例的重扫为可接受的正确性代价。

## Constraints
- **Technical**: Go + gob 元数据；存储后端多样（badger/filestore/ngprecomputed 等），删除必须仅依赖后端已有的幂等 Delete；无跨键事务；单键 gob 序列化要求新增字段只能尾部追加并容错。
- **Business**: 不得改变既有只读查询与 conflict-free merge 的对外可观察行为。
- **Dependencies**: `datastore` 包元数据管理器、`server/web.go` 路由与错误响应、`storage` 后端接口。

## Assumptions
- 已锁定（已提交）父版本的数据不可变，等待期间唯一可能的外部变化是：父版本新增其他子节点、目标实例被改名/删除/重建、repo 其他分支活动。
- 进程重启后不自动续跑；客户端会按 API 约定重发相同请求（用户已确认）。
- 已完成的操作记录随 repo 长期保留（记录体很小），以支持重复提交返回同一 child。

## Acceptance Criteria

### AC-1: 首次提交即持久化冻结计划
- **Type**: `rule`
- **Given**: 一个含 ≥2 个已提交父版本、≥1 个数据实例的 repo。
- **When**: 客户端首次 POST resolve。
- **Then**: 在任何扩展节点创建或 tombstone 写入前，元数据中存在含有序父版本、实例集合、note、父版本冻结子集合、实例身份（InstanceID/DataUUID）的操作记录。
- **Pass Condition**: 代码检查确认计划先于变更落盘；测试在计划创建后、首次删除前可观察到持久记录，且重新加载元数据后记录仍在。
- **Evidence**: datastore 包新增聚焦测试；实现代码引用。

### AC-2: 删除未全部完成前不得提交或合并
- **Type**: `rule`
- **Given**: 多个目标实例的 resolve，在第 K 个实例处理期间注入失败。
- **When**: 操作返回错误。
- **Then**: 所有已创建扩展节点均为未锁定状态，DAG 中不存在以这些（父版本或扩展）为 parent 的最终 merge 节点；响应非 2xx。
- **Pass Condition**: 逐实例扫描 DAG 节点锁状态与父关系断言通过；HTTP 状态为 5xx。
- **Evidence**: 带故障注入钩子的聚焦测试。

### AC-3: 各阶段失败后重发与重启均从断点续跑且不重复副作用
- **Type**: `rule`
- **Given**: 分别在扫描、单键删除、扩展提交、最终 merge 阶段注入一次故障。
- **When**: 清除故障后用相同请求体重发；并对每种故障增加“故障后重新加载元数据（模拟重启）再重发”路径。
- **Then**: 操作最终 200 返回 child；每个父版本至多一个 resolve 扩展节点、全 repo 只有一个与本操作对应的 merge 节点；最终被删键集合与一次无故障参考运行完全一致（无多删、无漏删）；child 中读取各冲突键符合父版本优先级。
- **Pass Condition**: 节点计数、删除键集合 diff、child 读取断言全部通过。
- **Evidence**: 四阶段 ×（直接重发/模拟重启）矩阵测试。

### AC-4: 重复提交复用既有进度与最终 child
- **Type**: `rule`
- **Given**: 同一操作（a）进行中、（b）已完成。
- **When**: 提交相同标识请求；进行中场景另起并发请求同时提交。
- **Then**: 完成后所有提交返回同一 child UUID；无任何新增扩展/merge 节点；并发提交无一返回冲突错误。
- **Pass Condition**: 提交前后节点 UUID 集合不变；并发请求返回相同 child。
- **Evidence**: 聚焦测试（含并发）。

### AC-5: 等待期间状态变化时拒绝陈旧计划
- **Type**: `rule`
- **Given**: 一个处于删除阶段的持久化操作。
- **When**: 分别制造（a）某父版本新增一个不属于本操作的已提交子节点、（b）目标实例被改名或删除，然后重发相同请求。
- **Then**: 返回 409，操作转为 stale 终态并持久化；此后相同请求持续 409；不受此变化影响的全新计划可作为独立操作正常提交并完成。
- **Pass Condition**: 状态码、终态持久化、新计划成功三项断言通过。
- **Evidence**: 聚焦测试。

### AC-6: 全部成功才返回可继续使用的合并版本
- **Type**: `rule`
- **Given**: 完整 resolve 成功。
- **When**: 检查返回 child。
- **Then**: 仅在全部删除、提交、merge 与元数据落盘后返回 200；child 是普通可用版本：可在其上 NewVersion/Commit，冲突键按优先级读取正确，未选中实例在 child 上行为与普通 conflict-free merge 后一致。
- **Pass Condition**: 上述分支、提交、读取断言通过；失败路径中不存在被返回的 child。
- **Evidence**: 聚焦测试 + 既有 keyvalue resolve 测试保持通过。

### AC-7: 既有行为非回归
- **Type**: `rule`
- **Given**: 现有 conflict-free `/merge`、未选中实例、只读查询（含 merge 后惰性冲突报错）、旧格式 repo 元数据。
- **When**: 运行既有测试套件并手动核对旧 gob 加载。
- **Then**: 全部既有行为与输出不变；旧 gob 正常加载且等价为空操作记录；`/api/repos` 等 JSON 不含操作记录字段。
- **Pass Condition**: 受影响包既有测试全部通过，旧 gob 加载测试通过。
- **Evidence**: `go test ./datastore/... ./datatype/keyvalue/... ./server/...`（必要时 `make test`）输出。

### AC-8: 崩溃恢复与持久化设计质量
- **Type**: `rubric`
- **Dimension**: 崩溃窗口识别完整性、状态机清晰度、gob 兼容性处理、锁使用正确性。
- **Scale**: 1-5
- **Anchors**: 1 = 存在未覆盖的崩溃窗口或全局锁滥用；3 = 主要窗口覆盖但恢复逻辑分散重复；5 = 崩溃窗口矩阵全覆盖，状态机单一入口、幂等路径清晰，gob 兼容与锁约定明确。
- **Pass Threshold**: >= 4
- **Evidence**: 代码评审、崩溃窗口矩阵文档（tasks 证据）、`go test -race` 输出。
