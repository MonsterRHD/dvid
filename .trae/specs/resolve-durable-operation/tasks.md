# Resolve 冲突消解的持久化原子操作 - 实施计划

## Task 1: resolveOpT 操作记录类型与 gob 持久化骨架
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在新建的 `datastore/resolve.go` 中定义 `resolveOpT`（id、有序 parents、data 实例名、note、created/updated、phase、extensions、extCommitted、instDone、child、冻结指纹 parentChildren/instID/instDataUUID、errMsg），实现手动 `GobEncode/GobDecode` 以便字段保持非导出风格。
  - 定义阶段常量（phaseDeleting/Committing/Merging/Done/Stale）与操作键哈希函数：repo 根 + 有序父版本 + 规范化实例名集合（crypto/sha256 或 FNV），同输入稳定同键。
  - 在 `repoT` 增加 `resolveOps map[string]*resolveOpT`；`newRepo` 初始化；在 `GobEncode/GobDecode` 末尾（passcode 之后）追加该字段，解码缺字段时容错为空；decode 后 nil map 安全初始化。
  - 操作记录不进入 `MarshalJSON`（full 与 visibility 两种均不暴露）。
- **Acceptance Criteria Addressed**: AC-1, AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-1.1: gob 编码→解码后操作记录内容逐字段一致；旧格式 gob（无 resolveOps 字段）可正常解码且 resolveOps 为空 map；证据：datastore 包新增测试。
  - `rule` TR-1.2: 相同（根, 有序父版本, 实例集合）两次计算得到相同操作键；父版本顺序不同或实例集合不同得到不同键；证据：单元测试。
  - `rule` TR-1.3: repo 的两种 JSON 序列化输出均不含 resolve 操作字段；证据：测试断言。
- **Notes**: 参考 passcode 尾部可选字段的容错先例。
- **Completion Evidence**:
  - TR-1.1/1.2/1.3 通过：`TestResolveOpGobRoundTrip`、`TestResolveOpsOldRepoGob`、`TestResolveOpKey`、`TestResolveOpsNotInJSON` 全绿（go test -tags 'badger filestore ngprecomputed'）。
  - 实现：[resolve.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go)；repo gob 尾部追加见 [repo_local.go GobDecode/GobEncode](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2709-L2756)。

## Task 2: 删除阶段重构——计划驱动、扩展唯一、错误上抛
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 `datastore/resolve.go` 实现 `Resolve(urlUUID dvid.UUID, names []dvid.InstanceName, parents []string, note string) (dvid.UUID, error)` 入口与删除阶段：解析 URL repo/父版本/实例（≥2 唯一且同 repo、已锁定；≥1 唯一实例），加锁查找/创建操作记录（创建时先拍指纹：每父版本锁定状态与子节点集合、每实例 InstanceID/DataUUID），计划持久化后再开始删除。
  - 扩展节点助手：每父版本至多一个，UUID 预生成并写入 op 后以 assign 方式经 `newVersion` 创建（单次 r.save 同时落盘计划与节点），分支名含 op 短标识（`resolve-{opID短}-{序号}`）；已登记但 DAG 缺失则用同 UUID 补建，已存在则复用；无冲突父版本不建扩展。
  - 将 `DeleteConflicts` 重构为操作驱动变体：goroutine 内 FindConflicts/删除错误通过共享错误收集上抛，任一键失败即整体返回错误（保留同批次后续删除尝试），不再 log-and-continue 报成功；instDone 记录实例完成态，成功一个持久化一个，重发跳过已完成实例。
  - 定义测试故障注入变量（nil 默认，非测试文件中明确命名为 test hook），支持在扫描/删除点失败。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-6
- **Test Requirements**:
  - `rule` TR-2.1: 正常多实例 resolve 后，被删键集合与优先级结果与一次无故障参考运行一致；证据：聚焦测试。
  - `rule` TR-2.2: 扫描与单键删除分别注入故障时返回错误、无 2xx，已建扩展全部未锁定、DAG 无最终 merge 节点；证据：测试断言节点 locked/parents。
  - `rule` TR-2.3: 故障清除后相同请求重发完成，每父版本至多一个扩展节点，删除键集合无多删/漏删；证据：前后节点计数与键集合 diff。
  - `rule` TR-2.4: 计划记录的首次持久化先于任何扩展节点与 tombstone 落盘；证据：代码检查与计划创建点断言。
- **Notes**: 既有 `DeleteConflicts` 包级函数若无其他调用方则改为内部变体。
- **Completion Evidence**:
  - TR-2.1/2.2/2.3/2.4 通过：`TestResolveDeletionHappyPath`、`TestResolveDeletionScanFailure`（含 CloseReopenTest 重启续跑）、`TestResolveDeletionDeleteFailure` 全绿。
  - 旧 extensionNode/deleteConflict/DeleteConflicts 已从 [datastore.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/datastore.go) 移除；新删除逻辑在 [resolve.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go)，扩展节点 assigned 创建在 [newVersionAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2006-L2152)。

## Task 3: 提交与最终 merge 阶段——幂等 assigned merge
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 状态机驱动器按 deleting→committing→merging→done 推进，每次阶段切换先持久化。
  - 提交阶段：对每个已用扩展，已锁定且归属本操作则认领，否则调用 commit；记录 extCommitted 并逐节点持久化；提交触发的 SyncOnCommit 等既有异步副作用保持不变。
  - 在 `repo_local.go` 新增内部 assigned merge：child UUID 预生成并先持久化于 op.child；登记版本（若缓存已含该 UUID 则复用其 VersionID）；DAG 节点已存在且父集合（扩展优先、无扩展用原父版本）匹配则认领返回；DAG 节点缺失则直接构建节点并连线、InitVersion、r.save，禁止产生第二个 merge 节点。
  - merge 成功后持久化 done 态。
  - 故障注入点覆盖 commit 与 merge。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-8
- **Test Requirements**:
  - `rule` TR-3.1: commit 阶段注入故障后重发完成，每个扩展仅被提交一次（重发认领不报错），证据：节点 locked 状态与提交计数。
  - `rule` TR-3.2: merge 各崩溃窗口（child 已持久化未登记、缓存已登记无 DAG 节点、DAG 节点已落盘未标 done）经重新加载元数据后续跑均得到唯一同一 child；证据：逐窗口测试。
  - `rule` TR-3.3: 全流程只存在一个与本操作对应的 merge 节点，child 中冲突键读取符合优先级；证据：DAG 断言与读取测试。
  - `rubric` TR-3.4: 崩溃窗口矩阵完整性；scale 1-5；anchors 1=存在未覆盖窗口, 3=窗口覆盖但恢复路径重复分散, 5=矩阵全覆盖且恢复逻辑单一入口；threshold >= 4；证据：矩阵清单与测试输出。
- **Notes**: 不改动包级 `Merge` 与 `/merge` 端点。
- **Completion Evidence**:
  - TR-3.1 通过：`TestResolveCommitFailure`（direct/restart）。
  - TR-3.2/3.4 通过：`TestResolveMergeCrashWindows` 覆盖 3 个 merge 崩溃窗口 × direct/restart；崩溃窗口矩阵 = 删除(tombstone 写入失败/扫描失败) × 提交失败 × merge(reserved/cached/built)，全部唯一 child、无重复节点。
  - TR-3.3 通过：merge 父集合断言与 assertChildReads 优先级读取全绿。实现：[mergeAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2257-L2387)。

## Task 4: 陈旧计划检测与并发互斥
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 每次执行续跑前校验指纹：父版本仍存在且已锁定，当前子节点集合 = 冻结集合 + op 已登记扩展；实例名解析仍存在且 InstanceID、DataUUID 与冻结一致；不一致则 op 转 stale 终态、持久化并返回明确冲突错误。
  - 进程内单飞控制（repo 级，非持久）：相同操作键的并发调用排队复用首个结果；同一 repo 存在其他活动操作时，不同计划返回冲突错误。
  - 完成态操作被重发时校验 child 仍存在且父集合匹配，随后直接返回 child。
- **Acceptance Criteria Addressed**: AC-4, AC-5, AC-9
- **Test Requirements**:
  - `rule` TR-4.1: 删除阶段中给某父版本新增其他已提交子节点，或改名/删除目标实例后重发，得到 409 且 op 持久化为 stale，重复重发持续 409；证据：测试。
  - `rule` TR-4.2: 全新（不受变化影响的）计划在 stale 后可独立提交并成功完成；证据：测试。
  - `rule` TR-4.3: 相同操作并发请求均返回同一 child 且无新增节点；不同计划并发请求之一得到 409；证据：并发测试（配合 -race）。
  - `rule` TR-4.4: 完成态重发返回同一 child；若 child 已被外部删除（直接改状态）则返回冲突而非伪造新节点；证据：测试。
- **Completion Evidence**:
  - TR-4.1/4.2 通过：`TestResolveStaleForeignChild`、`TestResolveStaleInstanceChanged`（rename/delete），stale 持久化且全新反向计划成功。
  - TR-4.3 通过：`TestResolveConcurrentIdentical`（4 并发同 child）、`TestResolveConcurrentDifferent`（409）。
  - TR-4.4 通过：`TestResolveDoneChildMissing`。

## Task 5: HTTP handler 接线与 API 文档更新
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 4
- **Description**:
  - 重写 [repoResolveHandler](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2400-L2483)：保留请求体 JSON 格式与校验（data/parents 必填），改调 `datastore.Resolve`；按错误类型映射 400（格式/参数）、409（stale/繁忙）、5xx（可重试阶段失败），成功返回 `{"child":"..."}`。
  - 更新 web.go 中 resolve 端点帮助文本：说明计划冻结、失败重发续跑、重复提交幂等、陈旧计划 409；修正“spawns an asynchronous routine”描述（实际同步执行到完成或返回错误）。
  - 鉴权链与只读模式行为不变。
- **Acceptance Criteria Addressed**: AC-6, AC-7
- **Test Requirements**:
  - `rule` TR-5.1: 成功路径经 HTTP 返回可解析的 child，且 child 上优先级读取正确；既有 keyvalue 包 resolve 测试（HTTP 端到端）不改或仅做兼容性微调后通过；证据：测试输出。
  - `rule` TR-5.2: 故障路径 HTTP 状态码符合 400/409/5xx 约定且响应体含错误说明；证据：server/datastore 聚焦测试。
- **Completion Evidence**:
  - TR-5.1 通过：`TestKeyvalueVersioning`（含 HTTP POST resolve 端到端）与 `TestDiamondGetOnMerge` 全绿，未改测试。
  - TR-5.2 通过：错误分类经 datastore 各聚焦测试覆盖（badinput 包装 ErrResolveBadInput）。实现：[repoResolveHandler](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2401-L2447)。

## Task 6: 端到端测试、竞态检测与全量验证
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 5
- **Description**:
  - 增补端到端场景：多实例 resolve 中实例 1 成功、实例 2 失败 → 重启模拟 → 重发完成；断言未选中实例无任何键变化、child 可继续 NewVersion/Commit。
  - 运行 `go test -race` 覆盖 datastore 与 datatype/keyvalue 包；运行受影响包完整测试；视耗时运行 `make test`。
  - 整理崩溃窗口矩阵作为 AC-8 证据写入完成记录。
- **Acceptance Criteria Addressed**: AC-3, AC-6, AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-6.1: 端到端中断/重启/续跑场景全绿，未选中实例键集合与 mtime 不变；证据：测试输出。
  - `rule` TR-6.2: `go build ./...` 通过；`go test -race ./datastore/... ./datatype/keyvalue/...` 通过（环境不支持则记录并降级说明）；证据：命令输出。
  - `rule` TR-6.3: 受影响包全部既有测试通过；若未运行 `make test` 全量验证，在最终回复中明确说明；证据：测试输出与说明。
- **Completion Evidence**:
  - TR-6.1 通过：`TestResolveEndToEndRestart`（部分失败 → CloseReopenTest → 完成；未选中实例快照一致；child commit/branch 成功；优先级读取正确）。
  - TR-6.2 通过：`go test -race` datastore + keyvalue 全绿；标准标签构建通过。
  - TR-6.3 通过：完整套件（make test 同包集合：./dvid/... ./storage/... ./datastore ./server ./datatype/... ./tests_integration）全部 ok，exit 0；其中 labelmap 28s、server 4.9s、datastore 2.2s。

## 崩溃窗口矩阵（AC-8 证据）

| 阶段 | 崩溃点 | 持久状态 | 恢复行为 |
| --- | --- | --- | --- |
| 删除 | 扫描中/扫描后 | phase=deleting, instDone 部分 | 重扫未完成实例，已写 tombstone 幂等重写 |
| 删除 | tombstone 写入失败 | 扩展节点未锁定 | 同请求重试，扩展复用 |
| 提交 | commit 失败 | 扩展未锁定, extCommitted=false | 重试 commit，已锁定则认领 |
| 合并 | child 已记录未登记缓存 | phase=merging, op.child 已持久化 | mergeAssigned 登记并构建 |
| 合并 | 缓存已登记 DAG 缺节点 | uuid→V 缓存已持久化 | 复用 V 构建节点 |
| 合并 | DAG 节点已落盘未标 done | merge 节点存在 | 父集合匹配则认领 → done |
| 任意 | 外部状态变化 | verifyFrozen 不通过 | 转 stale 终态，409 |

## Issue I-1: resolve 状态与并发 repo gob 编码间数据竞争（Review R1 F-1）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R1
- **Description**:
  - resolve 执行期间同 repo 上其他变更（NewVersion/Commit → r.save）的 gob 编码与 resolveOps map/op 字段写无同步。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-I-1.1: resolve 暂停期间对其父版本做并发建分支+提交（触发 r.save），`go test -race` 无竞争；操作随后按预期 stale；证据：`TestResolveConcurrentSaveRace`。
  - `rule` TR-I-1.2: 全部既有 TestResolve* 与 server/keyvalue 套件全绿；全量套件 exit 0。
- **Completion Evidence**:
  - 统一锁协议：resolveMu 改为 RWMutex，repo GobEncode 持 resolveMu.RLock + 每个 op.mu.RLock（锁序 resolveMu→op.mu）；op 全部可变字段写经 `mutate`/setPhase/reserveChild；入口不在 resolveMu 内读 op 字段、不在 resolveMu 内 r.save。
  - 回归：`TestResolveConcurrentSaveRace` -race 通过；datastore+keyvalue -race 通过；全量套件全绿。
  - 顺带补齐 F-2 advisory：`TestResolveDeleteFailure` 增加 restart 子测试。
  - F-3（putNewIDs 预存在无锁读）按评审意见属独立预存在问题，不在本次范围。

## Issue I-2: busy 检查 TOCTOU 与 dag.nodes 锁约定不一致（Review R2 F-4/F-5）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: I-1
- **Discovered By**: Review R2
- **Description**:
  - F-4：I-1 将入口拆成两段临界区，busy 检查在 resolveMu 锁外，不同新计划可同时通过 busy 检查并双双成为 runner。
  - F-5：dag.nodes 写入锁约定不统一（标准 merge/mergeAssigned 仅持 r.Lock，而 resolve 侧读仅持 dag.RLock），-race 实测 map 竞争。
- **Acceptance Criteria Addressed**: AC-8, AC-9
- **Test Requirements**:
  - `rule` TR-I-2.1: 10 个不同计划并发提交，恰 1 个 runner、其余 ErrResolveBusy，map 中恰 1 个批操作；证据：`TestResolveDistinctPlansConcurrent`（-race 通过）。
  - `rule` TR-I-2.2: resolve 暂停期间并发标准 Merge（写 dag.nodes），-race 无竞争，resolve 恢复后 stale；证据：`TestResolveConcurrentMergeRace`（-race 通过）。
- **Completion Evidence**:
  - F-4：候选 op 锁外预建（不可见），busy 检查 + map 插入 + running 登记合并为单个 resolveMu 临界区。
  - F-5：四处 dag.nodes 写入统一为 r.Lock+r.dag.Lock，读侧 r.RLock 或 dag.RLock 均与写互斥。
  - 顺带修复 F-6（advisory 预存在）：移除 repo GobEncode 中无意义的嵌套 r.RLock/RUnlock。
  - F-3 仍为预存在 advisory，建议另立 Issue。

## Issue I-3: hideBranch dag.nodes 删除锁协议（Review R3 F-7）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: I-2
- **Discovered By**: Review R3
- **Description**:
  - hideBranch 删除 dag.nodes 仅持 r.Lock，id 缓存删除不持 idMutex，与 resolve 的 dag.RLock-only 读路径不互斥；deleteRepo range 同类。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-I-3.1: HideBranch 与 8 个并发重复 resolve（verifyFrozen 读 dag）并存时 -race 无竞争；隐藏后计划不再报成功；证据：`TestResolveHideBranchRace`（-race 通过）。
- **Completion Evidence**:
  - hideBranch 改为持 repoMutex+r.Lock+dag.Lock，id 缓存删除在 idMutex 内。
  - deleteRepo 改为先在 dag.RLock 下快照版本列表，再在 idMutex 下处理。
  - F-3 族（putNewIDs 等无锁读）仍为预存在 advisory。

## Issue I-4: repoMutex↔idMutex 锁序倒置与 children 无锁写（Review R4 F-8/F-9）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: I-3
- **Discovered By**: Review R4
- **Description**:
  - F-8：I-3 的 hideBranch 嵌套锁序为 repoMutex→idMutex，与 deleteRepo/repos 列表的 idMutex→repoMutex 相反，ABBA 死锁。
  - F-9：hideBranch 重写 node.children 不持 node.Lock。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-I-4.1: HideBranch × DeleteRepo × repos 列表并发不死锁（20s 看门狗）；证据：`TestResolveHideDeleteDeadlock`。
  - `rule` TR-I-4.2: HideBranch × 重复 resolve 并发 -race 干净；证据：`TestResolveHideBranchRace`（race 套件通过）。
- **Completion Evidence**:
  - hideBranch 重构为分阶段：id/dag 快照只读 → idMutex→repoMutex 全局删除（统一全局锁序）→ r.Lock→dag.Lock→node.Lock 本地修改（F-9 补 node.Lock）。
  - F-3 族仍为预存在 advisory。
