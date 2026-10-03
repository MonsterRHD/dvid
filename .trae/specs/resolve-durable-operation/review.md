# Resolve 冲突消解的持久化原子操作 - Independent Review

## Checkpoints

- [x] CP-1: 首次提交时操作记录先于任何扩展节点/删除落盘，且含全部冻结内容
  - **Type**: `rule`
  - **Covers**: AC-1, FR-1
  - **Evidence**:
    - 代码：[Resolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L296-L306) 中 `newResolveOp` → `snapshotFingerprint`（每父版本锁定状态与 children 集合、每实例 InstanceID/DataUUID，[resolve.go#L365-L398](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L365-L398)）→ `r.resolveOps[key] = op` → `r.save()` 全部完成后才进入 `runResolve`（[resolve.go#L326](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L326)）。扩展节点仅在删除 goroutine 内经 [resolveExtension](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L492-L506) 惰性创建，tombstone 更在其后。
    - 测试：`TestResolveScanFailure` 首次运行失败后记录存在，经 `CloseReopenTest()` 重新加载后记录仍在（评审实测 PASS，见 Review History）。
    - 稳定标识：[resolveOpKey](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L152-L164) = root + 有序 parents + 排序实例名集合；`TestResolveOpKey` 验证顺序/集合敏感性；note 仅在 newResolveOp 创建时写入，重发不改。
    - **R3 复验**：代码路径不变（[resolve.go#L315-L362](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L315-L362)）；`TestResolveOpGobRoundTrip`、`TestResolveOpsOldRepoGob`、`TestResolveOpKey` 评审亲自 -race 实跑 PASS。
    - **R4 复验**：I-3 未触及入口持久化路径，顺序仍为 newResolveOp→snapshotFingerprint→map 插入（resolveMu）→`r.save()`（[resolve.go#L315-L362](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L315-L362)）；评审亲自 `-race -v -run TestResolve` 实跑，`TestResolveOpGobRoundTrip`、`TestResolveOpKey` PASS（见 Review History R4）。

- [x] CP-2: 删除未全部完成前不可能 commit/merge；失败（含 goroutine 内单键错误）一律上抛、非 2xx
  - **Type**: `rule`
  - **Covers**: AC-2, FR-2, FR-3
  - **Evidence**:
    - 状态机单一入口 [runResolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L510-L636)：实例循环全部 `instDone` 后才 `setPhase(phaseCommitting)` 并 `r.save()`（L549-L553）；全部扩展 commit 后才进入 merging（L597-L601）；merge 成功后才 done（L628-L632）。每次阶段切换先持久化。
    - 删除 goroutine 内扫描、版本解析、FindConflicts、扩展创建、单键 Delete 的错误全部经 `addErr` 收集，`wg.Wait()` 后 `errors.Join` 返回（[resolve.go#L658-L745](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L658-L745)），不再 log-and-continue。
    - 测试：`TestResolveScanFailure`、`TestResolveDeleteFailure`、`TestResolveCommitFailure` 断言失败时扩展未锁定、`countMergeNodes==0`、phase 未推进；HTTP 层未包装错误走 500（[web.go#L2452-L2456](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2452-L2456)）。评审实测全绿。
    - 并发路径：同 key 仅一个 runner（resolveRunning 在 resolveMu 下登记，[resolve.go#L308-L315](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L308-L315)），重复提交只等待，不可能出现第二执行流提前提交。
    - **R3 复验**：单入口 [runResolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L564-L714) 门控顺序未变，错误收集仍为 `errors.Join`；`TestResolveScanFailure`、`TestResolveDeleteFailure/{direct,restart}`、`TestResolveCommitFailure/{direct,restart}` 评审亲自 -race 实跑全部 PASS。
    - **R4 复验**：单入口仍为 [runResolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L564-L714)，instDone 全完成→phaseCommitting+save、扩展全 commit→phaseMerging+save、merge 成功→phaseDone+save 的门控与 `errors.Join` 收集未变；评审 -race 亲自实跑 `TestResolveScanFailure`、`TestResolveDeleteFailure/{direct,restart}`、`TestResolveCommitFailure/{direct,restart}` 全 PASS。

- [x] CP-3: 各阶段失败/崩溃后直接重发与重启重发均断点续跑：唯一 child、无重复节点、删除集合一致
  - **Type**: `rule`
  - **Covers**: AC-3, FR-4, FR-5, FR-6
  - **Evidence**:
    - 扩展 assigned 幂等：[newVersionAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2006-L2152) 覆盖“UUID 已缓存无 DAG 节点（复用 VersionID 直建）/节点已存在（校验父子关系后认领）”；无冲突父版本不生成扩展（`TestResolveHappyPath` 断言 p1 无扩展）。
    - merge assigned 幂等：[mergeAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2257-L2387) 对 reserved/cached/built 三窗口分别登记构建/复用 V 构建/父集合匹配认领，child UUID 先持久化（[resolve.go#L605-L621](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L605-L621)）。
    - 矩阵测试评审实测：`TestResolveCommitFailure/{direct,restart}`、`TestResolveMergeCrashWindows/{reserved-unregistered,cached-no-dag,dag-built-not-done}×{,/restart}`、`TestResolveScanFailure`（含重启）、`TestResolveDeleteFailure/{direct,restart}` 全部 PASS；`assertTombstoneSet`（{a,c}）与 `assertChildReads`（a/c 取 p1、b 取 p2、d 取 p1）逐次通过。R2 已以 `-race -v` 亲自重跑上述全部用例，全部 PASS（见 Review History R2）。
    - ~~备注（advisory）：“单键删除失败 × 重启”单元格无显式测试，见 Finding F-2。~~ **R2 确认已补齐**：`TestResolveDeleteFailure/restart`（先单键删除注入失败 → CloseReopenTest → 同扩展续跑完成）PASS，F-2 关闭。
    - **R3 复验**：`TestResolveMergeCrashWindows/{reserved-unregistered,cached-no-dag,dag-built-not-done}×{,/restart}`、`TestResolveEndToEndRestart`、`TestResolveDeleteFailure/{direct,restart}` 评审亲自 -race 实跑全部 PASS；唯一 child/节点计数/tombstone 集合断言通过。
    - **R4 复验**：评审亲自 -race 实跑 `TestResolveMergeCrashWindows/{reserved-unregistered,cached-no-dag,dag-built-not-done}×{,/restart}`、`TestResolveEndToEndRestart`、`TestResolveDeleteFailure/{direct,restart}`、`TestResolveScanFailure` 全部 PASS，同 child、单 merge 节点、tombstone {a,c} 断言通过；assigned 幂等代码（[newVersionAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2017-L2163)、[mergeAssigned](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2270-L2402)）未变。

- [x] CP-4: 重复提交（含进行中并发）复用同一 child，不新增节点
  - **Type**: `rule`
  - **Covers**: AC-4, FR-7
  - **Evidence**:
    - done 态：[resolve.go#L272-L281](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L272-L281) 校验后直接返回既有 child；`TestResolveHappyPath` 二次提交 child 相同且节点计数不变。
    - 进行中并发：[waitResolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L379-L406) 在 cond 上排队，runner 成功后取同一 child；runner 失败则解锁后以冻结输入重试。`TestResolveConcurrentIdentical`（4 并发）评审实测 PASS，无冲突错误、节点数不变。
    - **R3 复验**：`TestResolveConcurrentIdentical`、`TestResolveHappyPath`（含二次提交）评审亲自 -race 实跑 PASS，child 相同、节点计数不变。
    - **R4 复验**：评审亲自 -race 实跑 `TestResolveConcurrentIdentical`（4 并发同 child）与 `TestResolveHappyPath`（含二次提交）PASS，child 相同、扩展/merge 计数不变。

- [x] CP-5: 陈旧计划指纹检测正确：外部 children/实例身份变化 → 409 + stale 终态持久化，豁免本操作自身节点
  - **Type**: `rule`
  - **Covers**: AC-5, FR-8
  - **Evidence**:
    - [verifyFrozen](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L402-L459)：父版本须存在且 locked；当前 children 集合须等于 冻结集合 + 本操作扩展（可解析时）+ 本操作 final child（当 child 节点以该 parent 为父时）；实例须仍可解析且 InstanceID/DataUUID 一致。集合比较 [sameVersionSet](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L461-L475) 顺序无关、可同时发现增删。
    - 豁免边界经代码核对：持久化的 extensions 必伴随已落盘 DAG 节点（扩展仅在 [newVersionAssigned 末尾 r.save](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2151) 才随 op 持久化），不会把外部节点误豁免；final child 豁免以 child DAG 节点真实 parent 连线为准。
    - 测试评审实测：`TestResolveStaleForeignChild`（p2 新增外来已提交子节点 → stale、持续 409、反向新计划独立成功）、`TestResolveStaleInstanceChanged/{rename,delete}` 全 PASS；[markStale](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L525-L536) 先持久化终态再返回 ErrResolveStale；`TestResolveDoneChildMissing` 验证 child 缺失返回冲突而非伪造节点。
    - **R3 复验**：`TestResolveStaleForeignChild`、`TestResolveStaleInstanceChanged/{rename,delete}`、`TestResolveDoneChildMissing` 评审亲自 -race 实跑全部 PASS；stale 终态持久化断言通过。
    - **R4 复验**：评审亲自 -race 实跑 `TestResolveStaleForeignChild`（含反向新计划成功）、`TestResolveStaleInstanceChanged/{rename,delete}`、`TestResolveDoneChildMissing` 全 PASS；stale 终态持久化断言通过。

- [x] CP-6: 同一 repo 的活动操作互斥；不同计划并发 → 409  **R3：F-4 已消除（见 Review History R3）；R2 曾 fail，历史见下**
  - **Type**: `rule`
  - **Covers**: FR-9
  - **Evidence**:
    - R3 代码核对：候选 op 在锁外预建（[resolve.go#L315-L323](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L315-L323)，含锁外 snapshotFingerprint，此时不可见）；busy 检查 + map 插入 + running 登记合并为单个 resolveMu 临界区（[resolve.go#L327-L348](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L327-L348)）：created 分支先遍历 map 全部 op，任一 `other.active()` 为真即返回 ErrResolveBusy，否则插入本 op，随后统一登记 running；首个 r.save 在解锁之后（[resolve.go#L350-L362](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L350-L362)）。两个不同新计划在该临界区被串行化，后者必见前者 active。
    - 续跑路径（found+active+not running，created=false）与另一新计划并发时：新计划的 busy 遍历同样会发现续跑 op 为 active（[resolve.go#L339-L344](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L339-L344)），无双 runner。
    - R3 亲自运行：`TestResolveDistinctPlansConcurrent`（10 计划并发：success=1、busy=9、map 批操作恰 1）与 `TestResolveConcurrentDifferent`（409/ErrResolveBusy）-race 全 PASS（见 Review History R3）。
    - 历史：R2 曾因 busy TOCTOU（F-4）fail，R2 实测细节保留于 Review History R2。
    - **R4 复验**：busy 临界区（[resolve.go#L327-L348](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L327-L348)）未变；评审亲自 -race 实跑 `TestResolveDistinctPlansConcurrent`（success=1/busy=9）、`TestResolveConcurrentDifferent`（409）PASS。注意：CP-6 仅约束 resolve 操作互斥，不受 F-8 影响。

- [x] CP-7: 仅全部阶段成功才 200 返回可用 child；HTTP 400/409/5xx 语义正确
  - **Type**: `rule`
  - **Covers**: AC-6, FR-10
  - **Evidence**:
    - [runResolve](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L510-L636) 任一步失败即返回 error、无 child 返回；done 态 r.save 成功后才返回 child。
    - handler 错误分类：ErrResolveBadInput→400，ErrResolveStale/ErrResolveBusy→409，其余→500；成功体 `{"child":"..."}`（[web.go#L2446-L2461](file:///Users/ding/Ding/Documents/swe/09271/project-06/server/web.go#L2446-L2461)）；请求体 JSON 格式与校验（data/parents 必填）保持兼容。
    - child 可用性：`TestResolveEndToEndRestart` 在 child 上 Commit → NewVersion → Commit 成功，冲突键优先级读取正确；既有 keyvalue HTTP 端到端 `TestKeyvalueVersioning`（POST resolve）评审实测不改测试通过。
    - **R3 复验**：评审亲自运行 `./datatype/keyvalue/`（-race，ok 1.969s）与 `./server/`（ok 2.774s）全绿；handler 400/409/5xx 分类代码未变（[web.go#L2446-L2458](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2446-L2458)）。
    - **R4 复验**：评审亲自运行 `./datatype/keyvalue/`（-race，ok 2.039s）与 `./server/`（ok 2.931s）全绿；handler 分类与成功体（[web.go#L2446-L2460](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2446-L2460)）未变；`TestResolveEndToEndRestart` 中 child 上 Commit→NewVersion→Commit 成功。

- [x] CP-8: 既有行为非回归：conflict-free merge、未选中实例、只读查询、旧 gob、JSON 不泄漏
  - **Type**: `rule`
  - **Covers**: AC-7, FR-11, NFR-3
  - **Evidence**:
    - 包级 `Merge`/`merge` 与 `/merge` 端点未改（[repo_local.go#L2154-L2247](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2154-L2247)）；`TestResolveEndToEndRestart` 对未选中实例 resdata3 做前后快照 DeepEqual（含部分失败后与最终完成后）。
    - 旧 gob：尾部追加字段位于 passcode 之后，解码失败容错为空 map（[repo_local.go#L3010-L3016](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3010-L3016)）；`TestResolveOpsOldRepoGob` 用无 resolveOps 字段的 gob 验证可加载、passcode 正常；未提升格式版本号。
    - JSON：[MarshalJSON](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3066-L3095) 与 [marshalJSONForVisibility](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3097-L3142) 均不含操作字段；`TestResolveOpsNotInJSON` 实测 PASS。
    - 旧 extensionNode/deleteConflict/DeleteConflicts 已从 datastore.go 干净移除（git diff 仅删除），无遗留引用。
    - **R3 复验**：`go build -tags 'badger filestore ngprecomputed' ./datastore/... ./server/... ./datatype/keyvalue/...` 评审亲自运行成功（BUILD_OK）；`TestResolveOpsOldRepoGob`、`TestResolveOpsNotInJSON` 亲自实跑 PASS；标准 Merge/merge 包级路径未改（[repo_local.go#L2154-L2249](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2154-L2249)）。
    - **R4 复验**：评审亲自运行同一 build 成功（BUILD_OK）；`TestResolveOpsOldRepoGob`、`TestResolveOpsNotInJSON` -race 亲自实跑 PASS；标准 [Merge](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2165-L2260) 包级路径未改，未选中实例快照断言（TestResolveEndToEndRestart）通过。

- [ ] CP-9: 崩溃恢复与持久化设计质量（崩溃窗口完整性、状态机清晰度、gob 兼容、锁正确性）
  - **Type**: `rubric`
  - **Covers**: AC-8, NFR-1, NFR-2
  - **Scale**: 1-5
  - **Anchors**: 1 = 存在未覆盖的崩溃窗口或全局锁滥用；3 = 主要窗口覆盖但恢复逻辑分散重复或锁约定有缺陷；5 = 崩溃窗口矩阵全覆盖，状态机单一入口、幂等路径清晰，gob 兼容与锁约定明确
  - **Pass Threshold**: >= 4
  - **Evidence**:
    - 正面：崩溃窗口矩阵（tasks.md 末表）与测试对应完整：删除（扫描/tombstone）、提交、merge（reserved/cached/built）均有 direct/restart 覆盖；状态机单入口、幂等路径集中在 newVersionAssigned/mergeAssigned；gob 尾部容错规范；R4 全量 -race 套件（datastore/keyvalue）与 server 套件全绿。
    - **F-7 在 R4 确证关闭**：dag.nodes 的全部活动写点（4 处插入 + hideBranch 删除）均持 r.Lock+dag.Lock（hideBranch 另持 repoMutex），全部活动读点持 dag.RLock 或 r.RLock，锁协议一致且互斥；hideBranch 的 id 缓存删除纳入 idMutex；deleteRepo 改为 dag.RLock 快照后再在 idMutex 下处理。TestResolveHideBranchRace -race PASS。
    - **R4 新扣分点 F-8（actionable）**：hideBranch 的嵌套顺序为 `repoMutex→r.Lock→dag.Lock→idMutex`（[repo_local.go#L1806-L1818](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1806-L1818)），而 deleteRepo 在 idMutex 内取 repoMutex（[L1105-L1113](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1105-L1113)）、manager.MarshalJSON/MarshalJSONFiltered 在 idMutex.RLock 内取 repoMutex.RLock（[L361-L367](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L361-L367)/[L378-L384](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L378-L384)）——锁序倒置 ABBA。评审实证：屏障探针首轮即挂死（两次），无任何同步的自然时序探针在 20 轮中第 12 轮挂死；生产入口均为普通 RPC/HTTP 路径（hide-branch、delete repo、GET /api/repos）。
    - **R4 另扣分点 F-9（actionable，低概率）**：hideBranch loop2 重写 node.children 不持 node.Lock（[repo_local.go#L1822-L1832](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1822-L1832)），而 resolve 读路径在释放 dag 后仅持 node.RLock 即读 children（verifyFrozen、snapshotFingerprint）；协调式 -race 探针首轮复现（读探针 vs 写 L1830）。纯生产 API 自然 hammer（-count=20）未复现，窗口仅亚微秒，写模式为 HEAD 预存在，故严重级别低。
    - **评分：3/5（R4 仍未达 >= 4 阈值）→ fail**——崩溃窗口与状态机本身已达 4 分水准，但锁约定存在可确定触发的 live 缺陷（F-8 死锁），符合 anchor 3“锁约定有缺陷”。

## Review History

### Review R1
- **Result**: `fail`
- **Evidence**:
  - 独立通读 spec.md（FR-1~FR-11、NFR-1~NFR-4、AC-1~AC-8）、tasks.md、全部实现文件（datastore/resolve.go、repo_local.go、datastore.go、server/web.go）与全部 5 个同包测试文件；未采信 tasks.md 自述。
  - 亲自执行（构建标签均为 `badger filestore ngprecomputed`，CGO 默认开启）：
    1. `go build -tags 'badger filestore ngprecomputed' ./datastore/... ./server/... ./datatype/keyvalue/...` → 成功（BUILD_OK）。
    2. `go test -tags 'badger filestore ngprecomputed' -timeout 300s ./datastore/` → `ok ... 1.594s`。
    3. `go test ... -v -run 'TestResolve' ./datastore/` → 14 个 Resolve 测试/子测试全部 PASS。
    4. `go test ... -timeout 300s ./server/ ./datatype/keyvalue/` → `ok server 2.839s`、`ok keyvalue 0.842s`（含 HTTP 端到端 resolve 的 TestKeyvalueVersioning）。
    5. `go test ... -race -run 'TestResolve' ./datastore/` → `ok 2.455s`（既有测试套件 -race 通过）。
    6. 为验证评审要求中提示的并发路径，编写临时同包测试（评审后已删除，工作树已恢复原状）：resolve 执行期间并发调用 `r.save()`。`go test -race -run TestReviewScratch` 两次独立复现 DATA RACE：
       - race A：gob 读 map（repo_local.go:3060，encodeMap 于 repoT.GobEncode）vs `Resolve` 写 map（resolve.go:301 `r.resolveOps[key] = op`）。
       - race B：op.GobEncode 读（resolve.go:762）vs runResolve 写（resolve.go:539 `op.instDone[name] = true`，未持 op.mu）。
  - 未运行全量 `make test`（按 AGENTS.md 要求在此明示）；受影响包 datastore、server、datatype/keyvalue 已完整运行。
- **Findings**:
  - **F-1: `actionable`，严重级别 high** —— resolve 操作状态与并发 repo gob 编码之间缺少同步，存在数据竞争：
    - (a) map 级：[resolve.go:301](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L301) 在 resolveMu 下写 `r.resolveOps`，但 repo gob 编码 [repo_local.go:3060](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3060) 不经 resolveMu，任何并发 `r.save()` 都会并发遍历同一 map。
    - (b) 字段级：[resolve.go:493-495](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L493-L495) 写 `extensions`、[resolve.go:538-539](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L538-L539) 写 `instDone`（另多处写 errMsg/updated）均未持 `op.mu`，而 [op.GobEncode](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L750-L778) 在 op.mu.RLock 下读取这些字段——加锁不对称，互斥不成立。
    - 复现：repo 内一次 resolve 执行期间，另一执行流并发 `r.save()`（生产等价路径：同 repo 上另一客户端并发 NewVersion/Commit，例如 POST `/api/repo/{uuid}/branch`、`/commit`，其处理器调用 r.save）；以 `-race` 运行必现上述告警（评审在 6 次迭代内首次迭代即报）。
    - 影响：Go 数据竞争属未定义行为；gob 输出可能撕裂/缺字段，导致 repo 元数据单键损坏、重启后无法加载；违反 NFR-2 与 AC-8 锁正确性要求。
    - 期望：`go test -race` 对“resolve 与同 repo 并发元数据变更”场景无告警；resolveOps map 及 op 全部可变字段的读写经一致的锁协议保护（编码路径与状态变更互斥），且不引入 resolveMu↔repo/DAG 锁倒置（当前 runResolve 在 resolveMu 之外执行，修复时需保持该约束）。
  - **F-2: `advisory`，严重级别 low** —— AC-3 矩阵中“单键删除失败 × 模拟重启”单元格无显式测试（`TestResolveDeleteFailure` 仅 direct；重启路径仅覆盖扫描失败）。代码路径分析无缺陷，建议补充该单元格以闭合矩阵。
  - **F-3: `advisory`，严重级别 low（预存在问题，非本次变更引入）** —— [putNewIDs](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L512-L521) 在 idMutex 之外无锁读取 `m.versionID`/`m.instanceID`（L518），与并发 `newUUID` 写操作竞争；评审 scratch 中并发 NewVersion 亦复现。newVersionAssigned/mergeAssigned 复制了同一模式。建议另行立项统一修复（putNewIDs 调用纳入 idMutex 保护），不作为本次 fail 依据。
- **Recommended Issues**:
  - Issue：修复 resolve 状态与 repo gob 编码的同步（resolveOps map 与 op 可变字段统一锁协议），优先级 high，关联 AC-8/NFR-2；回归要求新增“resolve 期间并发 r.save / 并发 NewVersion+Commit”的 -race 测试并通过，且既有 TestResolve* 全绿。

### Review R2
- **Result**: `fail`
- **结论摘要**：F-1 在原指定场景（单个 resolve 执行期间同 repo 并发 NewVersion/Commit → r.save）确已消除，锁序 resolveMu→op.mu 正确、未发现持锁 r.save；F-2 已补齐；F-3 仍为预存在问题。但 I-1 修复引入两个新的 actionable 缺陷（F-4 busy TOCTOU、F-5 dag.nodes 锁约定竞争），CP-6 不再成立，故 R2 = fail。
- **独立读码**：重新通读 [resolve.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go)（Resolve 入口、waitResolve、verifyFrozen/markStale、resolveExtension、runResolve、deleteInstanceConflicts、op GobEncode/GobDecode）、[repo_local.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go)（repoT GobEncode/GobDecode、save/saveToStore、newVersion、newVersionAssigned、merge、mergeAssigned、putNewIDs、newUUID）、[server/web.go](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2415-L2461)；未采信 tasks.md 自述。
  - F-1 核验：map 全部读写在 resolveMu 下（[L284-L291](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L284-L291)、[L334-L349](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L334-L349)、[L354-L360](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L354-L360)、[L366-L371](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L366-L371)）；编码路径 [repo_local.go#L3028-L3037](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3028-L3037) 持 RLock 并逐个持 op.mu.RLock，map 编码 [L3074](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3074) 在同一 RLock 内。op 可变字段写全部经 mutate/setPhase/reserveChild（搜索字段赋值：仅这三个入口与 GobDecode；extensions/instDone/extCommitted 的索引写亦在 mutate 闭包内）；op.GobEncode 不自锁、由调用方持锁，TestResolveOpGobRoundTrip 与 repo GobEncode 均满足。锁序核查：waitResolve 持 resolveMu 时取 op.mu（同序），无任何路径持 op.mu 后取 resolveMu；所有 r.save 调用点均在 op.mu/resolveMu 释放之后。
  - 残留无锁读（runResolve L572 `instDone[name]`、L621-622 range extensions/extCommitted、verifyFrozen L471 extensions、mergeParents L163）：写方要么是同一 runner goroutine 顺序执行，要么在 delete goroutine 内但已由 `wg.Wait()`/resolveRunning 建立 happens-before，与 gob 读仅构成读-读；不构成 F-1 残留。建议后续统一走 getter（advisory）。
- **亲自执行（标签均为 `badger filestore ngprecomputed`，CGO 默认开启，全部 -count=1 强制实跑）**：
  1. `go build -tags 'badger filestore ngprecomputed' ./datastore/... ./server/... ./datatype/keyvalue/...` → `BUILD_OK`。
  2. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -timeout 400s ./datastore/ ./datatype/keyvalue/` → `ok datastore 2.631s`、`ok datatype/keyvalue 1.967s`。
  3. `go test -tags 'badger filestore ngprecomputed' -count=1 -timeout 300s ./server/` → `ok server 2.658s`。
  4. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -v -run 'TestResolve' ./datastore/` → 17 个测试/子测试全部 PASS（含新增 TestResolveConcurrentSaveRace 0.03s、TestResolveDeleteFailure/{direct,restart}），`ok 2.311s`。
  5. 重点：`go test ... -race -v -run TestResolveConcurrentSaveRace ./datastore/` → `--- PASS (0.05s)`，无 DATA RACE。
  6. 为怀疑修复不完整编写 4 个临时同包测试（评审后全部删除，`git status` 已恢复原文件集合）：
     - save hammer（resolve 全生命周期 8 goroutine 并发 r.save + 4 个重复提交，-race -count=5）→ 挂死；经最小化测试定位为预存在 F-6（见下）。
     - busy TOCTOU（20 对不同新计划同启动门并发 + beforeScan 屏障）→ 复现 F-4：两个计划同时成为 runner；-race 同时报告 F-5（dag.nodes：[repo_local.go:2340](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2340) 写 vs [resolve.go:630](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L630) 读、vs [repo_local.go:2286](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2286) 读）。
     - 最小锁探针（无 resolve 代码：并发 r.save × r.Lock 循环）→ 15s 内挂死，栈：goroutine 阻塞于 [GobEncode L3022](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3022) 嵌套 `r.RLock()`，另一 goroutine 持 `r.Lock` → F-6 预存在。
     - resolve × 3 个并发标准 Merge → -race 报告的均为 F-3 族（versionID：[L518](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L518) 读 vs [L955](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L955)/[L2059](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2059) 写），并再次挂于 F-6。
  - 未运行全量 `make test`（按 AGENTS.md 要求在此明示）；受影响包 datastore、server、datatype/keyvalue 已完整实跑（含 -race）。
- **Findings**:
  - **F-4: `actionable`，严重级别 medium（CP-6 违规）** —— busy 检查与计划 map 插入之间存在 TOCTOU：I-1 将入口拆成两段临界区后，两个“不同 key 的全新计划”可同时通过 [L320-L324](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L320-L324) 的 busy 检查，并分别在 [L339-L348](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L339-L348) 插入 map、成为并发 runner；第二段临界区只复查本 key 的 running，未重扫 active 操作。复现：见上 6(b)，20 对并发中出现双 runner（同启动门、beforeScan 屏障计数），栈快照双 goroutine 均在 runResolve。影响：FR-9“同一 repo 活动操作互斥”被破坏；两个并发 resolve 的删除/提交/merge 无相互协调（多数情况下互判 stale，但无冲突计划可能各自产出 merge 节点）。期望：busy 检查与 map 插入/runner 登记在同一 resolveMu 临界区内原子完成（snapshotFingerprint 可在锁外先做，插入时持锁重扫 active ops 并插入）。
  - **F-5: `actionable`，严重级别 medium** —— dag.nodes 锁约定不一致导致数据竞争：[mergeAssigned L2339-L2341](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2339-L2341) 写 `r.dag.nodes` 仅持 `r.Lock`（镜像标准 merge [L2179-L2181](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2179-L2181)），而新代码读侧 [resolve.go L420/L456/L479/L629](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L629-L631) 与 mergeAssigned 自身 [L2285/L2304/L2344](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2285-L2287) 仅持 `dag.RLock` 不持 r 锁——两种约定不相交。对比 newVersion/newVersionAssigned 写 map 时同时持 r.Lock+dag.Lock。-race 实测：L2340 写 vs L630 读、vs L2286 读（6(b)）。预存在的“r 锁为共同约定”被新引入的 dag.RLock-only 读路径打破；并发标准 merge 与活动 resolve 同样可触发该竞争。期望：统一 dag.nodes 协议（写同时持 r.Lock+dag.Lock，或读侧同时持两把锁），并补“resolve × 并发 merge”的 -race 回归。
  - **F-6: `advisory`，严重级别 low（预存在，非本次变更引入）** —— [GobEncode L3022-L3023](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3022-L3023) 的 `r.RLock(); r.RUnlock()` 在 [saveToStore L3203-L3213](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3203-L3213) 已持 r.RLock 时构成嵌套读锁；期间若有 r.Lock 写者等待，嵌套 RLock 阻塞 → 自死锁。最小探针（无 resolve 代码）15s 内必挂。HEAD 代码即存在，两个并发标准 Merge 亦可触发。建议另行立项（移除该对无意义的 RLock/RUnlock，或改为不重入的快照），不阻塞本次。
  - **F-1：关闭** —— 指定场景下 -race 全绿（命令 2/4/5），map 与 op 可变字段读写协议一致、编码与写路径互斥，无锁倒置。
  - **F-2：关闭** —— TestResolveDeleteFailure/restart PASS，矩阵闭合。
  - **F-3：维持 advisory 预存在，不阻塞** —— 本轮多次 -race 再确认（并发 NewVersion/Merge 路径），建议与 F-6 一并在独立 Issue 中修复。
- **Recommended Issues**:
  1. 修复 F-4：busy 检查/插入原子化，新增“两个不同新计划并发提交”的 -race 测试。
  2. 修复 F-5：统一 dag.nodes 锁约定，新增“resolve × 并发标准 Merge”的 -race 测试。
  （F-3/F-6 为预存在问题，建议另行立项。）

### Review R3
- **Result**: `fail`
- **结论摘要**：F-4（busy TOCTOU）确已消除：候选 op 锁外预建且不可见，busy 检查 + map 插入 + running 登记合并为单个 resolveMu 临界区，两个不同计划无法同时进入 runResolve；未发现新增死锁，无持 resolveMu 时 r.save。F-1/F-2/F-6 保持关闭，F-3 确为预存在问题。但 **F-5 未完全消除**：I-2 统一了四处 dag.nodes“插入”写点，遗漏第五个活动写点 [hideBranch](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1809) 的 `delete(r.dag.nodes, v)`——仅持 r.Lock、不持 r.dag.Lock，与 resolve 侧仅持 dag.RLock 的读路径不互斥；R3 以**纯生产路径**（HideBranch RPC × 完成态 resolve 的并发重复提交）-race 探针确定复现 → Finding F-7（actionable）。CP-9 锁约定仍有 live 缺陷，评分 3/5，故 R3 = fail。
- **独立读码（未采信任何自述）**：
  - 通读 [resolve.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go) 全文（899 行）：Resolve 入口、waitResolve、snapshotFingerprint、verifyFrozen、sameVersionSet、markStale、resolveExtension、runResolve、deleteInstanceConflicts、op GobEncode/GobDecode；[repo_local.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go) 的 newData、newVersion、newVersionAssigned、merge、mergeAssigned、commit、hideBranch、deleteRepo、addRepo、repoT GobEncode/GobDecode、save/saveToStore、putNewIDs、manager.versionFromUUID；[server/web.go](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2415-L2461) handler；[server/rpc.go hide-branch](file:///Users/ding/Documents/swe/09271/project-06/server/rpc.go#L652-L658)；并逐一核对全部 dag.nodes 访问点（grep 全仓 76 行命中）。
  - **F-4 核验**：第一段临界区仅查表（[L284-L287](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L284-L287)）；新计划候选 op 在锁外 `newResolveOp` + `snapshotFingerprint`（[L315-L323](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L315-L323)），未入 map 前任何 goroutine 不可见；第二段临界区（[L327-L348](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L327-L348)）内 created 分支遍历 map 全部 op 检查 `other.active()`，有 active 即 ErrResolveBusy，否则插入并登记 running。两个不同新计划被该锁串行化，后者必见前者 phaseDeleting（active）。续跑（created=false）与新计划并发时，新计划的 busy 遍历同样能发现续跑 op。同 key 竞争由 `already running` 复查 + waitResolve 兜底。无残留双 runner 路径。
  - **dag.nodes 访问点逐一核对结果**：四处插入写点 [newVersion L1975-L1980](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1975-L1980)、[newVersionAssigned L2131-L2136](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2131-L2136)、[merge L2179-L2183](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2179-L2183)、[mergeAssigned L2341-L2345](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2341-L2345) 均 r.Lock+r.dag.Lock ✓；resolve 侧读点 [snapshotFingerprint L419-L421](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L419-L421)、[verifyFrozen L455-L457/L478-L480](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L455-L480)、[runResolve commit L628-L630](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L628-L630) 均 dag.RLock ✓；其余读点持 r.RLock/r.Lock（getNodeByVersion、lockedUUID、commit、makeMaster 等）✓；加载/addRepo/remap/copy/push 为离线或传输上下文（豁免）✓。**例外**：[hideBranch L1803-L1809](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1803-L1809) 持 r.Lock 但不持 r.dag.Lock 即 `delete(r.dag.nodes, v)`（活动 RPC 路径，非离线）；同函数 L1807-L1808 删除 m.versionToUUID/m.uuidToVersion 亦不持 m.idMutex。另：[deleteRepo L1099](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1099) 在 idMutex 下无 r/dag 锁 range dag.nodes（repo 删除路径，同类）。
  - **死锁/锁序核验**：锁序 resolveMu→op.mu（waitResolve、busy 遍历 other.active()、GobEncode）；r→resolveMu→op.mu（saveToStore 持 r.RLock 进 GobEncode）；r→dag（四个写点与 newVersion 的姐妹检查）。未发现反向序（无持 op.mu 等 resolveMu、无持 dag 等 r）；busy 临界区内无 r.save，首个 r.save 在 [L350-L362](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L350-L362) 解锁之后；markStale 先释放 op.mu 再 r.save；F-6 修复后全程无挂死（各探针均秒级完成，R2 的 save hammer 挂死不再出现）。
  - **F-1/F-2/F-6 复核**：map 全部读写在 resolveMu 下；op 可变字段写全部经 mutate/setPhase/reserveChild；repo GobEncode 持 resolveMu.RLock + 每个 op.mu.RLock（[L3025-L3079](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3025-L3079)）；F-2 矩阵单元格（delete failure/restart）测试在；F-6 嵌套 RLock 已移除，[saveToStore L3200-L3218](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3200-L3218) 仅持一次 r.RLock。F-3（putNewIDs 无锁读、NumVersions 无锁 len 读）HEAD 预存在代码确认。
- **亲自执行（标签均为 `badger filestore ngprecomputed`，CGO 默认开启，全部 -count=1 拒绝缓存；Go 1.27.1 darwin/arm64）**：
  1. `go build -tags 'badger filestore ngprecomputed' ./datastore/... ./server/... ./datatype/keyvalue/...` → `BUILD_OK`。
  2. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -timeout 400s ./datastore/ ./datatype/keyvalue/` → `ok datastore 2.859s`、`ok datatype/keyvalue 1.969s`。
  3. `go test -tags 'badger filestore ngprecomputed' -count=1 -timeout 300s ./server/` → `ok server 2.774s`。
  4. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -v -run 'TestResolveDistinctPlansConcurrent|TestResolveConcurrentMergeRace|TestResolveConcurrentSaveRace' ./datastore/` → 三测试全部 PASS（0.05s/0.03s/0.05s），`ok 1.520s`，无 DATA RACE。
  5. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -v -run TestResolve ./datastore/` → 19 个测试（含全部子测试）全 PASS，`ok 2.444s`。
  6. 临时同包探针 `datastore/r3probe_test.go`（仅生产导出 API：buildResolveTestEnv → callResolve 完成 → 8 goroutine 并发 callResolve（done 态走 verifyFrozen）；主线程 NewVersion(root,"hideable") → Commit → HideBranch(root,"hideable")），评审后已删除：
     - `go test -race -count=1 -v -run TestR3HideBranchProbe ./datastore/` 首轮即报 **2 个 DATA RACE**：
       - race A（F-3 族）：写 `mapassign_faststr` [hideBranch repo_local.go:1808](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1808)（delete m.uuidToVersion，无 idMutex）vs 读 `mapaccess1_faststr` [manager.versionFromUUID repo_local.go:976](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L976)（持 idMutex.RLock，写方不配对），调用链 done-case verifyFrozen [resolve.go:298](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L298) → :451。
       - race B（F-5 残留，actionable）：写 `mapassign_fast32` [hideBranch repo_local.go:1809](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1809)（delete r.dag.nodes，仅 r.Lock）vs 读 `mapaccess1_fast32` [verifyFrozen resolve.go:456](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L456)（仅 dag.RLock）；done-case 入口 [resolve.go:298](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L298)。测试以 `--- FAIL (0.04s)` 结束。
     - 同场景另一版本（paused resolve + 额外 verifyFrozen 读者）首轮复现同样两个告警，读点另有 resolve.go:451/456，写点 L1808/L1809。
  - 探针删除后 `git status --porcelain` 与评审开始前逐字节一致（仅 feature 既有改动 + .trae/），无残留探针文件。
  - 未运行全量 `make test`（按 AGENTS.md 要求在此明示）；受影响包 datastore、server、datatype/keyvalue 已完整实跑（含 -race）。
- **Findings**:
  - **F-7: `actionable`，严重级别 medium（F-5 未完全消除 / AC-8 锁正确性）** —— dag.nodes 第五个活动写点被遗漏：[hideBranch](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1792-L1827) 在 [L1809](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1809) `delete(r.dag.nodes, v)` 时仅持 r.Lock（L1801），不持 r.dag.Lock；而本特性引入的 resolve 读路径（[verifyFrozen L455-L457/L478-L480](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go#L455-L480)、snapshotFingerprint L419-L421、runResolve L628-L630）仅持 dag.RLock——两把锁不相交，I-2“写持双锁则两种读都安全”的论证对该写点不成立。复现：JSON-RPC `hide-branch`（[rpc.go:652](file:///Users/ding/Documents/swe/09271/project-06/server/rpc.go#L652)）与活动/完成态 resolve 的并发提交同 repo 执行，-race 首轮必报 mapaccess_fast32 vs mapassign_fast32（探针已记录）。特性前所有读者均持 r 锁、与 hideBranch 安全，故缺陷由本特性新读路径暴露。影响：Go 数据竞争属未定义行为，并发 DAG 删除/读取可撕裂元数据状态；违反 NFR-2 与 F-5 的关闭标准“全部读写路径锁协议一致”。期望：将 hideBranch 的 dag.nodes 删除纳入 r.Lock+r.dag.Lock（并把 m.versionToUUID/m.uuidToVersion 删除纳入 m.idMutex），同步排查 deleteRepo L1099 同类 range；新增“resolve（含 done 态重复提交）× HideBranch”的 -race 回归并通过，既有 TestResolve* 保持全绿。
  - **F-3 族（advisory，预存在，不阻塞）**：探针 race A 显示 hideBranch 不持 idMutex 删 id 缓存（L1807-L1808），与 R1 记录的 putNewIDs 无锁读同源；建议与 F-3 合并立项。
  - **F-4：关闭** —— busy/插入/登记单临界区原子（代码核对 + TestResolveDistinctPlansConcurrent -race PASS：success=1/busy=9/map 批操作=1）。
  - **F-1/F-2/F-6：保持关闭** —— 见上独立读码与命令 2/4/5。
- **Recommended Issues**:
  1. 完成 F-5 收尾：hideBranch（及 deleteRepo）dag.nodes 写路径补齐双锁、id 缓存写补 idMutex，新增 resolve × HideBranch 的 -race 回归。
  2. F-3 族 id 缓存无锁访问另行立项统一修复。

### Review R4
- **Result**: `fail`
- **结论摘要**：F-7 确证消除——dag.nodes 的全部活动插入/删除写点均持 r.Lock+dag.Lock（hideBranch 另持 repoMutex），全部活动读点持 dag.RLock 或 r.RLock，协议一致互斥；hideBranch 的 id 缓存删除已纳入 idMutex；deleteRepo 改为 dag.RLock 快照版本列表后再在 idMutex 下处理；TestResolveHideBranchRace -race PASS。F-1/F-2/F-4/F-6 保持关闭，F-3 确为预存在 advisory。但 I-3 的嵌套锁序引入新的 actionable 缺陷 **F-8（ABBA 死锁）**：hideBranch 为 repoMutex→…→idMutex，而 deleteRepo/MarshalJSON 为 idMutex→repoMutex；屏障探针与无同步的自然时序探针均实证挂死。另有低概率 **F-9**（hideBranch 不持 node.Lock 重写 node.children，与 resolve 的 node.RLock 读路径不互斥，协调式 -race 探针复现）。CP-9 评分维持 3/5，故 R4 = fail。
- **独立读码（未采信任何自述）**：
  - 重读 [resolve.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/resolve.go) 全文（899 行）与 [repo_local.go](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go) 关键函数：hideBranch（[L1798-L1837](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1798-L1837)）、deleteRepo（[L1062-L1122](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1062-L1122)）、newVersion/newVersionAssigned（[L1908-L2163](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1908-L2163)）、merge/mergeAssigned（[L2165-L2402](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L2165-L2402)）、repoT GobEncode（[L3036-L3090](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3036-L3090)）、save/saveToStore（[L3203-L3229](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3203-L3229)）、putNewIDs（[L512-L521](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L512-L521)）、MarshalJSON/Filtered（[L359-L397](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L359-L397)）；handler [web.go#L2415-L2461](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L2415-L2461)；生产入口 [rpc.go delete/hide-branch](file:///Users/ding/Documents/swe/09271/project-06/server/rpc.go#L418)、[web.go repos 列表](file:///Users/ding/Documents/swe/09271/project-06/server/web.go#L1756)。
  - **dag.nodes 访问点逐一核对（全仓 grep 命中逐一过）**：活动写点四处插入（newVersion L1987-1989、newVersionAssigned L2142-2145、merge L2190-2193、mergeAssigned L2352-2355）均 r.Lock+dag.Lock ✓；hideBranch 删除（L1818）持 repoMutex+r.Lock+dag.Lock ✓；活动读点 resolve 侧（snapshotFingerprint L419-421、verifyFrozen L455-457/L478-480、runResolve L628-430）持 dag.RLock ✓；其余活动读点持 r.RLock/r.Lock（getNodeByVersion、lockedUUID、commit、makeMaster 等）✓。豁免（离线/传输，无活动并发上下文）：加载（L596/L833）、addRepo（L1049）、duplicate（L2934）、repoT.versionFromUUID（L3308，shadow repos）、remapLocalIDs（L3344-3364，addRepo 前的传输态）、copy_local.go（L551-587，push 过滤传输上下文；该处预存在 repoMutex→idMutex 序，建议另立）。
  - **F-1/F-6 复核**：repo GobEncode 持 resolveMu.RLock 并逐个 op.mu.RLock（[L3040-L3049](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L3040-L3049)），map 编码在同一临界区；saveToStore 仅一次 r.RLock（L3215），F-6 嵌套 RLock 不存在；r.save 均在 resolveMu/op.mu 释放之后。F-3：putNewIDs L518 无锁读 m.repoID/m.versionID/m.instanceID，HEAD 预存在代码确认，不阻塞。
- **亲自执行（标签均为 `badger filestore ngprecomputed`，CGO 默认开启，全部 -count=1 拒绝缓存；Go 1.27.1 darwin/arm64）**：
  1. `go build -tags 'badger filestore ngprecomputed' ./datastore/... ./server/... ./datatype/keyvalue/...` → `BUILD_OK`。
  2. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -timeout 400s ./datastore/ ./datatype/keyvalue/` → `ok datastore 3.363s`、`ok datatype/keyvalue 2.039s`。
  3. `go test -tags 'badger filestore ngprecomputed' -count=1 -timeout 300s ./server/` → `ok server 2.931s`。
  4. `go test -tags 'badger filestore ngprecomputed' -race -count=1 -v -run 'TestResolve' ./datastore/` → 19 个顶层测试（含全部子测试共 31 个 RUN：CommitFailure/{direct,restart}、MergeCrashWindows 三窗口×{,/restart}、DeleteFailure/{direct,restart}、StaleInstanceChanged/{rename,delete} 等）全部 PASS，`ok 2.550s`，无 DATA RACE。
  5. 临时同包探针（评审后已移出至 /tmp 并确认工作树恢复）：
     - **TestR4HideDeleteDeadlock（hideBranch × deleteRepo，屏障启动）→ 首轮即 DEADLOCK，`--- FAIL (5.13s)`**：goroutine 持 repoMutex 阻塞于 [repo_local.go:1814](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1814)（idMutex.Lock）；另一 goroutine 持 idMutex 阻塞于 [repo_local.go:1113](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1113)（repoMutex.Lock）。
     - **TestR4HideJSONDeadlock（hideBranch × getManager().MarshalJSON）→ 首轮即 DEADLOCK，`--- FAIL (5.12s)`**：阻塞点 [repo_local.go:1814](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1814) vs [repo_local.go:363](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L363)。
     - **TestR4NaturalDeadlock（每轮随机 0-3ms 错峰、无任何屏障，20 轮）→ 第 12 轮 NATURAL DEADLOCK，`--- FAIL (10.44s)`**：栈同上（1814 vs 1113），证明普通时序即可触发。
     - **TestR4HideChildrenRace（8 读者在 dag.RUnlock 后等待释放再 node.RLock 读 children）→ 首轮 `WARNING: DATA RACE`**：读探针 r4probe_test.go:159 vs 写 [repo_local.go:1830](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1830)，`--- FAIL (0.51s)`。
     - TestR4ResolveHideRootRace（纯生产 API：完成态重复 Resolve × HideBranch）：PASS 0.08s，`-race -count=20` 亦 `ok 2.375s`——F-9 自然触发概率低，仅协调式必现。
  - 探针移出后 `git status --porcelain` 与评审开始前逐字节一致（仅 feature 既有改动 + .trae/）。
  - 未运行全量 `make test`（按 AGENTS.md 要求在此明示）；受影响包 datastore、server、datatype/keyvalue 已完整实跑（含 -race）。
- **Findings**:
  - **F-8: `actionable`，严重级别 high（AC-8 锁正确性 / 可用性）** —— I-3 把 hideBranch 的 idMutex 加在 repoMutex 临界区内，锁序变为 repoMutex→r.Lock→dag.Lock→idMutex（[L1806-L1818](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1806-L1818)）；而 deleteRepo 在 idMutex 下循环取 repoMutex（[L1105-L1115](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1105-L1115)）、manager.MarshalJSON/MarshalJSONFiltered 在 idMutex.RLock 下取 repoMutex.RLock（[L361-L367](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L361-L367)/[L378-L384](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L378-L384)）——两种方向构成 ABBA。前置 hideBranch 不触 idMutex，故倒置由 I-3 引入。复现：JSON-RPC `hide-branch` 与 `delete repo`（可针对不同 repo，锁为 manager 全局）、或 hide-branch 与 GET `/api/repos` 并发；屏障版首轮挂死、自然随机错峰 20 轮中第 12 轮挂死（均实测）。影响：请求永久挂起且后续任何需要这两把锁的操作全部阻塞，需重启进程；r.save 不会执行，隐藏停留在内存态。期望：统一锁序（建议全程 idMutex→repoMutex：hideBranch 应在 repoMutex 之外的独立 idMutex 临界区处理 id 缓存，或将 dag 快照/删改拆到锁外），并新增 hide-branch × delete repo/repos 列表的并发回归（带挂死看门狗）。
  - **F-9: `actionable`，严重级别 low（锁协议一致性）** —— hideBranch loop2（[L1822-L1832](file:///Users/ding/Documents/swe/09271/project-06/datastore/repo_local.go#L1822-L1832)）重写存活节点 node.children 不持 node.Lock；resolve 读路径（verifyFrozen L461-464、snapshotFingerprint L425-427）在 dag.RUnlock 之后仅持 node.RLock 读 children——与 F-7 同型的下一层锁缺陷。协调式 -race 探针首轮复现；纯生产 hammer（-count=20）未复现，窗口亚微秒；无锁写模式 HEAD 预存在，由本特性读路径暴露。期望：node.children 改写纳入 node.Lock（或全程持 dag.Lock 且读者在 dag 内读完再释放）。
  - **F-7：关闭** —— dag.nodes map 全部活动读写点锁协议一致互斥、id 缓存删除持 idMutex、deleteRepo 快照前置；TestResolveHideBranchRace -race PASS。
  - **F-1/F-2/F-4/F-6：保持关闭**；**F-3：维持预存在 advisory，不阻塞**。
- **Recommended Issues**:
  1. 修复 F-8：统一 repoMutex/idMutex 嵌套顺序，新增 hide-branch × delete repo / repos 列表并发回归（含挂死看门狗），既有 TestResolve* 保持全绿。
  2. 修复 F-9：hideBranch 的 node.children 重写补 node.Lock 并回归。
  3. F-3 族（含 putNewIDs、copy_local 锁序）id/锁约定问题另行立项统一处理。
