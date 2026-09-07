# Task06-B 对抗式复审 v0.1-rc2

## 1. 总体结论

**PASS — 可以进入实现**

本轮复审针对 rc1 的 1 个 BLOCKER、2 个 MAJOR、2 个 MINOR 逐项检查，并用现有 Task04/M4/M5/Task06-A 真实函数、内存 DuckDB 和临时 DuckDB 复现边界场景。五项 rc1 问题均已在 rc2 设计中闭环；未发现新的 BLOCKER、MAJOR 或 MINOR。

复审范围仍是设计合同和上游事实行为，不代表 Task06-B 已经实现或完成生产验收。业务评分结构及 35/15/20/10/10/6/4 权重未重新设计。

## 2. rc1 B1 / M1 / M2 / N1 / N2 closure table

| 编号 | rc1 问题 | rc2 闭环证据 | 复审结论 |
|---|---|---|---|
| B1 | 用当前 `episode_end_date IS NULL` 判断 D 日资格，会把 E=D 正常结束日判为合同异常，并使 E>D 结束后的历史 replay 改变 | §3.2 明确从 finalized `theme_m4_observation(D).custom_index_episode_id` 关联 Episode；资格为 `confirmed_date <= D AND (end_date IS NULL OR end_date > D)`；`end_date=D` 明确为 `NO_OPEN_EPISODE`；§5.1 强制从 D 日和 Episode 起点 index 派生 return；§11.5 要求 E>D 后 replay D 业务一致 | **CLOSED**。真实 `run_m4_daily()` 复现确认历史 M4 不变、结束日仍保留 pointer；rc2 规则能正确投影 D 视角 |
| M1 | M5 `SUCCESS` 硬依赖会阻断 popularity `UNAVAILABLE` 时的 `INCOMPLETE_INPUT` 物化 | §8.2、§11.1 Path A 将 availability 作为先决事实；required source `UNAVAILABLE` 时允许执行 Task06-B，不要求 M5(D) SUCCESS，eligible 行物化为 `INCOMPLETE_INPUT`；Path B 才要求 M5 完成和 fingerprint 一致；明确不扩建 generic scheduler、不放宽 M5 fail-closed | **CLOSED**。真实 M5 在 THS 空源时仍按正式合同 `FAILED`，rc2 已规定该路径绕过无条件 scheduler gate |
| M2 | 同日热榜 replacement 后，旧 M5 与新 availability 数量/序号一致但事实版本错配 | §8.4 要求对当前 Theme/Membership、两路热榜行只读重算 deterministic M5 fingerprint，并与 persisted `input_snapshot_id` 精确相等；数量/序号不变也必须阻断 stale M5；禁止比较 run 时间或异类 ID | **CLOSED**。真实 replacement 复现得到旧/新 appearance 反转且 fingerprint 不同 |
| N1 | 未充分区分 expected state 行缺失和行存在但 `is_above_or_equal_ma5` 合法 NULL | §5.4 先用正式 `trading_calendar` 构造 expected days 并逐日要求恰有一行；`IS NULL` 是合法状态，不转 false；分母为 non-NULL valid days，metadata 记录 expected/valid/null/true/false | **CLOSED**。真实状态 `[true,true,NULL,NULL,NULL]` 保持 5 行完整，ratio 为 `1.0`，不会误算为 `0.4` 或缩分母 |
| N2 | N=1、UNAVAILABLE、NO_VARIATION 组合优先级及全空一级维度未定义 | §6.4、§7.3、§8.7 明确 `INCOMPLETE_INPUT > INSUFFICIENT_UNIVERSE > NO_VARIATION > OK`；trusted UNAVAILABLE 覆盖 N=1；全维度无 active leaf 时 dimension score 为 NULL，全球贡献为 0，不二次重权；全 leaf NO_VARIATION 时 final 数值为 NULL | **CLOSED**。Task06-A rank helper、精确权重等价性和 rc2 聚焦断言均通过 |

## 3. 新发现问题（BLOCKER / MAJOR / MINOR）

未发现新问题。

具体复核结果：

- Episode 指针字段位于 `theme_m4_observation`，而非 `theme_custom_index_state`；rc2 已按真实 schema 修正。
- D-aligned Episode Return 不读取未来最终 episode return；现有上游 index 起点和 D 日 index 足以派生。
- M5 仍保持自身 fail-closed；rc2 只调整 Task06-B 的条件门禁，没有要求修改 M4/M5 schema 或放宽 M5 合同。
- fixed `U_D`、average ties、exact final tie key、NO_VARIATION-only 重权和 target-date atomic replace 没有回归。
- 没有因相关性、权重偏好或 Damage/M6/授权范围提出非阻塞架构建议。

## 4. 实测反例与测试结果

### 4.1 真实上游生命周期复现

执行 `audit_task06_b_rc1.py`，使用内存 DuckDB 和真实 `ThemePipelineService.run_m4_daily()`：

- Episode 在 2026-08-11 确认，2026-08-18 结束；2026-08-14 的 M4 行前后逐字段不变。
- 结束后的历史查询若使用 rc1 的 `IS NULL` 会得到 0 个 open 行；rc2 的 `confirmed_date <= D < end_date` 对 08-14 保持 eligible。
- 结束日 M4 仍携带 Episode pointer，属于正常 `NO_OPEN_EPISODE` 输入形态。
- open Episode 的存储 return 为 `0.0`，D-aligned return 为 `118/105 - 1 = 0.1238095238`；未来 final return 未被使用。

### 4.2 popularity unavailable 与 stale fingerprint

- 正式东财快照成功、THS 写入可信 `(UNAVAILABLE, 0)`；正式 M5 合同结果为 `FAILED / THEME_M5_THS_HOT_INPUT_INCOMPLETE`。
- rc2 Path A 不把该 M5 失败当作 Task06-B 的 scheduler 阻断条件，输出路径为 eligible `INCOMPLETE_INPUT`，而不是把 unavailable 当零人气。
- 同日替换东财 Top100 成员但保持数量和 `snapshot_seq=[1]` 不变，旧 M5 rates `[0.5, 0.0]`，当前 source 重算 rates `[0.0, 0.5]`；persisted 与重算 fingerprint 不同，必须 fail-fast。

### 4.3 状态、排名和持久化边界

- Above-MA5 完整行 `[true,true,NULL,NULL,NULL]`：expected=5、valid=2、null=3、raw=1.0。
- 160 组随机 N/并列输入验证 exact integer key 与 weighted NRS、一级维度展示分完全等价。
- average tie 得到 raw rank `1.5`；N=1 得到 `INSUFFICIENT_UNIVERSE`；所有 leaf `NO_VARIATION` 时不产生 0/0 或二次维度重权。
- 现有 Task06-A `_persist()` 注入 audit 主键冲突后，两张结果表均保持旧版本，验证 atomic rollback 模式可沿用。

### 4.4 测试汇总

- 定向上游 pytest：8 个测试文件，**82 passed**，1 个既有依赖弃用 warning。
- rc1 复现脚本：6 个场景组，**全部通过**，末行 `AUDIT REPRODUCTIONS PASSED`。
- 新增 rc2 聚焦复现：**15 assertions passed**，覆盖日期投影、D-aligned return、状态优先级、空维度、条件门禁和 fingerprint 语义。
- 未运行全仓测试或真实生产数据验收；Task06-B 当前尚未实现，本报告不宣称实现已通过生产验收。

## 5. 是否允许进入 Task06-B implementation

**允许。** rc2 设计合同已经处理 rc1 的时间语义、M5/availability 门禁、输入版本一致性、合法 NULL、退化状态和原子发布边界；可以进入实现阶段。实现时必须把 §11.1 的 Path A/Path B 条件门禁、§8.4 fingerprint 校验、§5.4 expected-day 完整性校验和 §8.7 状态优先级落实为可测试代码。

## 6. 若 FAIL，只列最小必须修订项

不适用。本轮结论为 PASS，无必须修订项。

复审材料：

- 设计书：`docs/QRP产品蓝图v1.1/task06/Task06-B System B Theme Trend Rank 设计书 v0.1-rc2.md`
- rc1 审计：`docs/QRP产品蓝图v1.1/task06/Task06-B Adversarial Audit v0.1-rc1.md`
- 原复现：`docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc1.py`
- 本轮复现：`docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc2.py`

复审分支：`feature/v1.1-task06-b`  
复审时 HEAD：`02032ef4777401f7fc0082071c1269c0345b8979`  
本轮新增/修改：本报告、新增 rc2 聚焦复现脚本；未修改业务实现。
工作区状态：未 clean；仅包含上述两份未提交审计材料，业务源码无改动。
