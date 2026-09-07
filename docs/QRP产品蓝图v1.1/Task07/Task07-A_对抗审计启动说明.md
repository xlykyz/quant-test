# Task07-A 二次对抗审计启动说明

请基于远端 `develop/v1.1` 最新状态，对以下 Revision 1 设计执行**第二轮对抗审计**：

- `docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md`

首轮结论为 `NEEDS_REVISION`。Revision 1 已逐项处置首轮 2 个 BLOCKER、6 个 MAJOR 以及全部 MINOR/NIT。第二轮不要因为“已修订”而降低攻击强度。

## 总原则

> Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。

## 第二轮必须逐项复核首轮问题

### B-1 Full Snapshot

确认 Revision 1 的以下语义是否充分且与现有 Portfolio Engine 一致：

- positions = 完整 desired portfolio state；
- omitted current asset = target 0；
- `positions=()` = all cash；
- native converter 不再存在 patch interpretation。

### B-2 Unified Result Validation

确认：

- 唯一 `validate_strategy_result()` 足以 fail-closed；
- 所有 QRP-owned `strategy.run()` 调用点都能在 Adapter / persistence / Engine 前接入；
- 不要求不现实地拦截外部 Python 调用者直接 `.run()`。

### M-1 Native Target Routing / Date

确认：

- `strategy_result_to_target_weights()` 是唯一最高层路由；
- native target 非空时绝不调用 decisions adapter；
- target `trade_date` = strategy signal/target date；
- native converter 不做日期 shift；
- Product existing timing shift 只发生一次。

### M-2 Holdings As-of

确认：

- holdings 是首个 prepared trade date 之前的 initial snapshot；
- `holdings_as_of_date < min(prepared_data.trade_date)` 是否足够 deterministic 且不过度限制合法场景；
- 多日运行由策略确定性推进 state 是否符合现有 Strategy Protocol 边界。

### M-3 Legacy Compatibility

确认 key union 等价规则是否完整：

```text
initial_positions.get(asset_id, False)
==
(asset_id in holdings)
```

以及 holdings 只包含 `current_weight > 0`, `entry_count >= 1` 的 current holdings 是否足够。

### M-4 Deterministic Serialization

确认新增 target evidence JSON-compatible tree、date ASC / asset ASC、canonical JSON 规则足够，同时没有为了 Task07-A 追溯重构 legacy evidence。

### M-5 Legacy StrategyBacktestRuntime

确认 native target 时 fail-fast unsupported-path 是最小且安全方案，不应为了本任务把 trade-level runtime 改造成 Portfolio Engine。

### M-6 Holdings Product/Backtest Boundary

重点攻击 Revision 1 的取舍：

- StrategyInput 底层支持 typed holdings；
- 07-A 不改造 cash-only PortfolioBacktestEngine 为 seeded-account engine；
- QRP-owned wrapper 不允许静默丢弃非空 holdings；
- Task08 可从空组合历史运行；Task09 再负责每日 production holdings orchestration。

判断这个边界是否既满足 Task07-A，又不会把缺口拖到 07-B/C 无法实现。

## 新增重点攻击问题

1. `omitted asset = 0` 与转换层“必要时补显式 zero rows”是否会产生跨日状态隐式依赖或 replay 不确定性。
2. native target Engine frame 中 `priority=0.0` 是否真的安全，现有 Portfolio Engine 是否会重新根据 priority 做不应有的业务选择。
3. `holdings_as_of_date` 严格早于首个 prepared date 是否与日内/收盘后 daily strategy 合同冲突。
4. `StrategyHoldingState` 缺 quantity/cost/cash 是否会真实阻塞 07-B/C；如声称阻塞必须给正式 System B 规则证据。
5. rich target evidence 只保留在 canonical StrategyRunResult、Engine frame 为 lossy projection，是否与现有 Product result/replay 基础设施兼容。
6. 新 unified result validator 是否会意外改变 legacy strategy behavior。
7. Revision 1 是否新增了任何首轮没有的 scope creep。

## 必查代码

至少核对：

- `src/qrp_atlas/strategies/models.py`
- `src/qrp_atlas/strategies/registry.py`
- `src/qrp_atlas/strategies/validation.py`
- `src/qrp_atlas/strategies/declarative/`
- `src/qrp_atlas/backtest/runtime/strategy.py`
- `src/qrp_atlas/backtest/portfolio/strategy.py`
- `src/qrp_atlas/backtest/portfolio/engine.py`
- `src/qrp_atlas/backtest/product/service.py`
- `src/qrp_atlas/backtest/product/timing.py`
- `src/qrp_atlas/backtest/results/` 相关 strategy/reproducibility persistence
- `docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md`
- `docs/QRP产品蓝图v1.1/03_开发路线图与工作包.md`
- Task05 / Task06 相关关闭设计与 walkthrough

## 输出格式

只输出仍然成立的新问题或未被充分解决的问题，按：

- BLOCKER
- MAJOR
- MINOR
- NIT

每项必须包含：

- Evidence
- Impact
- Minimal Fix

对首轮已充分解决的项，放在“Resolved Verification”中简短确认，不要重新包装成问题。

最终必须给：

```text
READY_FOR_IMPLEMENTATION
或
NEEDS_REVISION
```

约束：

- 只审计，不编码；
- 不创建实现分支；
- 不扩张为 Strategy Framework 全面重构；
- 没有证据不要脑补；
- 目标是判断 Revision 1 是否已足够进入实现，而不是追求理论完美。
