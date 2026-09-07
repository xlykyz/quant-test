# Task07-A 三次对抗审计启动说明

请基于远端 `develop/v1.1` 最新状态，对以下 **Revision 2** 执行第三轮对抗审计：

- `docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md`

前两轮结论均为 `NEEDS_REVISION`。第三轮仍按同等攻击强度执行，只报告仍成立的问题或 Revision 2 新引入的问题。

## 总原则

> Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。

## 本轮必须重点复核

### 1. Checked Strategy Runner

Revision 2 冻结：

```text
validate_and_normalize_strategy_input
→ strategy.run exactly once
→ validate_strategy_result
```

并建议一个薄 `run_strategy_checked()` Common helper。

重点攻击：

- 这是否是解决 holdings execution-before fail-closed 的最小方案；
- 是否能覆盖 Registry、StrategyBacktestRuntime、Portfolio helper、Product Service、cross-sectional、event、residual 等全部 QRP-owned 正式 run 路径；
- 是否会造成某些 strategy 内部二次 input validation 或行为变化；
- input normalization 是否会改变 legacy built-in/declarative semantics；
- 是否存在 QRP-owned direct `.run()` 仍会绕过；
- 是否不必要地演变为 Runtime Framework 重构。

如认为不需要统一 checked runner，必须给出另一种能够在 strategy business logic 执行前统一 fail-closed 的更小方案。

### 2. Canonical StrategyRunResult Persistence

Revision 2 冻结：

> validated `StrategyRunResult.to_dict()` 作为 canonical strategy result snapshot 写入现有 Product run `reproducibility.json`，不新增数据库 schema / result store。

重点攻击：

- 现有 `BacktestRunWriter` / reproducibility schema 是否允许最小扩展；
- strategy result snapshot 是否应该放在 reproducibility 而非 execution snapshot / config；
- 是否会导致结果包尺寸或 schema compatibility 明显问题；
- legacy run 是否能保持兼容；
- replay/load 是否真的能访问该字段；
- replay 是否必须在 Task07-A 比较 snapshot，还是只要求保存/读取即可，避免提前实现 Task08；
- reason/evidence/diagnostics 是否完整保留且 deterministic；
- 是否错误把 Engine target frame 当 canonical authority。

### 3. 复核前两轮已关闭项没有回归

至少重新确认：

- full snapshot: omitted asset=0, `positions=()`=all cash；
- target_weight 唯一 authority；
- holdings as-of initial snapshot；
- holdings / initial_positions key-union compatibility；
- native target / legacy decisions 不双 SSOT；
- target date 是 strategy target date，timing shift exactly once；
- legacy StrategyBacktestRuntime native target fail-fast；
- non-empty holdings 不被 wrapper 静默丢弃；
- deterministic target serialization；
- priority neutral value 不重新做业务容量决策；
- Account/OMS/Execution scope 没有回流。

## 必查代码

至少：

- `src/qrp_atlas/strategies/models.py`
- `src/qrp_atlas/strategies/validation.py`
- `src/qrp_atlas/strategies/registry.py`
- `src/qrp_atlas/strategies/builtin/cross_section.py`
- `src/qrp_atlas/strategies/declarative/`
- `src/qrp_atlas/backtest/runtime/strategy.py`
- `src/qrp_atlas/backtest/portfolio/strategy.py`
- `src/qrp_atlas/backtest/product/service.py`
- `src/qrp_atlas/backtest/product/cross_section.py`
- `src/qrp_atlas/backtest/product/event.py`
- residual strategy/product paths if any
- `src/qrp_atlas/backtest/results/writer.py`
- `src/qrp_atlas/backtest/results/` reproducibility load/service/replay code
- `docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md`
- `docs/QRP产品蓝图v1.1/03_开发路线图与工作包.md`

## 输出格式

按：

- BLOCKER
- MAJOR
- MINOR
- NIT

每项必须有：

- Evidence
- Impact
- Minimal Fix

如果首轮/二轮问题已经真正关闭，不要重复罗列为问题，可放入 `Resolved Verification`。

最终只给：

```text
READY_FOR_IMPLEMENTATION
```

或

```text
NEEDS_REVISION
```

## 约束

- 只审计，不编码；
- 不创建实现分支；
- 不做文风优化；
- 不发明 System B 07-B/07-C 新业务规则；
- 不扩张 Strategy Framework；
- 不要求 Task07-A 完成 Task08 replay orchestration；
- 没有具体代码/正式规则证据，不升级问题等级。
