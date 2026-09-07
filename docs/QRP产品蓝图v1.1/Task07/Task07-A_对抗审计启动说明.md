# Task07-A 最终对抗审计启动说明

请基于远端 `develop/v1.1` 最新状态，对以下 **Revision 3** 执行最终对抗审计：

- `docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md`

前三轮均曾给出 `NEEDS_REVISION`。本轮仍保持同等攻击强度；只报告仍成立的问题或 Revision 3 新引入的问题。

## 总原则

> Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。

## 本轮首要复核：第三轮 M-3 EventFrame

Revision 3 已冻结：

```text
checked runner
→ 先选择既有正确 input contract
→ validate/normalize
→ strategy.run exactly once
→ validate result
```

普通 ASSET/MARKET 仍沿用现有 scope validator；`event_drift_basic` 使用 EventFrame 专用 normalizer，以 `available_trade_date` 为 evaluation date，不要求 `trade_date`。

请重点攻击：

1. EventFrame normalizer 是否与当前 `event_drift_basic`、event product 和测试真实语义一致；
2. deterministic sort 是否足够且不会改变现有业务结果；
3. 不对 `(available_trade_date, ticker)` 强制唯一是否正确；
4. event strategy 当前 enriched-candidate 去重是否确实应留在 strategy 内；
5. `holdings_as_of_date < min(available_trade_date)` 是否是正确、最小的 event holdings 边界；
6. checked runner 如何选择 EventFrame normalizer 是否能用薄 dispatch/callable 实现，而不需要新增 EVENT InputScope / validator registry；
7. event 产品是否仍保持 `available_trade_date` 入场、无额外 next-open shift；
8. 是否存在其他既有正式策略输入形态也会被统一 checked runner 错误套用 ASSET/MARKET validator。

## 复核前两轮关闭项

重新确认没有回归：

- full snapshot：omitted asset=0，`positions=()`=all cash；
- `target_weight` 唯一 Strategy target authority；
- holdings as-of；
- holdings / legacy initial_positions 冲突 fail-closed；
- checked runner 的 input-before-run / result-after-run 顺序；
- native target / legacy decisions 不形成双 SSOT；
- native target date 不在 converter shift，产品 timing 只 shift 一次；
- legacy StrategyBacktestRuntime native target fail-fast；
- non-empty holdings 不被 wrapper 静默丢弃；
- deterministic serialization；
- canonical `StrategyRunResult.to_dict()` 写入既有 `reproducibility.json` 并可 load；
- Account / OMS / Execution / 新 result store / validator framework 没有 scope 回流。

## 必查代码

至少核对：

- `src/qrp_atlas/strategies/models.py`
- `src/qrp_atlas/strategies/validation.py`
- `src/qrp_atlas/strategies/registry.py`
- `src/qrp_atlas/strategies/builtin/event_drift.py`
- `src/qrp_atlas/strategies/declarative/`
- `src/qrp_atlas/backtest/runtime/strategy.py`
- `src/qrp_atlas/backtest/portfolio/strategy.py`
- `src/qrp_atlas/backtest/product/service.py`
- `src/qrp_atlas/backtest/product/cross_section.py`
- `src/qrp_atlas/backtest/product/event.py`
- `src/qrp_atlas/backtest/product/timing.py`
- `src/qrp_atlas/backtest/results/writer.py`
- `src/qrp_atlas/backtest/results/loader.py`
- `tests/strategies/test_event_drift_basic.py`
- 相关 product/replay tests

## 输出

按：

- BLOCKER
- MAJOR
- MINOR
- NIT

每项必须包含 Evidence / Impact / Minimal Fix。

最后只给一个结论：

```text
READY_FOR_IMPLEMENTATION
```

或

```text
NEEDS_REVISION
```

约束：

- 只审计，不编码；
- 不创建实现分支；
- 没有证据不要脑补；
- 不因为已经第四轮就降低标准；
- 不把审计扩张成 Strategy Framework 重构。
