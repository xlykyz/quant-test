# Task07-A 对抗审计启动说明

请基于远端 `develop/v1.1` 最新状态，对以下设计执行对抗审计：

- `docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md`

## 审计目标

不是优化文风，也不是扩展架构，而是验证 Task07-A 是否在以下原则下成立：

> Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。

## 必查代码与文档

至少核对：

- `src/qrp_atlas/strategies/models.py`
- `src/qrp_atlas/strategies/protocol.py`
- `src/qrp_atlas/strategies/registry.py`
- `src/qrp_atlas/strategies/validation.py`
- `src/qrp_atlas/strategies/declarative/`
- `src/qrp_atlas/backtest/runtime/strategy.py`
- `src/qrp_atlas/backtest/portfolio/strategy.py`
- `src/qrp_atlas/backtest/product/service.py`
- `src/qrp_atlas/backtest/product/catalog.py`
- `docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md`
- `docs/QRP产品蓝图v1.1/03_开发路线图与工作包.md`
- Task05 / Task06 已关闭设计与 walkthrough 中涉及 Strategy Result、Authorization、Rank 的部分

## 重点攻击问题

1. native `StrategyPortfolioTarget` 是否确实必要；若认为现有 `StrategyDecision -> target_weights` 足够，必须证明不会把 System B 业务规则泄漏到通用 Adapter。
2. `target_weight` 是否应作为唯一 Strategy Target authority；是否存在必须 `target_quantity` 的正式规则证据。
3. proposed holdings typed state 是否过度设计或字段不足。
4. `initial_positions` 与新 holdings 的兼容策略是否安全、确定。
5. native target 与 decision-derived target 是否会形成双 SSOT。
6. 是否破坏现有 built-in / declarative strategy / product backtest。
7. 是否把整数手、现金、停牌、涨跌停、T+1、成交等 Portfolio/Backtest 责任错误提升到 Strategy Contract。
8. 是否存在 PIT / replay nondeterminism。
9. 是否提前实现了 07-B / 07-C 的业务规则。
10. 是否存在任何没有被 System B Task07 实际需求证明的 Common 抽象或 scope creep。

## 输出格式

按以下等级输出：

- BLOCKER
- MAJOR
- MINOR
- NIT

每一项必须包含：

- Evidence：具体文件 / 代码 / 正式规则依据
- Impact：若不修复会导致什么
- Minimal Fix：最小修正方案

最后给出结论：

```text
READY_FOR_IMPLEMENTATION
或
NEEDS_REVISION
```

约束：

- 不要开始编码；
- 不要创建实现分支；
- 不要把审计扩张成 Strategy Framework 全面重构；
- 没有证据不要脑补；
- 优先识别 scope creep、双 SSOT、职责泄漏、向后兼容和 replay determinism 问题。
