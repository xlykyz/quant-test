# Task07-A 对抗审计结论

> 状态：`READY_FOR_IMPLEMENTATION`
>
> 审计基线：`develop/v1.1@d523818e911b1228e8f889314f53139da694b364`

## 结论

Task07-A — System B Portfolio Target Contract Integration 经过多轮对抗审计后，最终结果为：

```text
BLOCKER  0
MAJOR    0
MINOR    0
NIT      0

READY_FOR_IMPLEMENTATION
```

正式批准进入实现。

## 冻结原则

> Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。

实现阶段不得重新打开已经通过审计冻结的设计语义，除非代码事实证明存在新的 BLOCKER；若发现新的非阻塞优化机会，默认推迟，不扩大 Task07-A scope。

## 已冻结的核心合同

- native `StrategyPortfolioTarget` 是复杂策略的一等结果；
- target 是 full snapshot：omitted current asset = 0，`positions=()` = all cash；
- `target_weight` 是 Strategy Target 唯一业务权威；
- native target 与 legacy decisions target 只有一个 authority；
- typed holdings 是策略时间轴开始前的 initial snapshot，并有明确 as-of 语义；
-所有 QRP-owned 正式运行路径遵循 checked runner 顺序：正确输入合同选择/规范化 → `strategy.run()` exactly once → result validation；
- EventFrame 使用既有 `available_trade_date` 评估时间语义，不新增泛化 `EVENT` InputScope；
- canonical `StrategyRunResult` 写入既有 reproducibility evidence；
- legacy trade runtime 遇到 native target 必须 fail-fast；
- timing shift 只发生一次；
- Portfolio / Backtest 不得吸收 System B 业务规则；
- 不引入 Account / OMS / Broker / plugin framework / new result store scope。

## 实现准入

实现 Agent 可以创建 Task07-A 实现分支并编码。完成后至少必须：

1. 按设计书实现全部 contract / checked runner / routing / persistence 要求；
2. targeted tests 全部通过；
3. full regression 通过；
4. 输出修改文件、关键实现、测试证据与未实现项；
5. 不开始 Task07-B / Task07-C。
