# Task07-A — System B Portfolio Target Contract Integration 设计书

> 状态：DESIGN REVISION 2 / 待三次对抗审计
>
> 分支基线：`develop/v1.1`
>
> 任务身份：Task07 的薄 enabling work package。Task07 仍以 **System B 业务闭环** 为唯一主目标；本任务只补齐 System B 为输出完整目标组合而实际暴露出的最小 QRP Common 能力缺口。
>
> Revision 1：冻结 full snapshot、统一 result validation、native target 唯一路由、holdings as-of、legacy runtime fail-fast 与 deterministic serialization。
>
> Revision 2：吸收第二轮审计 `NEEDS_REVISION` 的两个 MAJOR，进一步冻结 **统一 checked strategy runner（input validate → run → result validate）** 与 **canonical StrategyRunResult 写入既有 reproducibility snapshot**；不新增数据库 schema、不改造 Account/Execution、不扩大 Task07-A 产品范围。

---

## 1. 背景与任务定位

QRP v1.1 当前核心链路：

```text
pipeline
→ contracts
→ stock_collections
→ indicators
→ strategies
→ portfolio target
→ production run / replay / result
```

Task05 已完成 System B 新增仓授权结果正式挂载；Task06 已完成 Asset Rank 与 Theme Rank。Task07 首次需要把市场判断、横截面排名、当前持仓状态与组合规则汇合成一个**完整目标组合**。

现有 Strategy Framework 已具备：

- `StrategyDefinition` / `StrategyProtocol` / `StrategyRegistry`；
- ASSET / MARKET 两种 `StrategyInputScope`；
- 参数、required fields、required indicators、indicator requests；
- `StrategyDecision`；
- `StrategyAuthorization`；
- `StrategyRunResult`；
- declarative strategy；
- strategy catalog / product backtest；
- `StrategyDecision -> target_weights -> PortfolioBacktestEngine` 通用适配链。

但 System B 这类复杂策略存在关键断点：

```text
StrategyRunResult
  ├─ decisions
  └─ authorizations

        ↓ 缺少策略原生、typed、完整 portfolio target

Portfolio / Backtest
```

简单 ENTER/HOLD/EXIT 策略可以由下游 Adapter 推导 target；System B 最终组合包含持仓生命周期、加仓次数、容量竞争、权重约束等业务语义，因此 **Portfolio Target 本身属于 System B 策略结果的一部分**，不能依赖通用 Adapter 在策略外部猜测。

Task07-A 只负责为 07-B / 07-C 提供最小、稳定的输入输出边界。

---

## 2. Task07 总原则（冻结）

### 2.1 唯一主目标

> **Task07 以 System B 业务闭环为唯一主目标；为实现该目标暴露出的 QRP Common 能力缺口，仅进行最小、通用、向后兼容扩展，不将 Task07 扩张为独立的 Strategy Framework 重构任务。**

该原则对 Task07-A / B / C 全部生效。

### 2.2 Common 只能被动补齐

正确顺序：

```text
System B 业务需求
→ 暴露 Common 缺口
→ 补最薄、可复用的一层
→ 返回 System B 主线
```

禁止：

```text
发现抽象机会
→ Strategy Framework v2
→ plugin / external / account / OMS 扩建
→ 再回来实现 System B
```

### 2.3 最小、通用、向后兼容

任何 Common 扩展必须同时满足：

1. **最小**：只覆盖 Task07 已真实需要的语义；
2. **通用**：类型/命名不得硬编码 System B 业务知识；
3. **向后兼容**：现有 built-in、declarative strategy、`StrategyDecision` 与既有产品回测路径默认行为不变。

### 2.4 Strategy 与 Execution 严格分层

Task07 稳定边界停在：

```text
strategy result / desired portfolio target
```

System B strategy 负责：

- 谁应该持有；
- 是否继续持有；
- 是否退出；
- 是否允许新增 / 加仓；
- 组合容量竞争；
- desired business target weight / state。

Portfolio / Backtest 负责：

- T+1；
- 停牌；
- 涨跌停；
- 整数手；
- 价格相关现金可实现性；
- 成交成本；
- 现实成交失败；
- realized holdings / capital changes。

**通用 Portfolio Engine 不得包含 System B、Theme Rank、Asset Rank、MA5 两日退出、第二次加仓等业务知识。**

---

## 3. Task07 三包关系

```text
07-A：System B Portfolio Target Contract Integration
     定义“输入输出长什么样”

             ↓

07-B：System B Holding / Entry / Exit Policy
     决定“业务上想持有什么”

             ↓

07-C：System B Portfolio Constraint Resolution & Final Target
     决定“约束后最终持有什么”
```

07-A 是薄 enabling task；07-B / 07-C 才是 System B 业务核心。

---

## 4. Task07-A 目标

Task07-A 只完成：

1. `StrategyRunResult` 增加可表达**完整 desired portfolio** 的 typed contract；
2. `StrategyInput` 增加 System B 后续实际需要的**最小 typed initial holdings**；
3. 建立统一 QRP-owned checked runner：`input validation → strategy.run → result validation`；
4. 建立 `StrategyRunResult → target frame` 唯一最高层路由；
5. native target 成为 authority 时不再从 decisions 二次推导 target；
6. canonical `StrategyRunResult` 进入现有 Product/replay reproducibility evidence；
7. 保持旧简单策略、declarative strategy 和旧 decisions 路径默认行为不变；
8. 不提前实现 07-B / 07-C 业务规则；
9. 不为了 seeded holdings 改造 PortfolioBacktestEngine / Account 模型。

---

## 5. 最小 Common Contract（冻结）

### 5.1 Native Portfolio Target

新增公共类型：

```python
@dataclass(frozen=True)
class StrategyPortfolioTargetPosition:
    asset_id: str
    target_weight: float
    reason_code: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class StrategyPortfolioTarget:
    trade_date: str
    strategy_code: str
    strategy_version: str
    positions: tuple[StrategyPortfolioTargetPosition, ...]
    diagnostics: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]: ...
```

`StrategyRunResult` 增加：

```python
portfolio_targets: tuple[StrategyPortfolioTarget, ...] = ()
```

公共类型必须从 `qrp_atlas.strategies` 暴露。

### 5.2 Full Snapshot 唯一语义

> **每个 `StrategyPortfolioTarget` 都是该 `trade_date` 的完整 desired portfolio state，不是增量 patch。**

唯一解释：

- `positions` 中列出的资产具有对应目标权重；
- 未出现在 `positions` 的任何当前持仓资产，其 desired target weight = 0；
- `positions=()` = desired portfolio 全现金；
- Adapter / persistence / replay 不得解释为“省略即保持”；
- 策略无需为所有历史退出资产长期保留显式 0 行；
- Engine-facing 转换如需要显式 0，只能依据完整 snapshot 语义确定性生成。

### 5.3 `target_weight` 是唯一 Strategy Target authority

v1.1 不引入并列权威 `target_quantity`。

要求：

- finite；
- `0 <= target_weight <= 1`；
- 单 target 总权重 `<= 1 + tolerance`；
- residual = cash；
- quantity / lot / price-dependent cash feasibility 下沉 Portfolio / Backtest。

### 5.4 Target 日期语义

`StrategyPortfolioTarget.trade_date` = **strategy target/signal date**，不是最终 execution date。

Canonical：

- exact `YYYY-MM-DD`；
- 无 time / timezone；
- 同 result 内 target date 唯一；
- date ASC；
- native target converter 不做任何日期 shift；
- 调用链既有 timing 层只 shift 一次。

### 5.5 Authorization / Decision / Target 职责

```text
StrategyAuthorization = 策略级许可/否决
StrategyDecision      = 资产级判断与解释
StrategyPortfolioTarget = 组合级最终 desired state
```

三者可共存，但 portfolio authority 唯一：

```text
portfolio_targets 非空 → authority = native targets
portfolio_targets 为空 → authority = legacy decisions adapter
```

native target run 中 decisions 只用于 explanation / audit。

---

## 6. Deterministic Serialization（冻结）

### 6.1 排序

```text
portfolio_targets: trade_date ASC
positions: asset_id ASC
```

禁止依赖输入顺序、权重或 reason 排序。

### 6.2 Target evidence 类型域

新增 target evidence 只允许 JSON-compatible tree：

- null / bool / string / integer / finite float；
- list / tuple → JSON list；
- string-keyed Mapping，递归规范化并按 key 排序。

拒绝：

- NaN / Inf；
- set / frozenset；
- 非字符串 key；
- pandas / NumPy / datetime 等未显式规范化对象；
- 任意不可 JSON 序列化对象。

不借 Task07-A 追溯重构全部 legacy `StrategyDecision.evidence`。

### 6.3 Canonical JSON

新增类型提供稳定 `to_dict()`；`StrategyRunResult.to_dict()` 必须包含 `portfolio_targets`。

Canonical JSON 至少满足：

```text
UTF-8
sort_keys=True
allow_nan=False
stable separators
```

优先复用既有 deterministic JSON helper；不得建立第二套业务 SSOT。

---

## 7. 最小 typed holdings（冻结）

### 7.1 类型

```python
@dataclass(frozen=True)
class StrategyHoldingState:
    asset_id: str
    current_weight: float
    entry_count: int
    first_entry_date: str | None = None
    last_entry_date: str | None = None

    def to_dict(self) -> dict[str, Any]: ...
```

`StrategyInput` 增加：

```python
holdings: Mapping[str, StrategyHoldingState] = field(default_factory=dict)
holdings_as_of_date: str | None = None
```

只进入 System B Task07 已证明需要的字段，不引入 quantity/cost/cash/account/order/PNL 等 Broker/Account 语义。

### 7.2 唯一 as-of 语义

> **`holdings` 是本次 `prepared_data` 第一个交易日开始评估之前的 initial holding snapshot。**

规则：

- holdings 非空 → `holdings_as_of_date` 必填；
- exact `YYYY-MM-DD`；
- `holdings_as_of_date < min(prepared_data.trade_date)`；
- Common 不判断是否上一合法交易日，不查询 calendar；
- PIT input preparation 层负责提供合法历史快照；
- 多日 prepared_data 中 state progression 由策略确定性推进；
- 07-A 不建设 date-keyed account ledger。

### 7.3 holdings 只包含当前持仓

Mapping 中出现的资产必须：

- `current_weight > 0` 且 finite；
- `entry_count >= 1` 且非 bool；
- key == state.asset_id；
- entry dates 若存在均为 `YYYY-MM-DD`；
- entry dates <= holdings_as_of_date；
- first <= last。

未出现的资产 = 当前未持有。

### 7.4 Legacy `initial_positions` 兼容

保留现有：

```python
initial_positions: Mapping[str, bool]
```

若两者同时存在，对 key union 冻结：

```text
legacy_held = initial_positions.get(asset_id, False)
typed_held  = asset_id in holdings
legacy_held must equal typed_held
```

冲突必须在 **strategy execution 前** fail-closed。

---

## 8. 统一 Checked Strategy Runner（Revision 2 冻结）

### 8.1 为什么必须有统一入口

仅有 `validate_strategy_result()` 不足，因为 holdings、as-of、legacy conflict 等属于 **StrategyInput 合同**，必须在业务策略执行前拒绝。

因此所有 QRP-owned strategy execution 的正式顺序冻结为：

```text
raw StrategyInput
        ↓
validate_and_normalize_strategy_input()
        ↓
normalized StrategyInput
        ↓
strategy.run(normalized_input)
        ↓
validate_strategy_result()
        ↓
validated StrategyRunResult
```

### 8.2 最小公共 helper

允许新增一个很薄的 QRP Common helper，例如：

```python
run_strategy_checked(
    strategy: StrategyProtocol,
    strategy_input: StrategyInput,
) -> StrategyRunResult
```

其职责严格限定为：

```text
1. validate + canonicalize input
2. invoke strategy.run exactly once
3. validate + canonicalize result
4. return validated result
```

它不是新 Runtime Framework，不做：

- 数据库查询；
- indicator preparation；
- portfolio construction；
- execution；
- persistence；
- System B 业务判断。

### 8.3 Input validator 返回规范化 StrategyInput

现有 `validate_strategy_input()` 当前主要返回 canonical DataFrame。Revision 2 冻结的是**能力语义**，具体实现可最小选择：

- 向后兼容保留现有低层 validator，并新增 `validate_and_normalize_strategy_input()`；或
- 在不破坏现有调用方的前提下扩展现有 helper。

正式 QRP-owned runner 最终必须得到一个**规范化后的 `StrategyInput`**，其中：

- prepared_data canonical；
- parameters 保持调用方已 resolve 的值；
- initial_positions 已验证；
- holdings / holdings_as_of_date 已验证；
- holdings 与 legacy initial_positions 冲突已拒绝。

### 8.4 QRP-owned 调用点不得绕过

至少覆盖：

- `StrategyRegistry.run()`；
- `StrategyBacktestRuntime`；
- `run_strategy_portfolio_backtest()`；
- Product Service 直接 strategy instance 路径；
- cross-sectional product path；
- event product path；
- residual / 其他当前 QRP-owned strategy runner；
- 后续 07-B / 07-C 新增正式 runner。

实现 Agent 必须先 grep/审计所有 QRP-owned `.run(StrategyInput(...))` / 等价调用，确保不存在正式路径绕过 checked runner。

不要求技术上拦截外部 Python 用户直接调用任意 strategy object `.run()`；该行为不属于 QRP-owned product contract。

---

## 9. Result Validation（冻结）

统一：

```python
validate_strategy_result(
    definition: StrategyDefinition,
    result: StrategyRunResult,
) -> StrategyRunResult
```

至少校验：

- result definition code/version 与被执行 strategy definition 一致；
- target strategy code/version 与 result definition 一致；
- target date exact、唯一、ASC；
- position asset_id 非空、唯一、ASC；
- target_weight finite/range/sum；
- evidence JSON-compatible；
- diagnostics string-only；
- canonical serialization 可重复。

非法 result 在任何 Adapter / persistence / Engine 前 fail-closed。

---

## 10. Strategy Result → Target Frame 唯一路由（冻结）

建立唯一最高层入口，例如：

```python
strategy_result_to_target_weights(
    strategy_result: StrategyRunResult,
    *,
    legacy portfolio args...
) -> pd.DataFrame
```

唯一分支：

```text
if strategy_result.portfolio_targets:
    native full snapshots → target frame
else:
    existing strategy_decisions_to_target_weights(...)
```

`strategy_decisions_to_target_weights()` 保留为 legacy decisions 专用低层实现，不再作为 Product / Portfolio 最高层入口。

Engine-facing frame 继续为：

```text
trade_date
asset_id
target_weight
priority
```

native target 规则：

- date 原样保持 strategy target date；
- target_weight 原样来自 native contract；
- asset_id ASC；
- `priority` 仅使用稳定 neutral value 适配既有 frame；不得重新做 System B 容量决策；
- reason/evidence/diagnostics 不塞进 lossy Engine frame；
- native target 非空时绝不调用 decisions adapter。

---

## 11. Backtest / Product 接入边界（冻结）

### 11.1 Portfolio Product Path

正式链：

```text
prepare data / resolve params
→ construct StrategyInput
→ run_strategy_checked()
→ strategy_result_to_target_weights()
→ existing timing shift exactly once
→ PortfolioBacktestEngine
```

native 与 legacy strategy 最终复用同一个 Portfolio Engine。

### 11.2 Legacy `StrategyBacktestRuntime`

这是 ENTER/HOLD/EXIT trade-level runtime，不是完整 portfolio target runtime。

若 checked result 含 native `portfolio_targets`：

> **立即 fail-fast unsupported-path。**

禁止继续遍历空/旧 decisions 后返回“无交易成功”。

不借 07-A 把 legacy runtime 重构成第二个 Portfolio Engine。

### 11.3 typed holdings 产品边界

- 07-A 保证底层 `StrategyInput` 正式支持 typed holdings；
- 现有 PortfolioBacktestEngine 保持 cash-only initial account；
- QRP-owned wrapper 不得接收 holdings 后静默丢弃；
- 未正式支持 seeded holdings 的 API 要么不暴露 holdings 参数，要么对非空 holdings fail-closed；
- 07-B/C 单元与策略集成测试可直接构造 typed StrategyInput；
- Task08 可从空初始组合开始历史运行；
- Task09 再处理 daily production holdings orchestration；
- 若 07-B/C 证明正式 runner 必须接收 non-empty holdings，只加“传递到 StrategyInput”的薄参数，不扩 Portfolio Engine seeded-account semantics。

---

## 12. Canonical Strategy Result Persistence（Revision 2 冻结）

### 12.1 Rich strategy result 不能只存在内存

`StrategyPortfolioTargetPosition.reason_code/evidence`、target diagnostics、原始 target signal snapshot 都属于 **strategy result facts**。

Engine target frame 是有意的 lossy projection，不能成为 replay / audit 的唯一存档。

因此 Revision 2 冻结：

> **凡 Product/backtest product path 产生 canonical `StrategyRunResult`，必须将其 canonical `to_dict()` snapshot 写入现有 reproducibility evidence。**

### 12.2 复用现有 `reproducibility.json`

Task07-A 不新增数据库表，不新建第二套 result store。

优先复用现有 Product run 的 `reproducibility.json`，在其中增加稳定 strategy result snapshot 字段，例如：

```json
{
  "strategy_result": {
    "definition": "...existing canonical shape...",
    "parameters": {},
    "decisions": [],
    "authorizations": [],
    "portfolio_targets": [],
    "diagnostics": []
  }
}
```

具体 key 命名可按现有 writer/reproducibility schema 风格最小确定，但必须满足：

- snapshot = validated canonical `StrategyRunResult.to_dict()`；
- 写入发生在 result 已 validate 后；
- 不从 Engine frame 反推 rich strategy result；
- replay/load 能读取该 snapshot；
- replay 重新运行后可以对 canonical strategy result 做确定性一致性验证或至少纳入 reproducibility evidence；
- legacy run 若没有 native target，仍可写同一统一 shape，不要求另建 legacy schema。

### 12.3 不把 persistence 变成新产品系统

07-A 只允许对现有 `BacktestRunWriter` / reproducibility snapshot 做最小扩展。

禁止：

- 新 strategy result 数据库；
- 新 audit service；
- broker/execution result 混入 strategy result；
- Task08 replay orchestration 重写；
- Task09 production state store 提前建设。

---

## 13. Task07-A 不负责的 System B 业务规则

### 07-B

- authorization 如何约束新增仓；
- Theme Rank / Asset Rank 如何参与选择与比较；
- 新候选相对已有持仓评分门槛；
- 已有持仓每日重评；
- 身份变化不得机械触发卖出；
- 两个连续实际交易日收盘低于 MA5 的退出；
- 第二次加仓资格；
- 严重异动等业务判断。

### 07-C

- 单次 1/8；
- 单票最多两次；
- 计划 25%；
- 单票 <= 30%；
- 最多 6 只；
- 组合容量竞争；
- 退出释放容量；
- 同日 exit/add 策略级顺序；
- 最终完整 target snapshot。

07-A 只提供接口，不预实现这些规则。

---

## 14. PIT / Replay Determinism

必须满足：

- target date canonical；
- strategy code/version 明确；
- target/position ordering 固定；
- evidence canonical；
- holdings as-of 明确；
- strategy run 不主动查询“当前最新数据库状态”；
- 同 prepared input + params + initial holdings → 同 StrategyRunResult；
- checked runner 的 input/output validation 顺序唯一；
- timing shift 只执行一次；
- canonical StrategyRunResult 被写入 reproducibility evidence，而不是只保存 lossy target frame。

---

## 15. Validation 细则

### Input

- prepared_data 按现有 scope 规则 canonical；
- required fields / indicators 完整；
- identity 唯一；
- holdings state 合法；
- holdings as-of 合法；
- initial_positions 与 holdings 无冲突。

### Result

- definition/version 一致；
- target dates 唯一、canonical、ASC；
- positions asset_id 唯一、ASC；
- weights finite/range/sum；
- evidence JSON-compatible；
- diagnostics stable；
- full snapshot / serialization contract 合法。

---

## 16. 测试要求

Task07-A 至少覆盖：

1. legacy built-in 无 holdings/targets 行为不变；
2. declarative strategy 行为不变；
3. target/holding public exports 稳定；
4. `StrategyRunResult.to_dict()` 含 canonical targets；
5. `positions=()` = all cash；
6. omitted asset = target 0，无 patch interpretation；
7. duplicate asset/date fail-closed；
8. invalid weight / NaN / Inf fail-closed；
9. invalid evidence fail-closed；
10. target ordering 固定；
11. strategy/version mismatch fail-closed；
12. holdings/as-of validation；
13. holdings + initial_positions conflict 在 strategy.run 前 fail-closed；
14. `run_strategy_checked()` 固定顺序：input validate → run once → result validate；
15. Registry/runtime/portfolio/Product/cross-section/event/residual 等 QRP-owned paths 不绕过 checked runner；
16. native target 存在时不调用 decisions adapter；
17. native target 为空时 legacy adapter 行为不变；
18. Product native target 只发生一次 timing shift；
19. Legacy StrategyBacktestRuntime native target fail-fast；
20. wrapper 不静默丢弃 non-empty holdings；
21. Product result 的 existing reproducibility snapshot 包含 canonical `strategy_result`；
22. rich target reason/evidence/diagnostics 经 write/load 后保持 canonical；
23. replay/reproducibility 测试能读取 strategy result snapshot；
24. 同输入重复 run + serialization 输出一致；
25. full regression 通过。

---

## 17. 允许修改的典型区域

预期：

```text
src/qrp_atlas/strategies/models.py
src/qrp_atlas/strategies/validation.py
src/qrp_atlas/strategies/__init__.py
src/qrp_atlas/strategies/registry.py
src/qrp_atlas/backtest/runtime/strategy.py
src/qrp_atlas/backtest/portfolio/strategy.py
src/qrp_atlas/backtest/product/service.py
src/qrp_atlas/backtest/product/cross_section.py   # 若存在直接 run 绕过
src/qrp_atlas/backtest/product/event.py           # 若存在直接 run 绕过
src/qrp_atlas/backtest/results/writer.py          # existing reproducibility snapshot minimal extension
src/qrp_atlas/backtest/results/                   # load/replay evidence tests as needed
相关 tests
```

允许新增：

- 一个薄 `run_strategy_checked()` helper；
- 一个薄 result→target adapter；
- 必要 canonical serialization helper。

不得大范围搬迁目录或重构 Strategy Framework。

---

## 18. 明确禁止的 Scope Creep

禁止：

- Strategy Framework v2；
- dynamic plugin loader；
- external strategy RPC；
- PLUGIN / EXTERNAL runtime；
- 多账户 / Broker Position Model；
- seeded broker account / full account ledger；
- OMS / order plan / execution extension；
- 多资产通用 portfolio domain；
- 多币种 / margin / short；
- 新 strategy result database/store；
- System B 07-B / 07-C 业务规则；
- Task08 replay orchestration 重构；
- Task09 production orchestration。

如发现“顺手可做”，默认不做，除非阻塞本任务 DoD。

---

## 19. DoD

Task07-A 完成条件：

1. `StrategyRunResult` 能表达 typed full-snapshot Portfolio Target；
2. full snapshot / omitted asset / all-cash / residual cash 语义唯一；
3. target date、ordering、serialization canonical；
4. typed initial holdings 有明确 as-of 边界；
5. legacy initial_positions 兼容规则确定；
6. 所有 QRP-owned 正式策略运行采用 checked 顺序：input validate → run → result validate；
7. input conflict 在业务 strategy.run 前 fail-closed；
8. 有唯一 result→target highest-level router；
9. native target 与 decisions 不形成双 SSOT；
10. Product Portfolio path 可消费 native target，timing shift 仅一次；
11. legacy StrategyBacktestRuntime 不静默忽略 native target；
12. non-empty holdings 不被 wrapper 静默丢弃；
13. canonical `StrategyRunResult` 写入现有 reproducibility evidence；
14. rich target evidence 不因 Engine lossy frame 丢失；
15. replay/result load 能访问 canonical strategy result snapshot；
16. existing built-in / declarative strategy 默认行为不变；
17. 通用 Portfolio / Backtest 无 System B 业务知识；
18. 未引入 Account / OMS / Broker / 新 result store scope；
19. targeted tests + full regression 通过；
20. 三次对抗审计无 BLOCKER / MAJOR 后才允许实现。

---

## 20. 审计处置记录

### 第一轮

**BLOCKER**

- B-1 full snapshot 语义未冻结 → `omitted asset=0`, `positions=()=all cash`。
- B-2 result fail-closed 无统一入口 → `validate_strategy_result()` + QRP-owned run 后统一校验。

**MAJOR**

- native target route/date → 唯一 result→target 路由；converter 不 shift。
- holdings lifecycle → `holdings_as_of_date` initial snapshot。
- initial_positions conflict → key-union bool equivalence。
- deterministic serialization → target JSON domain + canonical ordering。
- legacy runtime ignore native target → fail-fast。
- Product/Portfolio holdings gap → 底层 typed input + wrapper 禁止静默丢弃，不扩 seeded account。

### 第二轮

**MAJOR M-1：holdings execution-before validation 无统一入口**

Revision 2 处置：

```text
validate_and_normalize_strategy_input
→ strategy.run exactly once
→ validate_strategy_result
```

统一收敛为 `run_strategy_checked()` 语义；所有 QRP-owned strategy execution path 必须接入。

**MAJOR M-2：Product/replay 未持久化 canonical StrategyRunResult**

Revision 2 处置：

- validated `StrategyRunResult.to_dict()` 写入现有 `reproducibility.json`；
- Engine target frame 继续只是 lossy projection；
- 不新增数据库 schema / result store；
- replay/load 测试必须覆盖 canonical strategy result snapshot。

---

## 21. Task07-A 完成后的架构结果

Task07-A 完成只意味着：

```text
QRP Strategy Runtime
可以用统一 input/result contract
承载复杂策略原生输出完整 Portfolio Target
并把 canonical strategy result 纳入现有 replay evidence
```

真正闭环仍需：

```text
07-A contract integration
        ↓
07-B System B business policy
        ↓
07-C final constrained target
        ↓
System B 业务闭环
+
QRP 复杂策略挂载路径第一次完整验证
```

“QRP 复杂策略挂载能力闭环”是 Task07 完成后的**架构成果**，不是 Task07 额外建设独立平台的目标。
