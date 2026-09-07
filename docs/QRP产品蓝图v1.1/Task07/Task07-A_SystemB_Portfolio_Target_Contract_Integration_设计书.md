# Task07-A — System B Portfolio Target Contract Integration 设计书

> 状态：DESIGN REVISION 3 / 待最终对抗审计
>
> 分支基线：`develop/v1.1`
>
> 任务身份：Task07 的薄 enabling work package。Task07 仍以 **System B 业务闭环** 为唯一主目标；本任务只补齐 System B 为输出完整目标组合而实际暴露出的最小 QRP Common 能力缺口。
>
> Revision 1：冻结 full snapshot、result validation、native target 唯一路由、holdings as-of、legacy runtime fail-fast 与 deterministic serialization。
>
> Revision 2：冻结 checked strategy runner（input validate → run once → result validate）与 canonical `StrategyRunResult` 写入既有 reproducibility snapshot。
>
> Revision 3：吸收第三轮审计唯一 MAJOR，冻结 **EventFrame 专用输入规范化合同**；checked runner 在执行前按既有正式输入形态选择 validator，不把 EventFrame 强塞进普通 ASSET `(ticker, trade_date)` 合同，也不新增泛化 Strategy Framework / 新 InputScope。

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

Task05 已完成 System B 新增仓授权正式挂载；Task06 已完成 Asset Rank 与 Theme Rank。Task07 首次需要把市场判断、横截面排名、当前持仓状态与组合规则汇合成一个**完整目标组合**。

现有 Strategy Framework 已具备：

- `StrategyDefinition` / `StrategyProtocol` / `StrategyRegistry`；
- ASSET / MARKET `StrategyInputScope`；
- 参数、required fields、required indicators、indicator requests；
- `StrategyDecision` / `StrategyAuthorization` / `StrategyRunResult`；
- declarative strategy；
- strategy catalog / product backtest；
- `StrategyDecision → target_weights → PortfolioBacktestEngine` 通用链路；
- cross-sectional / event 等已有产品化特殊路径。

System B 的关键缺口是：

```text
StrategyRunResult
  ├─ decisions
  └─ authorizations

        ↓ 缺少策略原生、typed、完整 portfolio target

Portfolio / Backtest
```

简单 ENTER/HOLD/EXIT 策略可以由通用 Adapter 推导 target；System B 最终组合包含持仓生命周期、加仓次数、容量竞争、权重约束等业务语义，因此 **Portfolio Target 本身属于 System B 策略结果的一部分**，不能由通用 Adapter 在策略外部猜测。

Task07-A 只负责给 07-B / 07-C 提供最小、稳定的输入输出边界。

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

1. **最小**：只覆盖 Task07 已真实需要，或统一 checked runner 为保持现有正式产品路径不回归而必须承认的既有输入语义；
2. **通用**：新 target / holdings 类型不得硬编码 System B 业务知识；
3. **向后兼容**：existing built-in、declarative、cross-sectional、event 及 legacy decisions 产品路径默认行为不变。

### 2.4 Strategy 与 Execution 严格分层

Task07 稳定边界停在：

```text
strategy result / desired portfolio target
```

System B strategy 负责：

- 谁应该持有；
- 是否继续持有 / 退出；
- 是否允许新增 / 加仓；
- 组合容量竞争；
- desired business target weight / state。

Portfolio / Backtest 负责：

- T+1；
- 停牌 / 涨跌停；
- 整数手；
- 价格相关现金可实现性；
- 成交成本 / 现实成交失败；
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

1. `StrategyRunResult` 增加 typed、完整 desired portfolio target；
2. `StrategyInput` 增加 07-B / 07-C 实际需要的最小 typed initial holdings；
3. 建立统一 QRP-owned checked runner：**选择正确输入合同 → input validate/normalize → strategy.run exactly once → result validate**；
4. 建立 `StrategyRunResult → target frame` 唯一最高层路由；
5. native target 为 authority 时不从 decisions 二次推导 target；
6. canonical `StrategyRunResult` 进入既有 Product/replay reproducibility evidence；
7. existing simple / declarative / cross-sectional / event / legacy decisions 路径默认行为不变；
8. 不提前实现 07-B / 07-C 业务规则；
9. 不为了 seeded holdings 改造 PortfolioBacktestEngine / Account 模型。

---

## 5. Native Portfolio Target Contract（冻结）

### 5.1 公共类型

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

公共类型从 `qrp_atlas.strategies` 导出。

### 5.2 Full Snapshot 唯一语义

> **每个 `StrategyPortfolioTarget` 都是该 `trade_date` 的完整 desired portfolio state，不是增量 patch。**

冻结：

- `positions` 中资产具有对应 desired weight；
- 未出现的任何当前持仓资产 desired weight = 0；
- `positions=()` = desired portfolio 全现金；
- Adapter / persistence / replay 不得解释为“省略即保持”；
- 策略无需为全部历史退出资产永久保留 0 行；
- Engine-facing 转换若需要显式 0，只能依据 full-snapshot 语义确定性生成。

### 5.3 `target_weight` 唯一权威

v1.1 不引入并列 `target_quantity`。

要求：

- finite；
- `0 <= target_weight <= 1`；
- 单 target 总权重 `<= 1 + tolerance`；
- residual = cash；
- quantity / lot / price-dependent feasibility 下沉 Portfolio / Backtest。

### 5.4 Target 日期语义

`StrategyPortfolioTarget.trade_date` = **strategy target/signal date**，不是 execution date。

Canonical：

- exact `YYYY-MM-DD`；
- 无 time / timezone；
- 同 result 内 target date 唯一；
- date ASC；
- native converter 不做日期 shift；
- 调用链既有 timing 层只 shift 一次。

### 5.5 Authorization / Decision / Target 职责

```text
StrategyAuthorization   = 策略级许可/否决
StrategyDecision        = 资产级判断与解释
StrategyPortfolioTarget = 组合级最终 desired state
```

portfolio authority 唯一：

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

只允许 JSON-compatible tree：

- null / bool / string / integer / finite float；
- list / tuple → JSON list；
- string-keyed Mapping，递归规范化并按 key 排序。

拒绝 NaN / Inf、set、非字符串 key、未规范化 pandas/NumPy/datetime 对象及任意不可 JSON 序列化对象。

不借 Task07-A 追溯重构全部 legacy `StrategyDecision.evidence`。

### 6.3 Canonical JSON

新增类型提供稳定 `to_dict()`；`StrategyRunResult.to_dict()` 必须包含 `portfolio_targets`。

至少：

```text
UTF-8
sort_keys=True
allow_nan=False
stable separators
```

优先复用既有 deterministic JSON helper，不建立第二套业务 SSOT。

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

不引入 quantity / cost / cash / account / order / PnL 等 Broker/Account 语义。

### 7.2 唯一 as-of 语义

> **`holdings` 是本次策略评估时间轴开始前的 initial holding snapshot。**

对于普通 bar / market panel：

- holdings 非空 → `holdings_as_of_date` 必填；
- exact `YYYY-MM-DD`；
- `holdings_as_of_date < min(canonical strategy evaluation date)`；
- Common 不查询 calendar，不判断是否上一合法交易日；
- PIT preparation 层负责合法历史快照；
- 多日 input 中 holding state progression 由策略确定性推进；
- 07-A 不建设 date-keyed account ledger。

**注意：evaluation date 由所选 input contract 定义。** 对普通 ASSET/MARKET panel 是 `trade_date`；EventFrame 是 `available_trade_date`。不得把 holdings as-of 校验硬编码为 DataFrame 必须存在 `trade_date`。

### 7.3 holdings 只包含当前持仓

Mapping 中资产必须：

- `current_weight > 0` 且 finite；
- `entry_count >= 1` 且非 bool；
- key == `state.asset_id`；
- entry dates 若存在为 `YYYY-MM-DD`；
- entry dates <= `holdings_as_of_date`；
- first <= last。

未出现资产 = 当前未持有。

### 7.4 Legacy `initial_positions` 兼容

保留：

```python
initial_positions: Mapping[str, bool]
```

两者同时存在时，对 key union：

```text
legacy_held = initial_positions.get(asset_id, False)
typed_held  = asset_id in holdings
legacy_held must equal typed_held
```

冲突在 **strategy.run 前** fail-closed。

---

## 8. 统一 Checked Strategy Runner（冻结）

### 8.1 固定执行顺序

所有 QRP-owned 正式 strategy execution：

```text
raw StrategyInput
        ↓
select existing input contract
        ↓
validate_and_normalize_strategy_input(...)
        ↓
normalized StrategyInput
        ↓
strategy.run(normalized_input) exactly once
        ↓
validate_strategy_result(...)
        ↓
validated StrategyRunResult
```

### 8.2 薄 Common helper

允许新增：

```python
run_strategy_checked(
    strategy: StrategyProtocol,
    strategy_input: StrategyInput,
    *,
    input_normalizer: StrategyInputNormalizer | None = None,
) -> StrategyRunResult
```

上式只表达能力；实现不必真的新增 `StrategyInputNormalizer` 公共 Protocol。**优先用最小 dispatch / callable 复用既有产品输入准备函数，不为 Task07-A 建插件化 validator registry。**

职责只有：

1. 选择/调用正确的既有输入规范化合同；
2. validate + canonicalize input；
3. `strategy.run()` exactly once；
4. validate + canonicalize result；
5. return validated result。

不做数据库查询、indicator preparation、portfolio construction、execution、persistence 或 System B 判断。

### 8.3 普通 ASSET / MARKET 输入合同

对于标准 bar / market panel，继续复用现有 `StrategyDefinition.input_scope` 规则：

- ASSET identity = `(ticker, trade_date)`；
- MARKET identity = `(trade_date)`；
- required fields / indicators 完整；
- canonical date / deterministic sorting；
- holdings / as-of / initial_positions 统一校验。

不得因 Revision 3 改变普通 built-in / declarative strategy 的现有输入语义。

### 8.4 EventFrame 专用输入合同（Revision 3 冻结）

`event_drift_basic` 是**既有正式产品路径**，其 prepared input 不是 bar panel：

```text
ticker
announcement_date
available_trade_date
forecast_type
profit_change_min
profit_change_max
event_series_id
source_record_id
...
```

其策略 evaluation / entry date authority 是 `available_trade_date`，不是不存在的 `trade_date`。

因此 checked runner 对 event product 必须在 `strategy.run()` 前选择 **EventFrame normalizer**，不得套用普通 ASSET `(ticker, trade_date)` validator。

EventFrame normalizer 的最小职责冻结为：

1. `prepared_data` 必须为 DataFrame；
2. 校验 strategy definition 声明的 event required fields；
3. canonicalize `ticker` 为稳定 string；
4. `announcement_date` / `available_trade_date` 规范化为 date-only `YYYY-MM-DD`；
5. 拒绝无效日期；
6. 保留 `event_series_id` / `source_record_id` 等事件身份字段，不把事件记录压平成 bar identity；
7. deterministic sort 至少按：

```text
available_trade_date ASC
 ticker ASC
announcement_date ASC
source_record_id ASC
```

8. **输入层不做 `event_drift_basic` 的正向事件筛选、评分、持有窗口、容量选择，也不提前替代策略内部业务去重。**

关于重复语义：

- Common EventFrame normalizer 只保证输入记录可确定排序和必要字段完整；
- 不套用 ASSET `(ticker, trade_date)` 唯一性；
- 同一 `available_trade_date + ticker` 可以存在多条源事件记录；
- `event_drift_basic` 当前依据 enriched candidates 后的业务规则进行排序/选择/去重，这是 strategy semantics，07-A 不上移到 Common validator；
- 如果原始事件的 `source_record_id` 在正式上游 contract 已要求唯一，则继续由既有上游/EventFrame contract 验证，不在 Task07-A 新发明第二套 uniqueness rule。

### 8.5 Event holdings as-of

若未来某 QRP-owned event runner 接收 typed holdings：

```text
holdings_as_of_date < min(available_trade_date)
```

而不是要求 `trade_date`。

当前 event 产品若不支持 non-empty typed holdings，则维持“API 不暴露，或非空时 fail-closed”，不得静默丢弃。

### 8.6 不新增第三种泛化 InputScope

Revision 3 **不要求**新增诸如：

```text
StrategyInputScope.EVENT
StrategyInputKind
Validator Plugin Registry
```

原因：EventFrame 是仓库中已经存在的产品专用输入形态，本次只需让统一 checked runner **承认并安全复用它**。

只有未来多个策略出现可稳定复用的 event-domain input contract，才另行评估是否提升为公共 InputScope；这不是 Task07-A DoD。

### 8.7 QRP-owned 调用点不得绕过

至少覆盖：

- `StrategyRegistry.run()`；
- `StrategyBacktestRuntime`；
- `run_strategy_portfolio_backtest()`；
- Product Service direct strategy path；
- cross-sectional product path；
- event product path；
- residual / 其他 QRP-owned strategy runner；
- 后续 07-B / 07-C runner。

实现 Agent 必须先 grep/审计所有 QRP-owned `.run(StrategyInput(...))` 或等价调用。

不要求拦截外部 Python 用户直接 `.run()`。

---

## 9. Result Validation（冻结）

统一：

```python
validate_strategy_result(
    definition: StrategyDefinition,
    result: StrategyRunResult,
) -> StrategyRunResult
```

至少：

- result definition code/version 与被执行 strategy 一致；
- target strategy code/version 一致；
- target date exact/唯一/ASC；
- position asset_id 非空/唯一/ASC；
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
if portfolio_targets:
    native full snapshots → target frame
else:
    existing strategy_decisions_to_target_weights(...)
```

`strategy_decisions_to_target_weights()` 保留为 legacy low-level adapter。

Engine-facing frame：

```text
trade_date
asset_id
target_weight
priority
```

native target：

- date 原样保持 strategy target date；
- target_weight 原样；
- asset ASC；
- `priority` 只用稳定 neutral value 适配既有 frame，不重新做业务容量决策；
- rich reason/evidence/diagnostics 留在 canonical `StrategyRunResult`；
- native target 非空时绝不调用 decisions adapter。

---

## 11. Backtest / Product 接入边界（冻结）

### 11.1 Portfolio Product

```text
prepare data / resolve params
→ construct StrategyInput
→ run_strategy_checked(correct input contract)
→ strategy_result_to_target_weights()
→ existing timing shift exactly once
→ PortfolioBacktestEngine
```

native / legacy 最终复用同一个 Portfolio Engine。

### 11.2 Legacy `StrategyBacktestRuntime`

它是 ENTER/HOLD/EXIT trade-level runtime，不是完整 target runtime。

checked result 含 native `portfolio_targets` 时：

> **立即 fail-fast unsupported-path。**

不把 legacy runtime 重构成第二个 Portfolio Engine。

### 11.3 typed holdings 产品边界

- 底层 `StrategyInput` 正式支持 typed holdings；
- PortfolioBacktestEngine 保持 cash-only initial account；
- wrapper 不得接收 holdings 后静默丢弃；
- 未支持 seeded holdings 的 API 不暴露该参数，或非空时 fail-closed；
- 07-B/C 策略级测试可直接构造 typed input；
- Task08 可从空初始组合历史运行；
- Task09 处理 daily production holdings orchestration；
- 若 07-B/C 证明正式 runner 必须接收 non-empty holdings，只加薄传递参数，不扩 seeded account semantics。

### 11.4 Event product

Event product 继续保留其既有时间语义：

- `announcement_date` = evidence date；
- `available_trade_date` = strategy entry/evaluation date；
- 不因 checked runner 人为制造 `trade_date`；
- 不额外执行一次 next-open shift；
- checked runner 的作用只是让既有 EventFrame 在业务策略执行前获得正确输入校验，并让输出进入统一 result validation。

---

## 12. Canonical Strategy Result Persistence（冻结）

### 12.1 Rich result 必须持久化

`reason_code/evidence/diagnostics/native target` 属于 strategy result facts。Engine frame 是有意 lossy projection，不能成为 replay/audit 唯一存档。

因此：

> **凡 Product/backtest product path 产生 validated `StrategyRunResult`，必须将 canonical `to_dict()` 写入现有 reproducibility evidence。**

### 12.2 复用 `reproducibility.json`

不新增数据库表或第二套 result store。

稳定字段例如：

```json
{
  "strategy_result": {
    "definition": {},
    "parameters": {},
    "decisions": [],
    "authorizations": [],
    "portfolio_targets": [],
    "diagnostics": []
  }
}
```

要求：

- snapshot = validated canonical `StrategyRunResult.to_dict()`；
- validate 后写入；
- 不从 Engine frame 反推；
- loader/replay 可读取；
- legacy run 使用同一统一 shape；
- Task07-A 只要求写入/读取/确定性测试，不提前重写 Task08 replay orchestration。

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

---

## 14. PIT / Replay Determinism

必须满足：

- target date canonical；
- strategy code/version 明确；
- target/position ordering 固定；
- evidence canonical；
- holdings as-of 根据所选 input contract 的 evaluation date 校验；
- strategy 不主动查询“当前最新数据库状态”；
- 同 prepared input + params + initial holdings → 同 result；
- checked runner input/output validation 顺序唯一；
- event input 不被强行映射为 bar `trade_date`；
- timing shift 只执行一次；
- canonical `StrategyRunResult` 写入 reproducibility evidence。

---

## 15. Validation 细则

### 15.1 普通 ASSET / MARKET input

- 按既有 scope canonical；
- required fields/indicators 完整；
- scope identity 唯一；
- holdings/as-of 合法；
- initial_positions 与 holdings 无冲突。

### 15.2 EventFrame input

- DataFrame；
- required event fields 完整；
- ticker canonical string；
- announcement/available date canonical；
- deterministic sort；
- 不要求 `trade_date`；
- 不套 `(ticker, trade_date)` uniqueness；
- 不上移 event strategy 内部筛选/评分/候选去重业务；
- holdings 如被支持，以 `available_trade_date` 作为 evaluation-date boundary。

### 15.3 Result

- definition/version 一致；
- target dates 唯一/canonical/ASC；
- positions unique/ASC；
- weights finite/range/sum；
- evidence JSON-compatible；
- diagnostics stable；
- full snapshot / serialization 合法。

---

## 16. 测试要求

至少覆盖：

1. legacy built-in 无 holdings/targets 行为不变；
2. declarative strategy 行为不变；
3. cross-sectional product 行为不变；
4. **event_drift_basic 的既有 EventFrame（无 `trade_date`）可通过 checked runner，且业务结果与当前基线一致**；
5. EventFrame 缺 required field / invalid available date 在 strategy.run 前 fail-closed；
6. EventFrame 不因同 `available_trade_date+ticker` 多源记录被 Common validator 错误拒绝；
7. event checked runner 不额外 next-open shift；
8. target/holding public exports 稳定；
9. `StrategyRunResult.to_dict()` 含 canonical targets；
10. `positions=()` = all cash；
11. omitted asset = target 0；
12. duplicate target asset/date fail-closed；
13. invalid weight/NaN/Inf/evidence fail-closed；
14. target ordering 固定；
15. strategy/version mismatch fail-closed；
16. holdings/as-of validation；
17. holdings + initial_positions conflict 在 strategy.run 前 fail-closed；
18. `run_strategy_checked()` 固定：correct input validator → run once → result validator；
19. Registry/runtime/portfolio/Product/cross-section/event/residual QRP-owned paths 不绕过；
20. native target 不调用 decisions adapter；
21. legacy target path 行为不变；
22. Product native target timing shift only once；
23. Legacy StrategyBacktestRuntime native target fail-fast；
24. wrapper 不静默丢弃 non-empty holdings；
25. reproducibility snapshot 含 canonical `strategy_result`；
26. rich target write/load 后保持 canonical；
27. replay/result loader 可读 snapshot；
28. 同输入重复 run/serialization 一致；
29. full regression 通过。

---

## 17. 允许修改的典型区域

预期：

```text
src/qrp_atlas/strategies/models.py
src/qrp_atlas/strategies/validation.py
src/qrp_atlas/strategies/__init__.py
src/qrp_atlas/strategies/registry.py
src/qrp_atlas/strategies/builtin/event_drift.py      # 仅复用/暴露既有 EventFrame preparation contract（如需要）
src/qrp_atlas/backtest/runtime/strategy.py
src/qrp_atlas/backtest/portfolio/strategy.py
src/qrp_atlas/backtest/product/service.py
src/qrp_atlas/backtest/product/cross_section.py
src/qrp_atlas/backtest/product/event.py
src/qrp_atlas/backtest/results/writer.py
src/qrp_atlas/backtest/results/                     # load/reproducibility tests
相关 tests
```

允许新增：

- 一个薄 checked runner helper；
- 一个薄 result→target adapter；
- 必要 canonical serialization helper；
- **一个薄 EventFrame input normalizer，或复用/收紧已有 `build_event_drift_prepared_data()`；不得建立 validator plugin framework。**

---

## 18. 明确禁止的 Scope Creep

禁止：

- Strategy Framework v2；
- 新泛化 EVENT InputScope（除非实现审计证明不新增反而无法保持既有路径，且必须先回到设计审计，不得自行扩）；
- validator plugin registry；
- dynamic plugin loader / external strategy RPC；
- 多账户 / Broker Position Model；
- seeded broker account / full account ledger；
- OMS / order plan / execution extension；
- 多资产通用 portfolio domain；
- 多币种 / margin / short；
- 新 strategy result database/store；
- 把 event_drift 业务筛选/去重搬进 Common validator；
- System B 07-B / 07-C 规则；
- Task08 replay orchestration 重构；
- Task09 production orchestration。

---

## 19. DoD

Task07-A 完成条件：

1. `StrategyRunResult` 能表达 typed full-snapshot Portfolio Target；
2. full snapshot / omitted asset / all-cash / residual cash 语义唯一；
3. target date / ordering / serialization canonical；
4. typed initial holdings 有明确 as-of；
5. legacy initial_positions 兼容规则确定；
6. 所有 QRP-owned 正式 strategy run 采用 checked 顺序；
7. checked runner 能选择普通 ASSET/MARKET 与既有 EventFrame 的正确 input contract；
8. EventFrame 无 `trade_date` 不被普通 ASSET validator 错误拒绝；
9. event 业务筛选/去重不被上移 Common；
10. input conflict 在 strategy.run 前 fail-closed；
11. 唯一 result→target highest-level router；
12. native target / decisions 不双 SSOT；
13. Product native target timing shift 仅一次；
14. legacy runtime 不静默忽略 native target；
15. non-empty holdings 不被 wrapper 静默丢弃；
16. canonical `StrategyRunResult` 写入既有 reproducibility evidence；
17. rich target evidence 不因 Engine lossy frame 丢失；
18. replay/result load 可访问 canonical strategy snapshot；
19. existing built-in / declarative / cross-sectional / event 行为不回归；
20. 通用 Portfolio/Backtest 无 System B 业务知识；
21. 未引入 Account/OMS/Broker/新 result store/validator framework scope；
22. targeted tests + full regression 通过；
23. 最终对抗审计无 BLOCKER / MAJOR 后才允许实现。

---

## 20. 对抗审计处置记录

### 第一轮

- B-1 full snapshot → `omitted asset=0`, `positions=()=all cash`。
- B-2 result validation → 唯一 `validate_strategy_result()`。
- native route/date → 唯一 result→target；converter 不 shift。
- holdings lifecycle → typed holdings + as-of。
- legacy conflict → key-union bool equivalence。
- serialization → canonical target JSON。
- legacy runtime ignore native → fail-fast。
- Product holdings gap → wrapper 禁止静默丢弃，不扩 seeded account。

### 第二轮

- M-1 input fail-closed 无统一入口 → checked runner：input validate → run once → result validate。
- M-2 canonical result 未持久化 → validated `StrategyRunResult.to_dict()` 写入现有 `reproducibility.json`。

### 第三轮

- M-3 EventFrame 会被普通 ASSET validator 错误要求 `trade_date` → Revision 3 冻结 EventFrame 专用 normalizer：以 `available_trade_date` 为 evaluation date，保留事件记录身份与业务去重边界；checked runner 按既有产品输入形态选择 validator；**不新增泛化 EVENT InputScope / validator registry。**

---

## 21. Task07-A 完成后的架构结果

Task07-A 完成只意味着：

```text
QRP Strategy Runtime
可以用统一 checked execution boundary
在不破坏既有特殊产品输入的前提下
承载复杂策略原生输出完整 Portfolio Target
并把 canonical strategy result 纳入 replay evidence
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
