# Task06-B Walkthrough｜System B Theme Trend Rank 交付说明

> 状态：**IMPLEMENTED / ADVERSARIAL AUDIT PASSED**  
> 分支：`feature/v1.1-task06-b`  
> 设计基线：`Task06-B System B Theme Trend Rank 设计书 v0.1-rc2.md`  
> 审计基线：`Task06-B Adversarial Re-audit v0.1-rc2.md`（PASS）  
> 本文覆盖 **Task06-B / System B Theme Trend Rank** 的完整设计实现、生产流水线、对抗闭环与测试验证。

---

## 1. 交付目标与架构定位

Task06-B 的目标是基于 System B 既有板块事实（M4 观察记录、Custom Index 日线与状态、Episode 生命周期、M5 热度事实与热度源可用性），在每个目标交易日 $D$ 计算主题/板块级的横截面相对趋势排名（Theme Trend Rank），并原子物化到主 DuckDB：

```text
Canonical Themes (C_D)
        +
Theme M4 Observation (Finalized Pointer)
        +
Custom Index Daily & State (D-aligned return, above MA5)
        +
Popularity Availability (dc_hot / ths_hot) & Theme M5 Observation
        ↓
Fixed Universe U_D (OPEN_AT_D & confirmed <= D)
        ↓
7 Leaf Raw Components Cross-Sectional Ranking (Average Ties)
        ↓
Normalized Rank Scores (NRS) + NO_VARIATION Proportional Reweight
        ↓
Theme Rank Snapshot + Component Audit (Atomic Replace)
```

正式落盘两张表：
1. `system_b_theme_rank_snapshot`：主排名结果快照，每个 $C_D$ 成员一行。
2. `system_b_theme_rank_component_audit`：叶子组件审计明细，每个 $C_D$ 成员 7 行（全量物化）。

---

## 2. 核心计算与业务语义实现

### 2.1 合格主题截面 $U_D$ 与 D 动态对齐（B1 闭环）
- **合格条件**：
  - Theme $i \in C_D$；
  - $M4(D)$ 中存在有效观测且 `custom_index_episode_id` 非空；
  - 关联 Episode 满足：`episode_confirmed_date <= D`，且 `episode_end_date IS NULL OR episode_end_date > D`（即状态为 `OPEN_AT_D`）。
- **D 动态对齐收益率**：
  - 动态计算 $P(D) / P(\text{start}) - 1$，严禁使用已终结 Episode 的最终全生命周期收益。
  - 历史重演时，即使在物理库中 Episode 后来在 $E > D$ 关停，在目标日 $D$ 重演计算出的指标完全稳定恒定。
  - 若 `episode_end_date == D`，合法识别为已关停形态（`rank_eligible = false`, `rank_eligibility_reason = NO_OPEN_EPISODE`）。

### 2.2 7 个 Leaf 组件与冻结权重
| 维度 (Dimension) | 叶子组件 (Component) | 基础权重 | 方向 | 取值与缺失规则 |
| :--- | :--- | :---: | :---: | :--- |
| **Momentum** (45%) | `episode_return` | 35% | 升序 (ASC) | $P(D)/P(\text{start})-1$，start/D 点位非法抛错 |
| | `episode_duration` | 10% | 降序 (DESC) | `confirmed_date` 到 $D$ 开市交易日数（负向指标） |
| **Structure** (35%) | `above_ma5_ratio` | 15% | 升序 (ASC) | `start_date` 到 $D$ 处于 MA5 之上天数比例；合法 NULL 保留并排除出分母，非法缺失/0有效日 fail-fast |
| | `turnover_ratio_ma5_ratio` | 20% | 升序 (ASC) | M4 中当日换手率相对 5 日均值比值 |
| **Heat** (10%) | `hot_stock_ratio` | 6% | 升序 (ASC) | M5 当日热股占比（Path A 下标记为 `INCOMPLETE_INPUT`） |
| | `hot_appearance_rate` | 4% | 升序 (ASC) | M5 热榜出现率，分母为 $N \times (\text{dc\_snapshots} + \text{ths\_snapshots})$ |
| **Breadth** (10%) | `limit_up_count` | 10% | 升序 (ASC) | M4 当日涨停家数 |

### 2.3 截面标准化打分与变异重权（N1 / N2 闭环）
- **标准化排名分 (NRS)**：
  $$NRS = 100 \times \frac{N - \text{rank}}{N - 1}$$
  - $\text{rank}$ 使用平均并列秩（average ties）；
  - 升序/降序严格映射（正向指标最小值为 rank $N \to 0$ 分，最大值为 rank $1 \to 100$ 分）；
- **无变异等比重归一化 (`NO_VARIATION`)**：
  - 当某组件在 $U_D$ 中所有值相同（包括单例 $N=1$ 或所有合格值相同）时，该叶子 $NRS = \text{NULL}$；
  - 组合总分仅在具有变异的有效叶子集合 $\mathcal{V}$ 上，将其基础权重按比例等比缩放求和：
    $$\text{composite\_rank\_score} = \sum_{c \in \mathcal{V}} \frac{w_c}{\sum_{k \in \mathcal{V}} w_k} \times NRS_c$$
  - 若所有叶子均无变异，则 `composite_rank_score = NULL`；
  - 四维展示分同理，由该维度内有效叶子重权计算；若该维度下叶子全部无变异，则维度展示分为 `NULL`。
- **状态优先级**：
  $$\text{INCOMPLETE\_INPUT} > \text{INSUFFICIENT\_UNIVERSE} > \text{NO\_VARIATION} > \text{OK}$$
  - 单例截面（$|U_D|=1$）优先判定为 `INSUFFICIENT_UNIVERSE`，但在 Path A 缺失热度输入时，优先判定为 `INCOMPLETE_INPUT`。
- **排序键与 Tie-Breaking**：
  - 最终生成整数排序键 $K_i$ 进行确定性 tie-breaking，并四舍五入保留两位小数。

---

## 3. 生产流水线与双路径调度（M1 / M2 闭环）

### 3.1 流水线契约
- **契约 ID**：`system_b_theme_rank_daily`
- **依赖门禁**：仅依赖 `theme_m4_production`。
- **解耦设计**：`theme_m5_production = SUCCESS` **不是** 无条件硬性依赖。

### 3.2 Path A vs Path B 分流逻辑
- **Path A（可信不可用）**：
  - 条件：`popularity_source_availability` 中任一源标记为 `UNAVAILABLE`。
  - 行为：
    - 不校验 M5 是否成功（即使 M5 为空或失败仍正常放行）；
    - 保留所有非热度指标计算与审计；
    - 热度叶子赋值为 `NULL`，并在审计表中标记为 `INCOMPLETE_INPUT`；
    - 全量物化 $C_D$。
- **Path B（全源可用）**：
  - 条件：`dc_hot` 与 `ths_hot` 均标记为 `AVAILABLE`。
  - 行为：
    - 强制要求 M5 完成且 `calculation_version == "v1.1"`；
    - 严苛校验指纹一致性：
      $$\text{persisted\_m5\_input\_snapshot\_id} == \text{recomputed\_fresh\_facts.input\_snapshot\_id}$$
    - 同日数据被修改而 M5 未重算时，立即 fail-fast 抛出 `THEME_M5_INPUT_SNAPSHOT_ID_MISMATCH`。

### 3.3 单事务原子性持久化
在目标日 $D$ 执行持久化时：
```sql
BEGIN TRANSACTION;
DELETE FROM system_b_theme_rank_snapshot WHERE trade_date = ?;
DELETE FROM system_b_theme_rank_component_audit WHERE trade_date = ?;
INSERT INTO system_b_theme_rank_snapshot ...;
INSERT INTO system_b_theme_rank_component_audit ...;
-- 校验实际行数与生成行数严格一致
COMMIT;
```
任一环节失败均执行 ROLLBACK，原数据保持不变。

---

## 4. 交付代码与文件变更

| 文件路径 | 变更类型 | 说明 |
| :--- | :---: | :--- |
| `deploy/duckdb/009_system_b_theme_rank.sql` | 新增 | 快照表与组件审计表 DDL |
| `src/qrp_atlas/contracts/system_b.py` | 修改 | 常量、枚举、计算版本、权重定义 |
| `src/qrp_atlas/contracts/schema.py` | 修改 | 注册两表 Schema 至主 DuckDB |
| `src/qrp_atlas/contracts/__init__.py` | 修改 | 导出相关契约类型与符号 |
| `src/qrp_atlas/indicators/system_b/theme_ranking.py` | 新增 | 纯指标计算层（无数据库依赖） |
| `src/qrp_atlas/pipeline/system_b_theme_rank/service.py` | 新增 | 生产服务层（输入校验、M5 指纹、原子持久化） |
| `src/qrp_atlas/pipeline/system_b_theme_rank/__init__.py` | 新增 | 服务对外导出接口 |
| `src/qrp_atlas/pipeline/system_b_theme_rank_contracts.py` | 新增 | 流水线契约定义与校验 |
| `src/qrp_atlas/pipeline/contract_catalog.py` | 修改 | 注册 `system_b_theme_rank_daily` |
| `tests/indicators/test_theme_ranking.py` | 新增 | 纯计算层单元测试（9 个用例） |
| `tests/pipeline/system_b_theme_rank/test_production.py` | 新增 | 生产边界与闭环测试（4 个用例） |
| `tests/pipeline/test_irm_contracts.py` | 修改 | 同步契约注册总数（34 → 35） |
| `tests/pipeline/test_market_data_contracts.py` | 修改 | 注册集合断言加入新增契约 |
| `tests/pipeline/test_pipeline_contract.py` | 修改 | 契约验证 CLI 与注册表用例同步 |
| `tests/pipeline/test_production_jobs.py` | 修改 | 生产作业契约无回退断言同步 |

---

## 5. 验证证据与对抗复审记录

### 5.1 核心测试套件
```text
pytest tests/indicators/test_theme_ranking.py tests/pipeline/system_b_theme_rank/test_production.py -v
============================= 13 passed in 2.01s =============================
```

### 5.2 对抗审计重演通过
```text
python docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc1.py
AUDIT REPRODUCTIONS PASSED

python docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc2.py
RC2 FOCUSED REPRODUCTIONS PASSED: 15 assertions
```

### 5.3 流水线与注册表回归
```text
pytest tests/pipeline/test_irm_contracts.py tests/pipeline/test_market_data_contracts.py tests/pipeline/test_pipeline_contract.py tests/pipeline/test_production_jobs.py -v
============================ 141 passed in 16.12s =============================
```

---

## 6. Linux 服务器部署与验收操作

1. **代码与 DDL 同步**：
   将 `deploy/duckdb/009_system_b_theme_rank.sql` 及 `src/qrp_atlas/` 变更提交并拉取到目标 Linux 环境。
2. **执行数据库迁移**：
   ```bash
   duckdb /path/to/quant.duckdb < deploy/duckdb/009_system_b_theme_rank.sql
   ```
3. **验证与验收**：
   ```bash
   # 1. 验证契约注册无误
   qrp-pipeline validate-contracts
   # 2. 试运行测试交易日
   qrp-pipeline run system_b_theme_rank_daily --parameters '{"trade_date": "YYYY-MM-DD"}'
   ```
