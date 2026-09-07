# Task06-B Walkthrough｜System B Theme Trend Rank 交付说明

> 状态：**IMPLEMENTED / ADVERSARIAL AUDIT PASSED**  
> 分支：`feature/v1.1-task06-b`  
> 设计基线：`Task06-B System B Theme Trend Rank 设计书 v0.1-rc2.md`  
> 审计基线：`Task06-B Adversarial Re-audit v0.1-rc2.md`（PASS）  
> 本文覆盖 **Task06-B / System B Theme Trend Rank** 的权威设计实现、生产流水线、对抗闭环与测试验证。

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

正式落盘两张表与物化范围：
1. `system_b_theme_rank_snapshot`：针对全量 canonical themes 集合 $C_D$ 物化，每个 $C_D$ 成员一行。未入选 $U_D$ 者标记为 `rank_eligible = false` 及对应原因（如 `NO_OPEN_EPISODE`）。
2. `system_b_theme_rank_component_audit`：严格只针对合格集合 $U_D$ 物化，每个 $U_D$ 成员物化 7 条叶子组件审计记录，总行数为 $|U_D| \times 7$。

---

## 2. 7 个 Leaf 组件与冻结权重（35/10/15/20/10/6/4）

系统严格实现 rc2 规定的 7 个叶子组件，绝对权重固定为 35/10/15/20/10/6/4：

| 维度 (Dimension) | 叶子组件 (Component) | 基础权重 | 方向 | 业务定义与取值规则 |
| :--- | :--- | :---: | :---: | :--- |
| **Momentum** (45%) | `episode_return` | 35% | 升序 (ASC) | D 动态对齐收益率 $P(D) / P(\text{start}) - 1$，严禁读取 D 之后的终结收益；点位非法直接 fail-fast |
| | `theme_daily_return` | 10% | 升序 (ASC) | M4 当日板块即时收益率，只表达 D 日即时强化，不支配中期趋势语义 |
| **Persistence** (35%) | `episode_duration` | 15% | 降序 (DESC) | 从 `episode_confirmed_date` 到 $D$（含首尾）的实际开市交易日数（负向指标，时间越短相对排名越高） |
| | `episode_above_ma5_ratio` | 20% | 升序 (ASC) | `start_date` 到 $D$ 的有效状态日中处于 MA5 之上的比例；保留合法 NULL 并剔除出分母，缺行或有效日为 0 fail-fast |
| **Structure** (10%) | `limit_up_diffusion` | 10% | 升序 (ASC) | 涨停扩散度 $theme\_limit\_up\_count / effective\_member\_count$；$U_D$ 保证分母 > 0，严禁用裸涨停家数 |
| **Popularity** (10%) | `hot_stock_ratio` | 6% | 升序 (ASC) | M5 当日热股占比 $theme\_hot\_stock\_count / theme\_member\_count$；保留 M5 的 PIT Theme 成员分母 |
| | `hot_appearance_rate` | 4% | 升序 (ASC) | M5 热榜出现率 $theme\_hot\_list\_appearance\_count / (theme\_member\_count \times valid\_snapshot\_count\_total)$ |

> **注意**：评分体系不存在 `turnover_ratio_ma5_ratio` 或裸 `limit_up_count`。

---

## 3. 截面排名、变异重权与状态优先级（B1 / N1 / N2 闭环）

### 3.1 合格截面 $U_D$ 与 D 动态对齐（B1 闭环）
- 必须通过 `OPEN_AT_D`：`episode_confirmed_date <= D` 且 `episode_end_date IS NULL OR episode_end_date > D`。
- 历史重演时，即使物理库中 Episode 后来在 $E > D$ 关停，在目标日 $D$ 重演计算出的指标完全恒定。
- 若 `episode_end_date == D`，合法识别为已关停形态（`rank_eligible = false`, `rank_eligibility_reason = NO_OPEN_EPISODE`）。

### 3.2 标准化排名分与等比重归一化（N1 / N2 闭环）
- **标准化排名分 (NRS)**：
  $$NRS = 100 \times \frac{N - \text{rank}}{N - 1}$$
  使用平均并列秩（average ties），升序/降序严格映射。
- **无变异等比重归一化 (`NO_VARIATION`)**：
  - 当某组件在 $U_D$ 中所有值相同（包括单例 $N=1$ 或所有合格值相同）时，该叶子 $NRS = \text{NULL}$；
  - 组合总分仅在具有变异的有效叶子集合 $\mathcal{V}$ 上，将其基础权重按比例等比缩放求和：
    $$\text{composite\_rank\_score} = \sum_{c \in \mathcal{V}} \frac{w_c}{\sum_{k \in \mathcal{V}} w_k} \times NRS_c$$
  - 若所有叶子均无变异，则 `composite_rank_score = NULL`；四维展示分同理按维度内有效叶子重权计算。
- **状态优先级**：
  $$\text{INCOMPLETE\_INPUT} > \text{INSUFFICIENT\_UNIVERSE} > \text{NO\_VARIATION} > \text{OK}$$
  单例截面（$|U_D|=1$）优先判定为 `INSUFFICIENT_UNIVERSE`，但在 Path A 缺失热度输入时，优先判定为 `INCOMPLETE_INPUT`。
- **整数键**：输出整数排序键 $K_i$ 用于稳定 tie-breaking。

---

## 4. 生产流水线、正式依赖与双路径调度（M1 / M2 闭环）

### 4.1 流水线契约与依赖关系
- **流水线 ID**：`system_b_theme_rank_daily`
- **正式依赖**：`dependencies = ("theme_m4_production",)`
- **解耦设计**：`theme_m5_production = SUCCESS` **不是**流水线的静态硬依赖。

### 4.2 Path A vs Path B 执行逻辑
- **Path A（可信不可用）**：
  - 触发条件：`popularity_source_availability` 中任一源标记为 `UNAVAILABLE`。
  - 调度行为：即使 M5 失败或未执行，也不阻塞 Theme Rank 计算；保留非热度指标计算与审计，热度叶子标记为 `INCOMPLETE_INPUT`，全量物化 $C_D$。
- **Path B（全源可用强一致性验证）**：
  - 触发条件：`dc_hot` 与 `ths_hot` 均标记为 `AVAILABLE`。
  - 强校验逻辑：
    1. **指纹比对**：校验 `persisted_m5_input_snapshot_id == recomputed_fresh_facts.input_snapshot_id`；
    2. **算术一致性**：校验 persisted M5 的 member_count、hot_stock_count、hot_stock_ratio、appearance_count、source_count 的内部逻辑关系；
    3. **业务输出对齐**：校验 persisted M5 的 5 项业务指标与 `fresh_m5_facts.observations` 逐字段完全相等；
    4. **快照推导校验**：从 fresh DC/THS rows 实际推导快照数与序列，严格比对 `popularity_source_availability`；
    5. **任一不一致立即 fail-fast**，在写库前中止，原有已发布 Rank 结果完整保留。

### 4.3 单事务原子性替换
在目标日 $D$ 单个 DuckDB 事务内原子执行两张表的 DELETE + INSERT，提交前严格校验插入行数与计算行数一致性，失败自动 ROLLBACK。

---

## 5. Comprehensive Provenance 规范（rc2 §12）

每一行 `system_b_theme_rank_snapshot` 的 `input_provenance` 字段均完整序列化以下 8 维元数据：
1. `trade_date`：目标交易日。
2. `calculation_version`：`system_b_theme_rank@0.1.0`。
3. `c_d_provenance`：$C_D$ 总数、所有板块 ID 列表、SHA256 指纹。
4. `m4_lineage`：M4 的 calculation_version、production_run_id、input_snapshot_id 及行数。
5. `m5_lineage`：执行路径（Path A / Path B）、persisted_input_snapshot_id、recomputed_input_snapshot_id 及版本。
6. `theme_index_state_episode_lineage`：Episodes、Index Daily、Index State 的 rule_versions、calculation_versions 及 input_snapshot_ids。
7. `popularity_availability`：DC 与 THS 热度源的完整可用性元数据字典。
8. `base_weights` 与 `effective_weights`：覆盖全部 7 个叶子组件的基础权重与动态重权后的有效权重。

---

## 6. 验证证据与对抗复审记录

### 6.1 核心测试套件
```bash
pytest tests/indicators/test_theme_ranking.py tests/pipeline/system_b_theme_rank/test_production.py -v
============================= 17 passed in 5.37s =============================
```
包含 9 个纯指标测试与 8 个生产边界测试（覆盖 B1、M1、M2、N1、N2 闭环及 M5 算术、输出对齐、快照推导反例测试）。

### 6.2 对抗审计重演
```bash
python docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc1.py
AUDIT REPRODUCTIONS PASSED

python docs/QRP产品蓝图v1.1/task06/audit_task06_b_rc2.py
RC2 FOCUSED REPRODUCTIONS PASSED: 15 assertions
```

### 6.3 契约注册表集成检查
```bash
pytest tests/pipeline/test_irm_contracts.py tests/pipeline/test_market_data_contracts.py tests/pipeline/test_pipeline_contract.py tests/pipeline/test_production_jobs.py -v
============================ 141 passed in 16.12s =============================
```

---

## 7. Linux 服务器部署与验收操作

1. **同步 DDL 与代码**：
   同步 `deploy/duckdb/009_system_b_theme_rank.sql` 与 `src/qrp_atlas/` 变更。
2. **执行数据库迁移**：
   ```bash
   duckdb /path/to/quant.duckdb < deploy/duckdb/009_system_b_theme_rank.sql
   ```
3. **验证与验收**：
   ```bash
   qrp-pipeline validate-contracts
   qrp-pipeline run system_b_theme_rank_daily --parameters '{"trade_date": "YYYY-MM-DD"}'
   ```
