qrp-atlas

Market Deconstruction Toolkit

一个用于量化分析与复盘的个人工具，用来把市场拆解成清晰可观察的结构，而不是做自动交易。

What is this?

qrp-atlas 是一个围绕 数据结构 + 可视化复盘 构建的研究工具。

它负责：

市场数据整理

基础量化分析

复盘与观察界面

它不负责：

自动交易

策略回测框架

交易决策

Machine handles structure. Human handles narrative.

Why it exists

市场信息越来越复杂。

目标不是预测市场，而是：

更清晰地看到结构

更稳定地记录变化

更高效地复盘

这是一个帮助“解构市场”的工具，而不是替代主观交易的系统。

Project Structure
qrp-atlas/
│
├─ src/            # 长期功能模块
├─ scripts/        # 一次性脚本
│
├─ data/
│   ├─ raw/
│   │   └─ daily_snapshot/
│   ├─ canonical/  # 数据备份，可重建数据库
│   └─ db/
│       └─ quant.db
│
└─ README.md

原则：

可复用功能只写在 src/

scripts 仅临时使用

canonical 是数据库恢复源

Data Flow
daily_snapshot → ingestion → DuckDB

进入日常阶段后：

不重复抓历史

只做每日更新

Tech Stack

Python 3.13

DuckDB

Pandas

Streamlit

保持最小可用，不追求复杂架构。

Current Goal

建立一个：

可持续迭代的市场解构工具

包括：

数据底座

量化辅助分析

可视化复盘界面

Notes

这是一个长期个人项目。

它会慢慢成长，而不是一次完成。