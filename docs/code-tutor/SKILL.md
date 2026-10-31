---
name: research-companion-code-guide
description: 阅读 Research Companion 的入口、权限、Agent、存储与测试，按实际源码解释业务链路。
---

# 阅读步骤

先确定要理解的用户行为及实际版本，再读根 README 的已知限制。用 `rg` 定位路由和函数，从 HTTP → context → 应用 → 领域/仓储追踪，给出精确路径和可验证输入输出。

解释时区分已实现能力、测试替身与真实服务行为；不要把历史设计的任务框当成实现证明。关注用户归属、管理权限、取消、错误结果和副作用，而不只描述成功流程。

需要改动时遵循用户授权的提交策略，只修改当前问题涉及的范围。用实际回归或构建验证，不伪造通过结果；说明未联调部分。

参考：`references/architectureChecklist.md`、`businessFlowTemplate.md`、`debugPlaybook.md`、`explainPatterns.md`、`learningPathTemplate.md`。
