# IoT Flink 实时计算平台 (test-flink)

基于 Apache Flink 1.19 的高性能物联网数据处理引擎。本项目采用 **计算与规则分离** 的设计理念，支持通过 MySQL 动态配置点位衍生逻辑，并提供完善的状态管理机制。

---

## 📂 模块详细描述

### 1. `test-flink-iot` (计算核心模块)
点位衍生计算的核心实现，采用 **KeyedBroadcastProcessFunction** 架构。
- **主程序**: `IotCalcJob.java`
- **核心逻辑 (`CalcProcessFunction`)**:
    - **双流协同**: 通过 `connect` 连接 Kafka 点位数据流与 MySQL CDC 规则广播流。
    - **分桶机制**: 使用 `MapState` 对输入数据进行按分钟分桶，确保在窗口计算时能获取到最完整的数据。
    - **定时触发**: 利用 Flink Timer Service 注册处理时间定时器，在每分钟结束时触发衍生点位的计算。
- **计算策略工厂**: `CalcFuncStrategyFactory` 根据规则类型动态路由至具体的执行策略。
- **内置策略**:
    - **ArithmeticCalcFunc**: 实现基础的算术表达式计算（加减乘除）。
    - **Time-Diff 策略**: 提供日 (`DayDiff`)、月 (`MonthDiff`)、年 (`YearDiff`)、累计 (`TotalDiff`) 维度的差值计算，基于 `Keyed State` 自动维护各周期的起始值。

### 2. `test-flink-common` (公共组件)
定义全局通用的基础设置，减少模块间耦合。
- **模型类 (POJO)**: 如 `PointData`（点位数据包）、`DevicePointRule`（衍生计算规则）。
- **常量定义**: 统一维护 Kafka 节点、Topic 名称、数据库连接等全局常量（`FlinkConstants`）。

### 3. `test-flink-job-state` (状态与容错实验)
专门用于验证 Flink 复杂状态管理与 S3 容错特性的实验模块。
- **State Experiment**: 实现一个累加器任务 `StateExperimentJob`，测试大规模 Key 下的状态存取性能。
- **S3 保存点**: 配置了对 S3/MinIO 的原生集成，验证在任务重启或扩缩容时，状态的持久化与恢复效率。
- **Exactly-Once**: 演示如何通过 Kafka Sink 的 2PC（两阶段提交）实现端到端的精确一次性处理。

### 4. `test-flink-job-kafka` (接入实验)
用于测试基础连接性的轻量级模块。
- **接入测试**: 包含 `KafkaStreamingJob`，用于快速验证 Kafka 集群的连通性及序列化性能。

---

## 🚀 关键技术特性

- **动态热更新**: 集成 Flink CDC，修改 MySQL 规则表后，计算任务无需重启即可感知变更。
- **智能状态清理**: 
    - 针对日维度的差值计算，配置 **2 天 (36-48h)** 的 TTL。
    - 针对全量累计，配置 **永不过期**，确保业务数据的绝对准确性。
- **抗抖动设计**: 通过分桶和 Timer 机制，解决各点位数据到达时间不一致（Out-of-Order）带来的计算延迟问题。

---

---

## 📖 快速开始 (快速链接)
- [编译打包说明](#环境依赖)
- [提交示例](#任务提交)
- [核心配置指南](#核心配置说明)
