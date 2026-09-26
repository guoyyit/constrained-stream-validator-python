# Constrained Stream Validator (Python)

面向模型输出的**流式校验与修复服务**。上游模型以字符/标记流持续输出，服务在
**不缓存完整响应即可校验**的前提下，实时判断输出是否满足已注册的命名约束；
违规时按可配置策略（立即中止 / 截断到最后合法前缀 / 丢弃非法片段后继续）处理，
并提供约束版本管理、进行中会话状态、统计与资源上限保障。

## 快速开始

```bash
uv sync                       # 安装依赖（需要 Python 3.14+）
uv run python main.py         # 启动 HTTP 服务，默认 :8000
uv run pytest                 # 运行全部测试
```

环境变量：`CSV_MAX_ACTIVE_SESSIONS`（默认 10000）、`CSV_MAX_LENGTH`、
`CSV_MAX_DURATION_SECONDS`、`PORT`。

## 架构

```
main.py                     服务入口（uvicorn main:app）
stream_validator/
  models.py                 领域类型：约束、策略、错误码、分块、事件、结果、统计
  constraints.py            四类增量（流式）校验器 + 工厂
  registry.py               命名约束的不可变版本注册表
  engine.py                 单会话引擎：乱序重组、策略执行、长度/时间上限、取消
  manager.py                会话管理器：隔离、并发上限、统计聚合、过期回收
  service.py                FastAPI HTTP 接口（含可选后台回收任务）
tests/                      单元 + HTTP 端到端 + 高并发测试
```

### 核心设计

* **真正的增量校验**：每个校验器都是有限状态机，逐字符消费，返回“本片段最长
  合法前缀 + 终止错误”。JSON / 枚举 / 长度校验器只保留有界结构状态；正则校验器
  依赖 `regex` 库的 *partial fullmatch* 能力判断“当前前缀是否仍可能完整匹配”，
  其内部前缀缓冲受会话 `max_length` 上限约束（见“兼容性边界”）。
* **版本固定**：注册约束只会追加不可变版本；会话创建时固定到具体
  `ConstraintVersion`（默认最新），版本更新不影响已开始的请求。
* **严格隔离**：共享的约束版本只读；每个会话构建独立的校验器实例，拥有独立锁，
  不共享任何可变状态。
* **有界内存**：乱序重组只缓存“缺口之后”的片段，连续片段立即排空；会话数量、
  输出长度、缺口跨度、持续时间均有上限；取消/终止即清空缓冲，管理器支持回收。

## 约束类型

| 类型 `type`   | 参数 `params`                     | 语义 |
|---------------|-----------------------------------|------|
| `structured`  | `kind="json"`，`max_depth=32`     | 输出必须构成完整 JSON；任何已接受前缀都必须能扩展为合法 JSON 文档 |
| `enum`        | `values: list[str]`（非空）       | 输出必须恰好等于某个枚举值（逐字符沿枚举前缀树推进） |
| `regex`       | `pattern: str`                    | 输出必须 `fullmatch`；增量阶段用 partial match 判断前缀是否仍可行 |
| `length`      | `max: int`                        | 输出字符数上限 |

## 违规处理策略（确定性）

同一输入 + 同一策略的结果**必须确定**：所有策略都基于逐字符的最长合法前缀，
不存在任何随机或时序相关的分支选择。

* `abort`（默认）：首次违规立即终止，状态 `aborted`，返回错误码、失败位置与
  已消费前缀；后续分块不再影响结果与统计。
* `truncate`：保留“最后一个合法前缀”作为输出，状态 `truncated`，同样返回错误
  细节；此后流终止。
* `skip`：丢弃导致违规的非法片段，并在**最早可重新同步的切点**恢复校验后继续；
  反馈事件的 `rejected_text` 标明被丢弃字符。若当前残片中找不到可恢复切点，则
  保留合法前缀并保持会话打开（等待后续分块可能补全 token）。注意：`length`
  上限属于硬性资源约束，**始终按 abort 处理**（不会被 skip/truncate 覆盖）。

## 分块协议

`StreamChunk{ seq, text, final }`：`seq` 是该片段在上游流中的**字符起始偏移**。

* 分块允许乱序到达：`seq` 超前的片段被缓存，缺口补齐后按序驱动校验器。
* 完全重复（相同 `seq` 与 `text`）幂等忽略，计入 `chunks_duplicate`；相同 `seq`
  但内容不同返回 `duplicate_seq`；与已消费边界部分重叠返回 `chunk_out_of_range`。
* `final=true` 表示流结束；`finish` 在存在未补齐缺口时返回
  `chunk_out_of_range`，否则做“完整性收尾校验”（如 JSON/枚举/正则是否完整）。

## HTTP 接口

| 方法 & 路径 | 说明 |
|---|---|
| `POST /constraints/{name}` | 发布命名约束的新版本，body：`{"type","params"}` |
| `GET  /constraints` | 列出约束与最新版本 |
| `POST /sessions` | 创建会话，body：`constraint`、`policy`、`constraint_version?`、`max_length?`、`max_duration_seconds?` |
| `POST /sessions/{id}/chunks` | 提交分块；正常 `200`，冲突类（违规/超长/超时/取消）`409` |
| `POST /sessions/{id}/cancel` | 取消，立即回收该请求状态 |
| `POST /sessions/{id}/finish` | 结束流并返回最终结果 |
| `GET  /sessions/{id}` | 当前结果快照 |
| `GET  /sessions/{id}/stats` | 单会话统计 |
| `GET  /stats` | 全局统计 |

错误体统一包含：`error_code`、`message`、`position`、`consumed_prefix`。

### 错误码

`constraint_violation`、`length_limit_exceeded`、`time_limit_exceeded`、
`cancelled`、`chunk_out_of_range`、`duplicate_seq`、`invalid_chunk`、
`unknown_constraint`、`constraint_type_unknown`、`session_not_found`、
`session_finished`。

### 示例

```bash
curl -X POST localhost:8000/constraints/word \
  -H 'Content-Type: application/json' \
  -d '{"type":"regex","params":{"pattern":"[a-z]+"}}'

SID=$(curl -s -X POST localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"constraint":"word","policy":"truncate"}' | jq -r .session_id)

curl -X POST localhost:8000/sessions/$SID/chunks \
  -H 'Content-Type: application/json' \
  -d '{"seq":0,"text":"hello9"}'
# 409 -> accepted_text="hello", error.position=5, consumed_prefix="hello"
```

## 兼容性边界

* **正则表达能力**：使用 `regex`（非标准库 `re`）以获得 partial fullmatch；
  回溯型/灾难性回溯的复杂 pattern 在长输入下可能带来 CPU 开销，pattern 由
  服务端注册方管控。
* **`skip` 的重同步**：通过“以已接受输出为前缀重建校验器”寻找最早可行切点；
  切点之后的输出与从头校验等价，语义确定。无法在当前残片恢复时会等待更多输入。
* **JSON 校验器**：只校验 JSON 语法（含嵌套深度上限），不做 JSON Schema 语义
  校验；数字按 JSON 词法逐字符判定。
* **长度语义**：所有长度均按 Python 字符（Unicode code point）计；统计字段
  `bytes_in/bytes_out` 按 UTF-8 字节计。
* **时间上限**：惰性判定（在 ingest/finish 时检查），不依赖实时定时器；需要
  强实时回收时可开启 `create_app(..., reaper_interval_seconds=...)` 后台回收。

## 性能与资源限制

* 校验为单次线性扫描，每字符均摊 O(1)（正则受 pattern 回溯特性影响）；乱序缓冲
  为有界字典，连续片段立即排空，不随响应长度无限增长。
* 并发规模受 `max_active_sessions` 与全局/每会话 `max_length`、
  `max_duration_seconds` 约束；取消与终止路径立即释放会话内部缓冲。
* 高并发基准见 `tests/test_concurrency.py`（数百线程混合策略/约束并发，验证
  隔离性与统计正确性）；绝对吞吐取决于运行环境，建议在目标硬件上以异步客户端
  另行压测。
