# Constrained Stream Validator (Python)

面向大模型输出的**流式校验与修复服务**。上游模型以字符/标记流持续输出，服务在
**不缓存完整响应**的前提下逐字符判断输出是否满足已注册约束，并在违规时按可配置
策略处理（中止 / 截断 / 丢弃继续），同时维护约束版本、进行中请求状态与聚合统计。

基于 FastAPI，运行时核心逻辑不依赖异步，可在线程模型下被高并发安全调用。

---

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

最小调用流程：

```bash
# 1) 注册一个枚举约束（自动分配版本 1）
curl -X PUT localhost:8000/constraints/answer \
  -H 'content-type: application/json' \
  -d '{"type":"enum","config":{"values":["yes","no"]}}'

# 2) 创建一个采用 drop 策略的校验会话
SID=$(curl -s -X POST localhost:8000/sessions \
  -H 'content-type: application/json' \
  -d '{"constraint":"answer","policy":"drop"}' | python -c 'import sys,json;print(json.load(sys.stdin)["session_id"])')

# 3) 按 seq 顺序推送分块（可乱序、可重复）
curl -s -X POST localhost:8000/sessions/$SID/chunks \
  -H 'content-type: application/json' \
  -d '{"seq":0,"text":"yeXs"}'
# -> {"status":"dropped","emitted":"yes","dropped_chars":1, ...}

# 4) 结束流
curl -s -X POST localhost:8000/sessions/$SID/chunks \
  -H 'content-type: application/json' -d '{"seq":1,"final":true}'
# -> {"status":"finalized", ...}
```

直接使用 Python API：

```python
from stream_validator.registry import ConstraintRegistry
from stream_validator.manager import SessionManager
from stream_validator.constraints.enum_like import EnumConstraint
from stream_validator.models import Policy

reg = ConstraintRegistry()
reg.register("answer", EnumConstraint("answer", 0, ["yes", "no"]))
mgr = SessionManager(reg)

snap = mgr.create_session("answer", Policy.DROP)
fb = mgr.feed(snap.session_id, seq=0, text="yeXs")
print(fb.emitted, fb.dropped_chars)   # yes 1
mgr.feed(snap.session_id, seq=1, text="", final=True)
```

---

## 2. 约束类型

约束按**名称**引用并携带**整数版本号**。同名约束每次注册产生新版本（`1,2,…`），
注册表返回版本不可变对象：

- **新请求**默认使用最新版本，也可通过 `version` 显式钉住某一版本；
- **已开始的请求**在创建时即持有该版本对象的直接引用，约束更新/删除对其无影响。

| type | 配置 | 说明 |
| --- | --- | --- |
| `max_length` | `{"max_length": 1024}` | 输出字符数（非字节数）上限 |
| `regex` | `{"pattern": "\\d{3}-[a-z]+", "max_length": 1000000}` | 整段输出必须完全匹配；流式判定基于正则的 *partial match* |
| `enum` | `{"values": ["yes","no"]}` | 输出必须完整等于某个枚举值（基于只读 trie 的前缀匹配） |
| `json` | `{"required_keys": ["id"]}` | 流式 JSON 良构校验（下推自动机，逐字符），可选校验顶层必填键 |

所有约束对象对请求只读共享；每个请求拥有独立的、隔离的可变状态。

### 流式判定原理

- `max_length`：仅保留计数器，O(1) 内存。
- `enum`：共享只读字符 trie，请求内仅记录当前节点。
- `json`：确定性下推自动机（模式栈 + 字符串转义状态），任何合法 JSON 的
  每个前缀都被接受；非法字符在到达当下即被检出。
- `regex`：使用第三方 [`regex`](https://pypi.org/project/regex/) 库的
  `fullmatch(..., partial=True)`。已消费前缀若**不能再扩展**为完整匹配即判违规；
  结束时要求完整匹配。正则请求保留“已消费前缀”，其长度被 `max_length` 严格封顶。

---

## 3. 违规处理策略（Policy）

策略在创建会话时指定，对同一输入与同一策略结果**确定**（无随机、无并发依赖）：

| 策略 | 行为 | 会话终态 |
| --- | --- | --- |
| `abort`（默认） | 遇到第一个非法字符立即停止，该字符及后续字符不写入结果 | `aborted` |
| `truncate` | 回退到最后一个合法前缀并结束流（失败判定是原子的，非法字符不入状态） | `truncated` |
| `drop` | 丢弃非法字符后继续校验，尽可能产出合法输出；无法修复的字符持续被丢弃 | 正常则 `completed` |

违规时反馈中包含可区分的字段：

- `violation.code`：机器可判定的错误码；
- `violation.offset`：非法字符在**输出坐标空间**中的位置（已发射字符数 + 本块内已丢弃数）；
- `violation.consumed_prefix`：违规时已消费的合法前缀（长度受预览上限裁剪）。

### 错误码

`constraint_violation`、`length_exceeded`、`regex_mismatch`、`enum_mismatch`、
`invalid_json`、`incomplete_output`、`cancelled`、`timeout`、`session_not_found`、
`constraint_not_found`、`concurrency_limit`、`reorder_overflow`、
`session_closed`、`bad_request`。

### 非致命约束（fatal=false）

注册约束时可在 config 中设置 `"fatal": false`（默认 `true`）。非致命约束下，
策略不再终止流：每个非法字符被跳过并计为一条**警告**（`ChunkStatus.WARNED`、
`warning_count` 增加，HTTP 状态仍为 200），流继续；结束时即使输出不完整也会
正常 `completed`，不完整仅追加一条警告。适用于“宁可继续产出也要观测偏差”的降级
场景。致命与非致命在同一输入下同样保持确定。

HTTP 状态码映射：校验类 422；超时 408；会话冲突/已关闭 409；不存在 404；
并发上限 503；请求格式 400。

---

## 4. 分块顺序、重复与取消

- 每个分块带单调非负整数 `seq`，从 `0` 开始。
- **乱序**：`seq > next_seq` 的分块进入有界乱序缓冲（默认窗口 32 块）；缺口闭合
  时自动按序排空，排空产生的发射/丢弃/终态聚合到触发分块的反馈中。
- **重复 / 迟到**：`seq < next_seq` 的分块被幂等忽略，返回 `duplicate`，
  **不写入结果，也不计入任何字符统计**。
- **超出乱序窗口**：返回 `reorder_overflow`（422），不缓冲，防止无限占用内存。
- **取消 / 断连**：`POST /sessions/{id}/cancel` 立即将会话置为 `cancelled`，
  清空乱序缓冲，之后到达的分块一律返回 `cancelled`（409）且不落盘、不入统计。
- 会话支持可选 `timeout`（秒）：超时后惰性失效，并有后台 reaper 周期回收
  （也可通过 `reap_once()` 手动驱动）。

---

## 5. 并发隔离与资源边界

- 约束配置跨请求**只读共享**；每个请求的校验状态、缓冲、前缀、计数器互不影响。
- 管理器用一把短临界区锁保护会话表与聚合统计，**不持有锁执行用户校验**，
  吞吐不被串行化；会话内部有自己的锁，对同一会话的并发分块调用会被串行化，
  乱序提交下最终结果仍按 `seq` 确定。
- **有界内存**：
  - 每会话只保留 `max_prefix_preview`（默认 4096 字符）的发射前缀用于错误反馈；
  - 乱序缓冲最多 `reorder_window` 块；
  - 长度/正则约束有字符上限；JSON 栈深度受长度上限约束；
  - `max_active` 限制并发活动会话数，超限返回 503；
  - 已关闭会话在 `retention`（默认 60 秒）后被回收，也可随时 `remove()`。

---

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| PUT | `/constraints/{name}` | 注册新版本约束 |
| GET | `/constraints` | 列出全部约束与版本 |
| GET | `/constraints/{name}?version=` | 查询某版本（默认最新） |
| DELETE | `/constraints/{name}` | 删除名称（进行中会话不受影响） |
| POST | `/sessions` | 创建会话 `{constraint, policy, version?, timeout?}` |
| POST | `/sessions/{id}/chunks` | 推送分块 `{seq, text, final}` |
| POST | `/sessions/{id}/cancel` | 取消会话 |
| GET | `/sessions/{id}` | 查询会话状态/快照 |
| GET | `/sessions?include_closed=` | 列出会话 |
| GET | `/stats` | 聚合统计 |

---

## 7. 兼容性边界

- Python ≥ 3.14（使用 `X | Y` 联合类型与 `enum` 标准行为）。
- 长度按 **Unicode 码位**计数（Python `str` 字符），不按 UTF-8 字节；调用方若需
  字节上限需自行换算。
- `regex` 约束的流式判定依赖底层引擎的 partial 匹配；病态回溯型正则（如大量嵌套
  量词）仍可能带来 CPU 开销，应在注册侧限制模式复杂度。
- JSON 校验器是面向流式的良构性检查器，不解析为对象、不做 schema 类型校验；
  `required_keys` 仅检查顶层键是否出现（重复键视为非法）。
- 反馈中的 `consumed_prefix` 是**预览**而非完整响应（受上限裁剪）；本服务设计上
  不持有完整响应，调用方应根据自己收到的分块反馈重建完整输出。
- 每个分块必须是 `str`；多字节字符若在网络层被截断，应在上游按字符边界组帧后再送入。

## 8. 性能限制

- 校验为逐字符处理：长度 O(1)/字符；枚举 O(1)/字符；JSON O(1)/字符；
  正则每字符可能触发一次 partial 匹配，成本取决于模式（前缀缓冲有长度上限）。
- 单会话处理在其锁内串行，这是保证乱序/并发分块确定性的前提；不同会话完全并行。
- 统计为进程内计数器，非分布式；多副本部署时需要外部聚合。
- 默认 reaper 为守护线程；嵌入使用时可传 `reap_interval=None` 关闭，改由调用方
  驱动 `reap_once()`，并在退出时 `shutdown()`。

---

## 9. 测试

```bash
uv run pytest
```

覆盖：合法/非法输出（四类约束参数化）、乱序与重复分块、乱序窗口溢出、中途取消、
惰性/后台超时与保留期回收、并发会话隔离、同会话并发分块、约束版本切换、
确定性（同输入同策略）、长度/预览/缓冲/活动会话数等资源上限。
