# InteretRead（正在构建中）

现有流程已经接入 FastAPI：HTTP 接口提交任务，PostgreSQL 保存状态和配置快照，Redis/Celery 唤醒独立 worker 执行原有六步流程。

## 启动 API

在 WSL/Linux 的项目根目录执行（现有 PostgreSQL、Redis 必须可连接）：

```bash
uv sync
```

首次运行前，复制示例配置并填写 `.env` 中的数据库、Redis 等连接信息：

```bash
cp .env.example .env
cp config/crawl.example.yaml config/crawl.yaml
cp config/filter.example.yaml config/filter.yaml
cp config/ai_interests.example.txt config/ai_interests.txt
cp config/ai_processing_prompt.example.txt config/ai_processing_prompt.txt
```

已有本地配置时跳过对应的复制命令，避免覆盖。`.env` 和实际 YAML 配置已被 Git 忽略；示例文件可提交。启用 AI 模式前，在 `.env` 中设置 `FILTER_AI__URL` 和 `FILTER_AI__API_KEY`，并在 `filter.yaml` 中选择模型。

AI 兴趣描述 `config/ai_interests.txt` 和处理提示词 `config/ai_processing_prompt.txt` 也被 Git 忽略，可分别从对应的 `.example.txt` 文件复制后修改。

先启动 API。它会自动创建配置指向的数据库（若尚不存在），并应用迁移。首次建库需要 PostgreSQL 账号具备建库权限，已有数据库只需具备其表的迁移权限。

分别在三个终端启动，三者都从项目根目录运行：

```bash
# 终端 1：HTTP 服务
uv run uvicorn src.main:app --host 127.0.0.1 --port 8000
```

```bash
# 终端 2：独立执行者；一次执行一轮，轮内采集和 AI 筛选仍按原设置并发
uv run celery -A src.worker:celery_app worker --loglevel=INFO
```

```bash
# 终端 3：从数据库读取投递记录，发送到 Redis；故障后可独立重启
uv run python -m src.worker
```

访问 http://127.0.0.1:8000/docs 可查看和调用接口。无需修改现有 YAML。
当前 Redis 6.0 环境使用 Celery 5.6 与 Kombu 5.6，依赖已限制版本；不能单独升级到要求 Redis 6.2 的 5.7 系列。

## API 调用

```bash
# 使用服务器当前配置创建一轮处理；立即返回 202、run_id 和查询地址
curl -X POST http://127.0.0.1:8000/api/runs \
  -H 'Content-Type: application/json' -d '{}'

# 将下面的 RUN_ID 替换为返回的 run_id
curl http://127.0.0.1:8000/api/runs/RUN_ID
curl http://127.0.0.1:8000/api/runs/RUN_ID/result

# 查看当前保留的完整资讯
curl 'http://127.0.0.1:8000/api/news?limit=20'
```

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | /api/runs | 创建完整采集、解析、筛选、统计、补字段及入库任务 |
| GET | /api/runs | 分页查询任务，可按 status 筛选 |
| GET | /api/runs/{run_id} | 查询排队/执行/终态、统计、错误数量及投递情况 |
| GET | /api/runs/{run_id}/result | 查询处理结果；排队和执行中返回 409 |
| GET | /api/runs/{run_id}/errors | 分页查询错误，包含错误 ID 和发生时间 |
| POST | /api/runs/{run_id}/cancel | 取消排队或执行中的任务；重复取消不重复写错误 |
| POST | /api/runs/{run_id}/resume | 恢复未最终提交的失败任务或执行权已过期的任务 |
| GET | /api/news | 分页查询主表完整资讯，默认只查 kept；支持标题 q、当前来源 source_id |
| GET | /api/news/{news_item_id} | 查询一条完整资讯 |
| GET | /api/news/{news_item_id}/observations | 分页查询采集历史，可按 source_id 筛选 |
| GET | /health/live、/health/ready | 进程存活、数据库连通检查；不表示 worker 或 Redis 已就绪 |

分页使用 limit（1～100）和 offset，返回 items、total、limit、offset。
资讯查询的 status 支持 kept、excluded、insufficient、failed。新运行的 excluded 只写入本轮汇总，excluded 查询仅返回此前已保存的记录。主表 source_id 是最近一次保存的观察来源，追溯已保存的观察请使用 observations 接口。

POST /api/runs 的允许参数示例：

```json
{
  "run_id": "00000000-0000-4000-8000-000000000001",
  "source_ids": ["rss-huggingface"],
  "filter_mode": "keyword",
  "keyword": {"include": ["agent"], "exclude": ["广告"], "match": "any"},
  "ai_concurrency": 2,
  "ai_batch_size": 12
}
```

所有字段可省略。source_ids 只能从服务器已启用来源中选取，示例 ID 也须在配置中启用。
不接受来源 URL、OpenCLI 命令、凭据或文件路径。
请求允许覆盖的参数优先于环境变量，再优先于 YAML、默认值；覆盖只对本轮生效，不改写配置文件。
AI 模式仍需先在服务器配置模型、兴趣与凭据，API 不接收模型密钥。
run_id 可用作客户端幂等 ID：重复 ID 返回已有任务，忽略新请求中的业务参数；要重新执行请使用新 ID。

## HTTP 认证

.env 支持以下可选设置（完整示例见 .env.example）：

```dotenv
API_AUTH_ENABLED=true
API_KEY=自行设置的访问密钥
TASK_QUEUE_NAME=interetread
TASK_WORKER_CONCURRENCY=1
TASK_POLL_INTERVAL=2
TASK_REDISPATCH_SECONDS=30
```

`TASK_WORKER_CONCURRENCY` 控制每个 Celery worker 同时执行的任务数，默认 1。修改后要重启 worker。

启用后，所有 /api 业务接口必须带 Authorization: Bearer <API_KEY>，Swagger 中使用 Authorize 输入密钥。
/docs、OpenAPI 模式说明及健康检查不要求密钥，不包含运行配置或凭据。
未启用认证时不要求密钥；启用却未设置 API_KEY 时服务拒绝启动。

## 任务与持久化

新增 task_outbox 是后台任务投递表，不存资讯：与 processing_runs 在同一事务中登记。
API 返回成功仅表示任务已持久化，并非已完成采集；不依赖 API 进程的后台函数维持任务生命。
只有 API 启动时任务会保持 queued，需要同时运行 worker 和 dispatcher。

投递进程每隔 TASK_POLL_INTERVAL 秒检查数据库，Redis 暂不可用时保留任务和脱敏错误类型，稍后重试。
投递后没有执行者领取的 queued 任务会在 TASK_REDISPATCH_SECONDS 后再次唤醒。
执行权失效的 running 任务也可以重新投递；状态、版本和心跳以 PostgreSQL 为准，不读取 Celery 结果作为业务结果。
重复投递只允许一个执行者持有有效执行权，已经提交结果的任务不会再次采集或入库。

恢复沿用原业务配置、兴趣及提示词快照，连接凭据重新读取服务器设置。仍需保持当前服务器采集配置可读取。
本轮未最终提交的正常数据仍在内存，中断恢复时重新采集筛选；已保存错误继续保留。
已提交的 success、partial、failed 以及 cancelled 任务不重新处理；重新采集或重筛使用新 run_id。
取消会先持久化状态并撤销执行权，正在等待的外部请求不保证瞬间结束；检查执行权后停止，最终提交会被拒绝。

结果接口的历史排名、筛选结果来自当轮记录，正文来自主表当前内容，不是旧正文快照。
尚未开放独立分析和报告接口，因为对应业务能力尚未实现。

## 验证范围

HTTP 认证、请求校验、配置覆盖、事务回滚、分页查询、重复提交、错误查询、取消、过期恢复和旧执行者失权提交已验证。
另外用临时数据库与隔离队列实际启动 Uvicorn、投递进程、Redis、独立 Celery worker 和本地 Feed HTTP 服务，验证完整调用链。
验证数据、临时队列和临时服务均在验证后清理；不新建持久测试目录。
外部 AI 模型、需登录的 OpenCLI 平台未在这次 API 验证中实际调用。

---

Python 流程入口：`src.service.extraction_service.run_pipeline()`。保留项目原有的 NewsNow、Feed（RSS / Atom / JSON Feed）、OpenCLI 三类采集通道。

## 处理顺序

```text
启动后同时进行：
    ├─ 固定来源立即采集 → 解析 → 拆批筛选
    └─ 关键词 / AI 兴趣生成搜索词 → 需要自动查询词的来源采集 → 解析 → 拆批筛选

全部来源和筛选批次结束 → 汇总统计 → 补齐身份等字段 → 最终入库
```

采集与解析按来源连续执行；一个来源完成后立即拆批进入筛选队列，不等待慢来源。队列有容量限制，避免无限投递模型请求。来源并发执行，最终列表仍按配置顺序排列，不按完成顺序排列。

AI 筛选默认最多并发 2 批。在 .env 中用 FILTER_AI__CONCURRENCY 设置同时处理的批次数（1～8），用 FILTER_AI__BATCH_SIZE 设置每批最多条数（1～200，默认 12，长文本可能提前拆批）。批大小的环境变量覆盖 YAML 的 ai.batch_size；代码显式传入 run_pipeline(filter_concurrency=...) 时覆盖并发环境设置。关键词筛选很轻，使用一个筛选消费者。OpenCLI 共用浏览器会话，继续串行执行。采集并发仍由现有 crawl.concurrency 和 RAJ.concurrency 控制，无需改 YAML。

例如 .env 可设置：

```dotenv
FILTER_AI__CONCURRENCY=2
FILTER_AI__BATCH_SIZE=12
```

每轮启动时读取，已启动或恢复中的批次沿用自己的配置快照。改变并发数会改变 asyncio.TaskGroup 启动的消费者任务数量；每个任务从共享队列取批次并等待模型响应，完成后领取下一批。改变批大小只影响切分，不改变消费者数量。这是异步 I/O 并发，不是启动对应数量的 CPU 进程。

同一个来源的数据也可以拆成多批并发筛选。批次筛选失败只影响该批，错误立即关联来源和条目位置保存。所有批次结束后完成全轮统计、补字段和最后事务入库。正常数据仍保存在内存，本次未改成每批提交数据库。

每个步骤处理自己的异常：单条失败不阻塞其他条目，来源失败不阻塞其他来源。错误在该来源或批次确认失败后独立提交，不等最终业务入库。正常数据在内存中处理，最后一次事务提交资讯、快照、精简筛选结果和本轮统计。

开始只登记一条批次记录及配置快照，用于关联及时保存的错误；不是每步保存正常数据。数据库暂时不可用时写结构化应用日志，最终提交前再独立尝试补存错误。日志明确标记未保存，不能把日志当成成功入库。

## 运行

直接用 Python 执行流程前，先启动过 API 完成数据库初始化。已有数据库也可以手动应用迁移：

```bash
uv sync
uv run alembic upgrade head
```

从项目根目录调用：

```python
import asyncio
import logging
from src.service.extraction_service import run_pipeline

logging.basicConfig(level=logging.INFO)
result = asyncio.run(run_pipeline())
print(result.run_id, result.status)
print(result.summary)
```

默认读取现有配置，不改写 YAML、.env、兴趣文件或提示词文件。`src.service.extraction_service.collect_and_store()` 同样执行上述流程；返回的资讯列表只含筛选保留且成功补字段的资讯。

`src.pipeline.collect` 中的 `collect_result()`、`collect()` 和 `collect_json()` 只执行采集和解析。需要完整筛选和入库时使用 `run_pipeline()`。

`src/` 按处理阶段组织。`pipeline/collect.py` 采集，`pipeline/parse.py` 解析，`pipeline/filter.py` 筛选，`pipeline/enrich.py` 补字段，`db/save_run_results.py` 负责最终入库，`service/run_service.py` 负责任务管理、资讯与结果查询。`service/extraction_service.py` 串起全流程，计算本轮汇总，并写入 `processing_runs.summary`。`db/failures.py` 接收并独立保存各阶段上报的错误，`db/database.py` 定义并初始化表。`ai_client.py` 封装模型调用，`router.py` 接 HTTP 请求，`schemas.py` 定义请求和响应模型，`main.py` 启动 FastAPI，`worker.py` 包含后台任务执行和投递入口。

## 仅改 YAML 能改什么

- NewsNow：增删来源 ID、修改开关和条数；请求同一配置 API，通过来源 ID 选榜单。
- Feed：增删 RSS、Atom、JSON Feed 订阅地址、修改开关和条数。
- OpenCLI：修改站点、命令、参数、查询词、字段映射和链接模板。站点和命令必须是已安装 OpenCLI 支持的命令，其输出须符合 OpenCLI 的成功结果结构。
- 筛选：切换 keyword / ai，调整包含和排除规则、类别、搜索拆分大小及 AI 设置。

上述操作使用现有 YAML 结构，不需要修改采集器和解析器。字段映射使用已有的统一字段集合；新增一种传输协议、改变 NewsNow 接口结构或扩展统一字段集合，仍属于代码变更。

OpenCLI 的 `fields` 映射直接读取返回条目的字段；支持现有配置中的 `source_updated_at` 和 `version_summary`。映射必须与实际命令输出一致。

NewsNow 和 Feed 入口固定，先采候选再筛选。OpenCLI 固定列表同样先采后筛；带 `{query}` 的搜索命令先确定搜索词。手填 `query` 时直接使用；没有手填时从关键词类别或 AI 兴趣计划生成。每个来源的多个查询结果合并去重，最终仍受该来源 limit 限制。

来源关闭时不请求，也不计作失败。搜索词规划失败只影响需要自动查询词的来源，不影响固定列表和订阅来源。

搜索词规划与固定来源采集并发进行；只有需要自动查询词的来源等待规划。固定来源优先领取采集任务，避免搜索来源等待时占满共享任务。规划失败只隔离受影响的搜索来源，取消会同时终止规划与采集。AI 规划和筛选共用并发额度，合计不会超过 FILTER_AI__CONCURRENCY；OpenCLI 内部仍统一串行。

## 筛选

关键词匹配标题、来源摘要和来源正文，英文忽略大小写。排除词优先；逗号表示或，& 表示且，`match: all` 要求所有包含分支都满足。一条资讯可归入多个类别；空 include 表示不限制包含词，但没有正向词时无法自动生成平台搜索查询。

AI 使用现有兴趣和提示词文件，按 batch_size 和文本长度拆批。只有需要生成搜索词时才调用 plan；固定来源无需等待 plan，process 可以先按原始兴趣判断候选。计划完成后的批次再附带 plan。上下文截断只影响提交给模型的文字，不覆盖原采集内容。

严格输出方式：

- DeepSeek：严格函数工具输出，`strict: true`，要求配置支持严格模式的 beta 完整请求地址。见 [DeepSeek 官方说明](https://api-docs.deepseek.com/guides/tool_calls/)。
- zhipu：发送严格 `json_schema` 请求；所选接口和模型必须支持它，不支持则明确失败，不退化到普通 JSON mode。
- codex：本地 `codex exec --output-schema`，使用临时目录、只读环境和超时控制。

结构约束之外，程序核对输入输出编号是否一一对应、保留结果是否有摘要和标签、排除结果是否为空摘要等。网络超时、429、5xx 有限退避重试；无效结果按 format_retries 修正，再尝试备用模型。每次请求有 timeout。整批最终失败只保存一份批错误，关联受影响编号，其他批次继续。

筛选结果分为 kept、excluded、insufficient、failed。排除和信息不足不是调用错误，不能把失败当成排除。

## 四张业务表

| 表 | 保存内容 | 保存时间 |
|---|---|---|
| processing_runs | 整轮状态、时间、配置快照、执行权、来源统计和总统计 | 开始登记，最后更新 |
| processing_errors | 所有阶段错误的时间、来源、条目定位、原因及重试次数 | 错误确认后独立提交 |
| news_items | 完整资讯主表：标题、URL、正文、摘要、作者、来源、排名、当前筛选结果和 AI 摘要等 | 最终事务 |
| collection_observations | 补充记录：运行关联、来源、排名及每轮处理结果；不保存旧正文 | 最终事务 |

来源统计用于区分“没取到数据”“有数据但全被排除”“请求失败”等情况，保存为 processing_runs.source_results 中的列表，无需单独来源统计表。source_failures 保存最终来源级失败摘要；详细、及时的错误保存在 processing_errors。

筛选结束后、最终入库前，按排除理由统计 `excluded` 条数，写入本轮 `summary.excluded_by_reason`，通过 `GET /runs/{run_id}` 查看。`excluded` 不逐条写资讯主表或采集观察记录，新运行的结果接口也不返回其逐条明细。其余解析有效的候选（kept、insufficient、failed）仍更新资讯主表和采集观察记录。解析失败的数据保存错误原因和来源位置，不保存整份原始 HTTP 响应。原始凭据和请求头不入库。

news_items 集中保存当前完整资讯，不同来源的相同 URL + 标题共用身份。正文、摘要等取最新非空内容，较旧执行结果不会覆盖更新的主表信息；来源字段标示最近一次观察，具体来源追踪见采集补充记录。采集补充记录通过 news_item_id 关联主表。URL 或标题变化会产生新身份，正文及排名变化不会改变身份。同一身份允许同轮多个来源或多轮采集记录。

查看和后续分析直接查询 news_items，使用 filter_status = 'kept' 取得当前保留资讯；load_news_item() 返回一条完整记录。load_collection_result() 仍只返回保留资讯；load_run() 同时返回所有候选的筛选结果和本轮统计。

错误定位：parse 使用来源原始列表从 1 开始的位置；流水线 filter 错误 key 为 source_filter，使用该来源解析有效列表从 1 开始的位置；enrich 使用从 0 开始的本轮统一列表编号，对应采集记录 position - 1。搜索查询内的解析错误另带查询序号 key。

最终事务将来源概览、资讯身份、全部候选及筛选结果、最终统计一起提交。任一步失败全部回滚，之前已独立提交的错误仍保留。processing_runs.collected_at 标识最终业务数据是否已提交，不再需要第二张采集批次表。来源为空时可以只有运行记录；全部正常排除也属于成功。

## 统计口径

- raw_count_known：已知原始条目数的合计；未知部分不当成 0，另列 raw_count_unknown_sources。
- examined_count：实际检查过的条目数。
- parse_valid_count：进入筛选的有效条目数，尚未按身份去重。
- parse_skipped_count：实际检查后被判无效的条目数。
- unexamined_count_known：已知因采集上限未检查的条目数。
- search_duplicate_count / search_limited_count：多个搜索查询合并时去掉的重复项和超限项。
- filter：保留、排除、信息不足、筛选失败的数量。
- enrich_failed_count：筛选保留但补字段失败的数量。
- excluded_by_reason：各排除理由对应的条数。关键词筛选按固定理由归类，AI 筛选按模型返回的理由分组，合并仅空白不同的写法。
- stored_observations / stored_unique_identities：本轮实际保存的候选记录数及不同身份数。排除项只计入汇总；已存在并更新的身份也计入。
- kept_observations / kept_unique_identities：最终保留记录数和其中的不同身份数。
- error_records / errors_by_stage：错误记录数，不是失败资讯数。一次整批失败是一条错误；同一坏条目可以包含多个校验原因。

第四步只计算这些结果，不重新筛选，不复制前面保存的错误。补字段结果和最终保存数量在第五步后补入最终概览。

summary.execution 提供实际批次数、筛选峰值并发，以及首次开始筛选、采集全部完成、筛选全部完成的相对耗时。它们可用于检查采集和筛选是否确实重叠执行。

## 重复执行、取消和恢复

相同 run_id 的已提交结果直接读回，包括完整保存的失败结果；不重新请求来源和模型。相同批次正在执行时拒绝第二次领取。

执行权有有效期和心跳，每次错误写入、最终提交、状态更新都校验版本。过期后可以重新领取，旧执行者不能提交。取消会持久化状态并使旧执行版本失效。

尚未最终提交而失败的批次，可用原 run_id 重试，并沿用原业务配置及兴趣/提示词快照。凭据和模型连接地址不写入快照，使用当前安全配置；NewsNow 请求头也不写入快照。

正常数据留在内存，所以中断后未提交的正常数据需要重新采集和筛选；本版不做逐条进度恢复。已经完整提交的 partial 结果不会自动重筛失败条目，新一轮处理使用新 run_id。

## 数据库初始化与范围

迁移 20261120_05 建立四张业务表，20261120_06 补齐分析字段与索引，20261120_07 增加 queued 状态及 task_outbox 投递表。API 启动时自动应用迁移，`uv run alembic upgrade head` 可用于手动迁移已有数据库。`alembic_version` 是版本管理表。

当前默认业务库 interetread 已与项目适配：news_items 保存完整资讯供查看和分析直接读取；collection_observations 保存每次来源、排名及处理结果等补充信息；processing_runs 与 processing_errors 的结构保持不变。正文保留最新非空值，不再逐轮保存旧正文。

本次验证产生的四条候选及一条运行记录已清空。旧迁移脚本、测试目录和验证数据已删除，不创建备份。代码可以通过 python -m compileall src migrations 检查语法。

真实模型和需要登录的 OpenCLI 平台尚未实际联调。报告、通知、定时业务调度和深度分析 Agent 尚未实现；HTTP API 与后台任务投递已实现。


## 面向分析的字段口径

四张表职责不变，processing_runs 与 processing_errors 结构不变。两个资讯表的每个字段都有数据库注释。

- news_items.content_length：数据库自动计算的当前正文字符数，不需要采集器或 AI 填写。0 表示没有正文；可结合 source_excerpt 区分只有标题、只有摘要和有正文。非零也不保证取得全文。
- collection_observations.observed_at：本次来源数据开始统一解析的 UTC 时间，不是整个运行最终提交的时间。同一来源响应的条目使用同一时间；一个来源多次搜索响应可以有不同时间。
- news_items.first_seen_at / last_seen_at：按观察时间取最早和最晚值。较晚提交的旧响应不会覆盖较新来源元数据；正文等仍保留最新非空值。
- collection_observations.content_hash：本次来源正文归一空白后的 SHA256。只比较同一 news_item_id、同一 source_id 的非空指纹；不同说明采到的正文文字不同，不能据此还原旧正文，也不能断言原网页已修改。无正文时为空，不能据此判断内容变化。
- rank 是来源内部排名，source_position 是原始列表位置；只有 rank 适合比较同一来源的排名变化，不同来源的排名不能直接比较。
- source_excerpt / version_summary 是来源信息，ai_summary 是模型判断。未提供的发布时间、更新时间、作者或排名保持空值，不能用采集时间或 0 冒充。

旧采集记录没有准确观察时间或历史正文指纹时保持为空，不根据当前正文伪造历史。正文指纹不参与去重；资讯身份仍只由 URL + 标题生成。
时间、筛选状态与发布时间、同一资讯同一来源的观察历史分别建立索引。主题分布可以统计 tags；AI 趋势、热度结论需要进一步分析，字段本身不生成热度分数。
