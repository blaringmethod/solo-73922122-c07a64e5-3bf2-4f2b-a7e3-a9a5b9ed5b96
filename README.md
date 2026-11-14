# 公共采购公告采集系统

面向招标数据员的公告采集交付件：FastAPI 采集器 + 带限流/延迟开关的本地模拟采购站，
Docker Compose 一键启动，浏览器即可启动采集、查看实时进度、核对同号公告两栏采购单位、
停止任务并下载 JSON（含核验表）。

## 目录结构

```
.
├── docker-compose.yml          # 两服务编排：procurement-site + collector
├── collector/                  # 采集器（FastAPI + httpx + BeautifulSoup）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API、核验表端点、JSON 下载、采购站同源代理、静态页挂载
│       ├── crawler.py          # 采集引擎：并发闸门、429 全站冷却、停止回收、同号两栏核验
│       ├── parser.py           # BeautifulSoup 列表/详情解析（失败抛 ParseError）
│       └── static/index.html   # 浏览器控制台（单文件，原生 JS，含同号核验表）
├── site/                       # 本地模拟公共采购站
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 栏目/分栏目详情页、滑动窗口限流、延迟开关、管理页
│       └── data.py             # 确定性内存数据集（跨栏目重复编号、同号异单位样例、1 个坏详情页）
└── tests/                      # 契约测试与解析器单测（标准库 + venv 依赖）
```

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集控制台 | http://localhost:8000/ | 启动/停止/进度/同号核验表/事件/下载 JSON |
| 采购站（同源代理入口） | http://localhost:8000/site/ | 浏览器入口，经采集器反代到站点 |
| 采购站限流管理 | http://localhost:8000/site/admin | 限流阈值、窗口、延迟毫秒开关 |
| 采购站直连 | http://localhost:8080/ | 仅排障时使用 |

## 采集规则与实现对照

| 需求 | 实现 |
| --- | --- |
| 固定站点两个栏目，遍历分页列表及详情 | `Crawler` 配置 `/tenders`、`/awards`；首页解析 `data-total-pages` 后入队其余列表页，再逐条入队详情页 |
| 同号公告两栏详情分别抓取 | 详情页按栏目分开（`/tenders/notice/{code}`、`/awards/notice/{code}`）；同号在两栏各入队一次详情，栏内重复编号才跳过 |
| 同号核验表（一致/冲突/单栏/待核验） | `Crawler.verification()` 按编号汇总两侧抓取结果；`GET /api/verification` 与控制台核验表面板展示 |
| 两侧详情都成功才判一致/冲突 | 任一侧失败或未抓取（含停止）即 `pending`（待核验），见 `_conclusion_for` |
| 两栏列表采齐才判单栏 | `lists_complete` 要求两栏列表页全部成功；缺页或停止导致无法确认另一栏时保持 `pending` |
| 公告 JSON 按编号去重并附核验表 | `_records` 按编号去重（两栏都成功时招标栏优先，招标栏失败由中标栏补入）；`GET /api/records/download` 的 JSON 附 `verification` 块供下游筛选 |
| 提取标题、采购单位、截止日期 | `parser.parse_detail_page`，缺任一字段即 `ParseError` |
| 可下载 JSON | `GET /api/records/download`，附件文件名带轮次 ID |
| 单进程同时只采一轮 | 单 uvicorn worker + `round_lock`；重复启动返回 `409` |
| 最多两个请求在途 | 全站共享 `asyncio.Semaphore(2)`，两个栏目共用 |
| 429 按 Retry-After 整数秒全站暂停 | `_extend_cooldown` 维护单一冷却钟，`_pass_gate` 拦截所有后续请求（含另一栏目与重试）；后续 429 只延长不缩短；已在途请求允许收尾 |
| 每页最多尝试三次 | 列表/详情统一 `_get_parsed_page`，传输错误、4xx/5xx、解析失败均消耗尝试次数 |
| 耗尽/解析失败须列出未完成页，不能报整轮成功 | 失败页进入 `unfinished`；轮次结束状态为 `partial` 而非 `completed` |
| 停止阻止排队/冷却请求，关闭在途请求并回收连接后才显示已停止 | `stop_round` 先置停止位，再 `aclose()` 关闭在途请求与连接池，`await` 轮次协程收尾后状态才变 `stopped`，随后重建客户端允许新轮 |
| 旧响应不能写入新轮 | 每轮生成 `round_id`，冷却唤醒与回写前校验；新轮重建客户端、状态、记录与核验容器 |

轮次状态机：`idle → running → completed | partial | stopped`（停止过程中短暂为 `stopping`）。

## 同号核验表

数据员向分析库交付前，用核验表核对两栏同号公告的采购单位，避免先抓到的详情掩盖冲突。
每个编号一行，结论四选一（JSON 字段 `conclusion`，中文标签 `conclusion_label`）：

| 结论 | 含义 | 判定条件 |
| --- | --- | --- |
| `match` 一致 | 两栏同号，采购单位相同 | 两栏列表均出现且两侧详情都抓取成功 |
| `conflict` 冲突 | 两栏同号，采购单位不同 | 两栏列表均出现且两侧详情都抓取成功 |
| `single_column` 单栏 | 仅一栏出现该编号 | 两栏列表页全部采齐（缺一栏未采齐则不可判） |
| `pending` 待核验 | 无法下定论 | 任一侧详情失败/未抓取，或列表缺页、轮次被停止 |

- 记录 JSON（`records`）始终按编号去重，冲突编号的两侧单位以核验表为准；
- 下载的 JSON 附 `verification` 块（`lists_complete`、`counts`、`rows`），下游可按
  `conclusion` 筛选后再入库；
- 控制台核验表支持按结论筛选，冲突行两栏单位标红，事件日志实时提示同号冲突。

## 模拟采购站说明

- 数据集：36 条招标公告（6 个列表页）+ 24 条中标公告（4 个列表页），
  中标公告复用前 24 个招标编号，用于验证跨栏目去重与同号核验；
- 详情页按栏目分开：`/tenders/notice/{code}` 与 `/awards/notice/{code}` 各自独立；
- `ZB-2026-018` 为同号异单位样例：招标栏与中标栏详情页采购单位不同，核验表判「冲突」；
- `ZB-2026-010` 招标栏详情页故意缺少“采购单位/截止日期”（中标栏同号页面正常），
  用于验证三次解析失败后进入未完成页面列表、整轮标记为 `partial`，且该编号核验判「待核验」；
- 限流为滑动窗口，默认 10 秒窗口 12 个请求，超限返回 `429` 与整数 `Retry-After: 5`；
- `/admin` 可运行时切换限流开关、窗口容量、窗口秒数与每请求延迟毫秒，
  保存即重置计数，也可单独重置计数。

## 推荐演示流程

1. `docker compose up --build`，打开 http://localhost:8000/ ；
2. 直接点“启动一轮采集”：观察到 `partial`——36 条记录（`ZB-2026-010` 由中标栏详情补入）、
   招标栏 `ZB-2026-010` 详情在未完成列表中；核验表 22 条一致、1 条冲突（`ZB-2026-018`）、
   12 条单栏（`ZB-2026-025` 起）、1 条待核验（`ZB-2026-010`）；
3. 在 `/site/admin` 把窗口容量调到 3：再启动一轮，观察控制台“全站冷却剩余”倒计时，
   冷却期间两个栏目均无新请求完成；
4. 冷却进行中点“停止”：状态在 1 秒内变为 `stopped`，排队/冷却中的页面进入未完成列表；
   列表未采齐时核验表中仅见一栏的编号全部保持「待核验」，不误判单栏；
5. 立即再启动一轮：状态、记录与核验表完全重置，证明旧轮响应不会写入新轮；
6. 点“下载 JSON”获取结构化结果（含未完成页清单与核验表，下游可按 `conclusion` 筛选）。

## 本地测试（无需 Docker）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r collector/requirements.txt
.venv/bin/python -I tests/test_parser.py
.venv/bin/python -I tests/test_crawler_contract.py
```

契约测试用内存假上游验证：在途请求峰值恰为 2、429 后整站等待不少于
Retry-After 且重试可恢复、冷却中停止后零请求泄漏、停止后新轮干净无旧数据，
以及核验表四规则——两侧成功判一致/冲突、列表采齐判单栏、缺页或停止保持待核验。
