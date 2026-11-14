"""End-to-end concurrency / cooldown / stop / verification test using the real
Crawler against a fake upstream server (no external processes required).

Run with the project venv:
  python -I tests/test_crawler_contract.py
"""

import asyncio
import sys
import time

sys.path.insert(0, "/workspace/collector")

from app.crawler import MAX_IN_FLIGHT, Crawler  # noqa: E402

TENDER_CODES = ["ZB-2026-001", "ZB-2026-002", "ZB-2026-003", "ZB-2026-004", "ZB-2026-005"]
AWARD_CODES = ["ZB-2026-001", "ZB-2026-002", "ZB-2026-003", "ZB-2026-004"]
CONFLICT_CODE = "ZB-2026-002"  # award column carries a different unit
AWARD_CONFLICT_UNIT = "中标复核单位"
NORMAL_UNIT = "测试单位"


class FakeUpstream:
    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0
        self.total_hits = 0
        self.lock = asyncio.Lock()
        # First N requests after this gate are answered with 429.
        self.reject_next = 0
        self.retry_after = 3
        # When true, award list page 2 always fails with HTTP 500.
        self.fail_award_page2 = False

    async def handle(self, scope, receive, send):
        path = scope["path"]
        if path == "/health":
            await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"ok"})
            return

        async with self.lock:
            self.inflight += 1
            self.total_hits += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
        assert self.inflight <= MAX_IN_FLIGHT, f"concurrency violated: {self.inflight} in flight"

        await asyncio.sleep(0.15)  # keep requests overlapping

        try:
            if self.reject_next > 0:
                self.reject_next -= 1
                await send({
                    "type": "http.response.start",
                    "status": 429,
                    "headers": [(b"retry-after", str(self.retry_after).encode()),
                                (b"content-type", b"text/html")],
                })
                await send({"type": "http.response.body", "body": b"<html>429</html>"})
                return

            if self.fail_award_page2 and path.startswith("/awards") and "page=2" in scope.get("query", ""):
                await send({"type": "http.response.start", "status": 500,
                            "headers": [(b"content-type", b"text/html")]})
                await send({"type": "http.response.body", "body": b"<html>500</html>"})
                return

            if path.startswith("/tenders/notice/") or path.startswith("/awards/notice/"):
                section = "tender" if path.startswith("/tenders") else "award"
                code = path.rsplit("/", 1)[-1]
                body = self._detail_html(section, code)
            elif path.startswith("/tenders") or path.startswith("/awards"):
                page = 2 if "page=2" in scope.get("query", "") else 1
                body = self._list_html(path, page)
            else:
                body = "<html>not found</html>"
                await send({"type": "http.response.start", "status": 404,
                            "headers": [(b"content-type", b"text/html")]})
                await send({"type": "http.response.body", "body": body.encode()})
                return

            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"text/html; charset=utf-8")]})
            await send({"type": "http.response.body", "body": body.encode()})
        finally:
            async with self.lock:
                self.inflight -= 1

    @staticmethod
    def _list_html(path: str, page: int) -> str:
        if path.startswith("/tenders"):
            section, codes = "tender", TENDER_CODES
        else:
            section, codes = "award", AWARD_CODES
        page_codes = codes[:3] if page == 1 else codes[3:]
        rows = "".join(
            f'<tr class="announcement"><td class="code">{code}</td>'
            f'<td><a href="/{section}s/notice/{code}">公告{code}</a></td></tr>'
            for code in page_codes
        )
        return (
            '<html><body><table id="announcement-list"><tbody>'
            + rows
            + '</tbody></table><nav class="pager" data-total-pages="2"></nav>'
            + f'<a href="/{section}s?page=2">2</a></body></html>'
        )

    @staticmethod
    def _detail_html(section: str, code: str) -> str:
        unit = NORMAL_UNIT
        if section == "award" and code == CONFLICT_CODE:
            unit = AWARD_CONFLICT_UNIT
        return f"""<html><body><h1 id="detail-title">标题 {code}</h1>
        <table class="detail-table">
        <tr><th>公告编号</th><td>{code}</td></tr>
        <tr><th>采购单位</th><td>{unit}</td></tr>
        <tr><th>截止日期</th><td>2026-12-31 17:00</td></tr>
        </table></body></html>"""


def conclusions_by_code(crawler: Crawler) -> dict:
    return {row["announcement_code"]: row["conclusion"] for row in crawler.verification()}


async def main() -> None:
    upstream = FakeUpstream()
    port = 18099
    ready = asyncio.Event()

    async def _pipe(reader, writer):  # noqa: ANN001
        # Minimal HTTP/1.1 server sufficient for httpx GETs.
        while True:
            line = await reader.readline()
            if not line:
                break
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.partition(b":")
                headers[k.strip().lower()] = v.strip()
            cl = int(headers.get(b"content-length", b"0"))
            if cl:
                await reader.readexactly(cl)

            collected: dict = {}

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                if message["type"] == "http.response.start":
                    collected["status"] = message["status"]
                    collected["headers"] = message["headers"]
                elif message["type"] == "http.response.body":
                    collected["body"] = message.get("body", b"")
                    writer.write(b"HTTP/1.1 " + str(collected["status"]).encode() + b" OK\r\n")
                    for k, v in collected["headers"]:
                        writer.write(k + b": " + v + b"\r\n")
                    writer.write(b"content-length: " + str(len(collected["body"])).encode() + b"\r\n\r\n")
                    writer.write(collected["body"])
                    await writer.drain()
                    writer.close()

            raw_target = line.split(b" ")[1].decode()
            path, _, query = raw_target.partition("?")
            scope = {"type": "http", "path": path, "query": query,
                     "headers": [], "method": "GET"}
            await upstream.handle(scope, receive, send)
            return

    async def fixed_server():
        srv = await asyncio.start_server(_pipe, "127.0.0.1", port)
        ready.set()
        async with srv:
            await srv.serve_forever()

    srv_task = asyncio.create_task(fixed_server())
    await ready.wait()

    crawler = Crawler(
        base_url=f"http://127.0.0.1:{port}",
        sections={"招标公告": "/tenders", "中标公告": "/awards"},
    )

    # ---- contract 1: max two in flight, full round succeeds, verification table
    await crawler.start_round()
    task = crawler._round_task
    await task
    snap = crawler.snapshot()
    print(f"[1] status={snap['status']} max_inflight={upstream.max_inflight} "
          f"records={snap['records_count']} hits={upstream.total_hits} "
          f"counts={snap['verification_counts']}")
    assert snap["status"] == "completed", snap["status"]
    assert upstream.max_inflight <= MAX_IN_FLIGHT
    assert upstream.max_inflight == 2, f"expected overlap of 2, got {upstream.max_inflight}"
    # Records stay deduplicated by code across the two columns.
    assert snap["records_count"] == 5
    assert not snap["unfinished"]
    # Both columns' details were fetched for shared codes (4 award + 5 tender
    # details + 2x2 list pages = 13 requests minimum).
    assert upstream.total_hits >= 13, upstream.total_hits
    # Verification conclusions.
    got = conclusions_by_code(crawler)
    expected = {
        "ZB-2026-001": "match",
        "ZB-2026-002": "conflict",      # award unit differs
        "ZB-2026-003": "match",
        "ZB-2026-004": "match",
        "ZB-2026-005": "single_column",  # tender only, lists complete
    }
    assert got == expected, got
    assert snap["lists_complete"] is True
    assert snap["verification_counts"] == {
        "match": 3, "conflict": 1, "single_column": 1, "pending": 0,
    }
    rows = {r["announcement_code"]: r for r in crawler.verification()}
    conflict_row = rows[CONFLICT_CODE]
    assert conflict_row["sides"]["招标公告"]["unit"] == NORMAL_UNIT
    assert conflict_row["sides"]["中标公告"]["unit"] == AWARD_CONFLICT_UNIT
    assert conflict_row["listed_in"] == ["招标公告", "中标公告"]
    assert rows["ZB-2026-005"]["listed_in"] == ["招标公告"]
    # Deduped record for the conflict code keeps the tender-column unit.
    rec = {r["announcement_code"]: r for r in crawler.records()}
    assert rec[CONFLICT_CODE]["procurement_unit"] == NORMAL_UNIT
    assert rec[CONFLICT_CODE]["section"] == "招标公告"

    # ---- contract 2: 429 triggers site-wide cooldown; retries recover
    upstream.reject_next = 2
    upstream.total_hits = 0
    t0 = time.monotonic()
    await crawler.start_round()
    await crawler._round_task
    elapsed = time.monotonic() - t0
    snap = crawler.snapshot()
    n429 = sum("429" in e for e in snap["events"])
    print(f"[2] status={snap['status']} 429_events={n429} elapsed={elapsed:.1f}s records={snap['records_count']}")
    assert n429 >= 1
    assert elapsed >= upstream.retry_after, "site did not wait for Retry-After"
    assert snap["status"] == "completed", snap["status"]
    assert snap["verification_counts"]["conflict"] == 1

    # ---- contract 3: stop during cooldown; queued pages never send
    upstream.reject_next = 4
    await crawler.start_round()
    await asyncio.sleep(0.6)  # let the first 429s and cooldown start
    hits_at_stop = upstream.total_hits
    await crawler.stop_round()
    snap = crawler.snapshot()
    print(f"[3] status={snap['status']} hits_at_stop={hits_at_stop} final_hits={upstream.total_hits} "
          f"unfinished={len(snap['unfinished'])}")
    assert snap["status"] == "stopped"
    # Give any wrongly-queued request a chance to leak out.
    await asyncio.sleep(0.5)
    assert upstream.total_hits == hits_at_stop, "requests were sent after stop"
    assert len(snap["unfinished"]) > 0
    # Lists never completed -> single-column conclusions must stay pending.
    assert snap["lists_complete"] is False
    assert all(c == "pending" for c in conclusions_by_code(crawler).values())

    # ---- contract 4: a fresh round after stop works (no stale writes)
    upstream.reject_next = 0
    rid = await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    print(f"[4] new_round={rid} status={snap['status']} records={snap['records_count']}")
    assert snap["round_id"] == rid
    assert snap["status"] == "completed"
    assert snap["records_count"] == 5
    assert conclusions_by_code(crawler) == expected
    assert all(rid[:8] in e or "429" not in e for e in snap["events"])

    # ---- contract 5: a missing list page keeps single-column codes pending
    upstream.fail_award_page2 = True
    await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    got = conclusions_by_code(crawler)
    print(f"[5] status={snap['status']} lists_complete={snap['lists_complete']} "
          f"counts={snap['verification_counts']}")
    assert snap["status"] == "partial", snap["status"]
    assert snap["lists_complete"] is False
    # Award page 2 (with ZB-2026-004) never arrived: 004/005 are only
    # confirmed in the tender column, but the award column is unknown.
    assert got["ZB-2026-004"] == "pending", got
    assert got["ZB-2026-005"] == "pending", got
    assert got["ZB-2026-001"] == "match"
    assert got["ZB-2026-002"] == "conflict"
    assert snap["verification_counts"]["single_column"] == 0
    assert any(
        u["section"] == "中标公告" and u["page_type"] == "list" and u["page"] == "2"
        for u in snap["unfinished"]
    )
    upstream.fail_award_page2 = False

    srv_task.cancel()
    print("\nALL CONTRACT TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
