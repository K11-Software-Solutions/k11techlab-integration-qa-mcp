"""K11 Shop: an order API with message consumers and a batch job, for k11 Integration QA's demo and tests.

  POST /orders {sku, qty}        stores a pending order and publishes OrderCreated to orders.events
  GET  /orders/{id}              the order and its status
  inventory consumer             on OrderCreated: reserves stock, marks the order reserved, publishes
                                 StockReserved (or OrderFailed when there isn't enough stock)
  commands consumer              orders.commands {"action": "cancel", "orderId": ...}; a message it
                                 can't read goes to orders.dlq with an "error" header
  jobs/sku_totals.py             batch job: totals of reserved quantity per SKU into sku_totals

Bugs to switch on (bugs=... or SHOP_BUGS=consumer,dlq,job), so the tests can prove both directions:
  consumer   reserves 1 item whatever the order quantity
  dlq        drops unreadable commands silently instead of dead-lettering them
  job        sku_totals adds to the existing totals, so running the job twice doubles them

Messaging is in-process (memory broker "shop") by default, or Kafka with --kafka host:port.

  python demo/shop/shop.py --port 8095 --kafka localhost:9092
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).parent
EVENTS, COMMANDS, DLQ = "orders.events", "orders.commands", "orders.dlq"
SCHEMA = """
CREATE TABLE IF NOT EXISTS inventory (sku TEXT PRIMARY KEY, stock INTEGER NOT NULL, reserved INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, sku TEXT NOT NULL, qty INTEGER NOT NULL, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sku_totals (sku TEXT PRIMARY KEY, qty INTEGER NOT NULL);
"""


def init_db(path: Path) -> None:
    with sqlite3.connect(path) as c:
        c.executescript(SCHEMA)
        c.execute("INSERT OR IGNORE INTO inventory (sku, stock, reserved) VALUES ('BAG-1', 1000, 0)")


class MemoryBus:
    def __init__(self):
        from k11_integration_qa.brokers import MemoryBroker

        self.broker = MemoryBroker.named("shop")

    def publish(self, topic, value, key=None, headers=None):
        self.broker.publish(topic, value, key, headers)

    def consume(self, topic, group, handler):
        self.broker.subscribe(topic, lambda m: handler(m.raw, m.key, m.headers))

    def close(self):
        pass


class KafkaBus:
    def __init__(self, bootstrap: str):
        from kafka import KafkaConsumer, KafkaProducer

        self.bootstrap, self.KafkaConsumer = bootstrap, KafkaConsumer
        self.producer = KafkaProducer(bootstrap_servers=bootstrap)
        self.stop = threading.Event()
        self.threads = []

    def publish(self, topic, value, key=None, headers=None):
        data = value if isinstance(value, (bytes, str)) else json.dumps(value)
        self.producer.send(topic, value=data.encode() if isinstance(data, str) else data,
                           key=key.encode() if key else None,
                           headers=[(k, str(v).encode()) for k, v in (headers or {}).items()]).get(timeout=15)

    def consume(self, topic, group, handler):
        c = self.KafkaConsumer(topic, bootstrap_servers=self.bootstrap, group_id=f"shop-{group}",
                               auto_offset_reset="latest", enable_auto_commit=True)
        ready = threading.Event()

        def run():
            while not self.stop.is_set():
                batch = c.poll(timeout_ms=200)
                if c.assignment():
                    ready.set()
                for records in batch.values():
                    for r in records:
                        key = r.key.decode() if r.key else None
                        hdrs = {k: v.decode() for k, v in (r.headers or [])}
                        handler(r.value.decode("utf-8", errors="replace"), key, hdrs)
            c.close()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        self.threads.append(t)
        ready.wait(30)  # partitions assigned: from here on no message is missed

    def close(self):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=3)
        self.producer.close(timeout=3)


class Shop:
    def __init__(self, port: int = 0, db: Path | None = None, bugs: str | set | None = None, kafka: str | None = None):
        bugs = os.environ.get("SHOP_BUGS", "") if bugs is None else bugs
        self.bugs = set(filter(None, re.split(r"[,\s]+", bugs))) if isinstance(bugs, str) else set(bugs)
        self.db = Path(db or HERE / "shop.db")
        init_db(self.db)
        self.lock = threading.Lock()
        self.bus = KafkaBus(kafka) if kafka else MemoryBus()
        self.bus.consume(EVENTS, "inventory", self._on_event)
        self.bus.consume(COMMANDS, "commands", self._on_command)
        shop = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body=None):
                data = json.dumps(body).encode() if body is not None else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):  # noqa: N802
                if self.path == "/health":
                    return self._send(200, {"status": "ok"})
                m = re.fullmatch(r"/orders/([\w-]+)", self.path)
                if m:
                    row = shop.query("SELECT id, sku, qty, status FROM orders WHERE id = ?", (m.group(1),))
                    return self._send(200, row[0]) if row else self._send(404, {"error": "not found"})
                self._send(404, {"error": "not found"})

            def do_POST(self):  # noqa: N802
                if self.path != "/orders":
                    return self._send(404, {"error": "not found"})
                try:
                    body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                    sku, qty = str(body["sku"]), int(body["qty"])
                    if qty < 1:
                        raise ValueError
                except (ValueError, KeyError, TypeError):
                    return self._send(400, {"error": "sku and a positive qty are required"})
                oid = str(uuid.uuid4())
                shop.execute("INSERT INTO orders (id, sku, qty, status) VALUES (?, ?, ?, 'pending')", (oid, sku, qty))
                shop.bus.publish(EVENTS, {"type": "OrderCreated", "eventId": str(uuid.uuid4()), "orderId": oid,
                                          "sku": sku, "qty": qty, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                                 key=oid, headers={"source": "shop-api"})
                self._send(201, {"id": oid, "status": "pending"})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), H)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    # ---- data ------------------------------------------------------------------------------------
    def query(self, sql, params=()):
        with self.lock, sqlite3.connect(self.db) as c:
            c.row_factory = sqlite3.Row
            return [dict(r) for r in c.execute(sql, params)]

    def execute(self, sql, params=()):
        with self.lock, sqlite3.connect(self.db) as c:
            return c.execute(sql, params).rowcount

    # ---- consumers -------------------------------------------------------------------------------
    def _on_event(self, raw, key, headers):
        try:
            e = json.loads(raw)
        except ValueError:
            return
        if e.get("type") != "OrderCreated":
            return
        qty = 1 if "consumer" in self.bugs else int(e["qty"])
        with self.lock, sqlite3.connect(self.db) as c:
            row = c.execute("SELECT stock - reserved FROM inventory WHERE sku = ?", (e["sku"],)).fetchone()
            ok = row is not None and row[0] >= qty
            if ok:
                c.execute("UPDATE inventory SET reserved = reserved + ? WHERE sku = ?", (qty, e["sku"]))
            c.execute("UPDATE orders SET status = ? WHERE id = ?", ("reserved" if ok else "failed", e["orderId"]))
        if ok:
            self.bus.publish(EVENTS, {"type": "StockReserved", "eventId": str(uuid.uuid4()), "orderId": e["orderId"],
                                      "sku": e["sku"], "qty": qty}, key=e["orderId"])
        else:
            self.bus.publish(EVENTS, {"type": "OrderFailed", "eventId": str(uuid.uuid4()), "orderId": e["orderId"],
                                      "reason": "unknown sku" if row is None else "out of stock"}, key=e["orderId"])

    def _on_command(self, raw, key, headers):
        try:
            cmd = json.loads(raw)
            if not isinstance(cmd, dict) or cmd.get("action") != "cancel" or not cmd.get("orderId"):
                raise ValueError("expected {\"action\": \"cancel\", \"orderId\": ...}")
        except ValueError as e:
            if "dlq" not in self.bugs:
                self.bus.publish(DLQ, raw, key=key, headers={**headers, "error": str(e)[:120] or "not JSON"})
            return
        order = self.query("SELECT sku, qty, status FROM orders WHERE id = ?", (cmd["orderId"],))
        if order and order[0]["status"] == "reserved":
            with self.lock, sqlite3.connect(self.db) as c:
                c.execute("UPDATE inventory SET reserved = reserved - ? WHERE sku = ?", (order[0]["qty"], order[0]["sku"]))
                c.execute("UPDATE orders SET status = 'cancelled' WHERE id = ?", (cmd["orderId"],))
            self.bus.publish(EVENTS, {"type": "OrderCancelled", "eventId": str(uuid.uuid4()), "orderId": cmd["orderId"]},
                             key=cmd["orderId"])

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.bus.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=8095)
    ap.add_argument("--db", default=str(HERE / "shop.db"))
    ap.add_argument("--kafka", help="bootstrap servers, e.g. localhost:9092 (default: in-process broker)")
    ap.add_argument("--bugs", default=None, help="comma-separated: consumer, dlq, job")
    a = ap.parse_args()
    s = Shop(a.port, Path(a.db), a.bugs, a.kafka)
    print(f"K11 Shop on {s.url} (bugs: {sorted(s.bugs) or 'none'}; messaging: {'kafka ' + a.kafka if a.kafka else 'in-process'})", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        s.close()
