"""Message brokers behind one small interface: publish, and listen from "now on".

A listener starts at the topic's current end, so a test only sees messages produced after it
subscribed: start listening before the action that should produce the event. Messages are kept
(not consumed) so several expectations can look at the same stream.

  memory   in-process broker (tests and demos; the service under test must run in the same process)
  kafka    Apache Kafka via kafka-python (pip install "k11-integration-qa-mcp[kafka]")
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Message:
    topic: str
    value: Any
    key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None
    timestamp: float = field(default_factory=time.time)
    raw: str = ""

    def as_dict(self) -> dict:
        return {"topic": self.topic, "key": self.key, "value": self.value, "headers": self.headers,
                "partition": self.partition, "offset": self.offset}


def decode(data: bytes | str | None) -> tuple[Any, str]:
    if data is None:
        return None, ""
    text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)
    try:
        return json.loads(text), text
    except ValueError:
        return text, text


def encode(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value).encode("utf-8")


class Listener:
    def __init__(self, topic: str):
        self.topic = topic
        self.messages: list[Message] = []
        self.lock = threading.Lock()
        self.error: str | None = None

    def add(self, m: Message) -> None:
        with self.lock:
            self.messages.append(m)

    def snapshot(self) -> list[Message]:
        with self.lock:
            return list(self.messages)

    def close(self) -> None:  # overridden by backends that hold resources
        pass


# ---- in-memory ----------------------------------------------------------------------------------

class MemoryBroker:
    """A process-wide broker: everything in this process that uses the same name shares topics."""

    _registry: dict[str, "MemoryBroker"] = {}
    _reg_lock = threading.Lock()

    @classmethod
    def named(cls, name: str) -> "MemoryBroker":
        with cls._reg_lock:
            if name not in cls._registry:
                cls._registry[name] = cls()
            return cls._registry[name]

    @classmethod
    def reset(cls) -> None:
        with cls._reg_lock:
            cls._registry.clear()

    def __init__(self):
        self.lock = threading.Lock()
        self.topics: dict[str, list[Message]] = {}
        self.listeners: dict[str, list[Listener]] = {}
        self.subscribers: dict[str, list] = {}  # topic -> callbacks (services under test)

    def publish(self, topic: str, value: Any, key: str | None = None, headers: dict | None = None) -> Message:
        parsed, raw = decode(encode(value))
        with self.lock:
            log = self.topics.setdefault(topic, [])
            m = Message(topic, parsed, key, dict(headers or {}), 0, len(log), raw=raw)
            log.append(m)
            listeners = list(self.listeners.get(topic, []))
            subs = list(self.subscribers.get(topic, []))
        for lst in listeners:
            lst.add(m)
        for cb in subs:
            threading.Thread(target=cb, args=(m,), daemon=True).start()
        return m

    def listen(self, topic: str) -> Listener:
        lst = Listener(topic)
        with self.lock:
            self.listeners.setdefault(topic, []).append(lst)
        return lst

    def subscribe(self, topic: str, callback) -> None:
        """For services under test: callback(message) for every new message."""
        with self.lock:
            self.subscribers.setdefault(topic, []).append(callback)

    def close(self) -> None:
        pass


# ---- Kafka --------------------------------------------------------------------------------------

class KafkaListener(Listener):
    def __init__(self, topic: str, bootstrap: str, extra: dict):
        super().__init__(topic)
        from kafka import KafkaConsumer, TopicPartition

        self._stop = threading.Event()
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, group_id=None,
                                      consumer_timeout_ms=200, **extra)
        parts = None
        deadline = time.time() + 10
        while not parts and time.time() < deadline:
            parts = self.consumer.partitions_for_topic(topic)
            if not parts:
                time.sleep(0.2)
        if not parts:
            self.consumer.close()
            raise ValueError(f"Kafka topic '{topic}' not found")
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        self.consumer.assign(tps)
        self.consumer.seek_to_end(*tps)
        for tp in tps:
            self.consumer.position(tp)  # resolve "end" now, so only later messages are seen
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                batch = self.consumer.poll(timeout_ms=200)
                for _tp, records in batch.items():
                    for r in records:
                        value, raw = decode(r.value)
                        key = r.key.decode("utf-8", errors="replace") if isinstance(r.key, bytes) else r.key
                        hdrs = {k: (v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v)) for k, v in (r.headers or [])}
                        self.add(Message(r.topic, value, key, hdrs, r.partition, r.offset, (r.timestamp or 0) / 1000, raw))
        except Exception as e:  # noqa: BLE001 - surfaced in results
            self.error = f"{type(e).__name__}: {e}"

    def close(self) -> None:
        self._stop.set()
        self.thread.join(timeout=3)
        try:
            self.consumer.close()
        except Exception:  # noqa: BLE001
            pass


class KafkaBroker:
    def __init__(self, bootstrap: str, extra: dict | None = None):
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise ValueError('Kafka needs kafka-python: pip install "k11-integration-qa-mcp[kafka]"') from None
        self.bootstrap, self.extra = bootstrap, dict(extra or {})
        self.producer = KafkaProducer(bootstrap_servers=bootstrap, **self.extra)
        self.listeners: list[KafkaListener] = []

    def publish(self, topic: str, value: Any, key: str | None = None, headers: dict | None = None) -> Message:
        fut = self.producer.send(topic, value=encode(value), key=key.encode() if key else None,
                                 headers=[(k, str(v).encode()) for k, v in (headers or {}).items()])
        md = fut.get(timeout=15)
        parsed, raw = decode(encode(value))
        return Message(topic, parsed, key, dict(headers or {}), md.partition, md.offset, raw=raw)

    def listen(self, topic: str) -> Listener:
        lst = KafkaListener(topic, self.bootstrap, {k: v for k, v in self.extra.items() if k.startswith(("security", "sasl", "ssl"))})
        self.listeners.append(lst)
        return lst

    def close(self) -> None:
        for lst in self.listeners:
            lst.close()
        try:
            self.producer.close(timeout=3)
        except Exception:  # noqa: BLE001
            pass


def open_broker(name: str, spec: dict):
    from .config import sub_env

    spec = sub_env(spec)
    if spec["type"] == "memory":
        return MemoryBroker.named(spec.get("name", name))
    extra = {k: v for k, v in spec.items() if k not in ("type", "bootstrap", "name")}
    return KafkaBroker(spec["bootstrap"], extra)
