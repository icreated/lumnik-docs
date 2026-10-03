#!/usr/bin/env python3
"""ACME topics — the broker every screen on docs/connectors/kafka.md reads.

One file that creates, fills and proves eleven topics on the Redpanda started by the
docker-compose.yml beside it. The SAME 2 400 fictional ACME partners as the REST demo
(docs/connectors/rest/demo/acme-api.py), generated from the row number, so a partner has the
same name and city on both pages and screens can be compared. Nothing is read from disk.

    docker compose -f docs/connectors/kafka/demo/docker-compose.yml up -d
    python3 docs/connectors/kafka/demo/acme-topics.py            # create + fill + verify
    python3 docs/connectors/kafka/demo/acme-topics.py verify     # is the broker still telling the truth?
    python3 docs/connectors/kafka/demo/acme-topics.py feed --rate 50 --seconds 60   # a live producer (--topic to pick one)
    python3 docs/connectors/kafka/demo/acme-topics.py trim 2000  # delete offsets 0..1999 of acme.partners-trimmed
    python3 docs/connectors/kafka/demo/acme-topics.py reset      # drop everything and start over

Each topic is shaped for one sentence of the page: six partitions for the per-partition
cursor, deterministic poison for the DLQ, a compacted topic with tombstones, a schema that
changes halfway, a top-level `id`, names a column cannot be, a log that can be trimmed, an
empty topic, a live one, and one that must never exist.

Dependency: `python3 -m pip install confluent-kafka` (Kafka has no stdlib client).
"""

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

try:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:  # pragma: no cover
    sys.exit("confluent-kafka is not installed: python3 -m pip install confluent-kafka")

BROKER = "localhost:19092"          # the host listener; the hub uses host.docker.internal:29092
CONTAINER = os.environ.get("KAFKA_DEMO_CONTAINER", "lumnik-demo-redpanda")  # `trim` goes through rpk

# ── The data — identical to acme-api.py ───────────────────────────────────────────────────
CITIES = [("Lyon", "FR"), ("Bruxelles", "BE"), ("Milano", "IT"), ("Porto", "PT"),
          ("Kraków", "PL"), ("Aarhus", "DK"), ("Valencia", "ES"), ("Bristol", "GB")]
TRADES = ["Hydraulique", "Roulements", "Fixations", "Pneumatique", "Étanchéité",
          "Transmission", "Outillage", "Levage", "Filtration", "Soudure"]
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def partner(n):
    """The REST demo's partner, with `id` renamed `partner_id` — `id` is a reserved hub column."""
    city, country = CITIES[n % len(CITIES)]
    return {
        "partner_id": 1000 + n,
        "name": f"{TRADES[n % len(TRADES)]} {city} {1000 + n}",
        "city": city,
        "country": country,
        "credit_limit": 500 + (n * 137) % 45_000,
        "updated_at": iso(EPOCH + timedelta(minutes=n)),
    }


def order(n):
    """Ten orders per partner, keyed by the partner so one partner's orders share a partition."""
    p = 1000 + n // 10
    return {
        "order_id": 500_000 + n,
        "partner_id": p,
        "documentno": f"SO-{n:06d}",
        "grand_total": round(120 + (n * 731) % 9_880 + (n % 100) / 100, 2),
        "ordered_at": iso(EPOCH + timedelta(hours=n % 2_000, minutes=n % 60)),
    }


PARTNERS = [partner(n) for n in range(2_400)]
POISON = [b'{"broken": ', b'[1, 2, 3]', b'"just a string"']   # truncated, an array, a scalar


# ── The topics ────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    expected: int | None            # exact message count `verify` asserts; None = not counted
    configs: dict = field(default_factory=dict)
    create: bool = True             # acme.absent is the one False
    shows: str = ""                 # the sentence of the page this topic is for


TOPICS = [
    Topic("acme.partners", 1, 2_400, shows="the reference: one partition, no auth"),
    Topic("acme.orders", 6, 24_000, shows="the per-partition cursor"),
    Topic("acme.partners-drift", 1, 2_400, shows="a schema that gains two fields halfway"),
    Topic("acme.partners-poison", 1, 1_000, shows="the per-message DLQ; 143 poison, never random"),
    Topic("acme.partners-compacted", 1, 1_300, {"cleanup.policy": "compact"},
          shows="1 000 rows then 300 tombstones — a deletion is not garbage"),
    Topic("acme.partners-reserved", 1, 500, shows="a top-level `id`: the reserved column"),
    Topic("acme.partners-raw", 1, 500, shows="names a column cannot be, nested objects, arrays"),
    Topic("acme.partners-trimmed", 1, 5_000, shows="a log whose start can be moved under a cursor"),
    Topic("acme.empty", 1, 0, shows="an honest empty run"),
    Topic("acme.live", 1, None, shows="fed while a run reads it — where does the run stop?"),
    Topic("acme.absent", 1, 0, create=False, shows="never created: the refusal must name it"),
]


# ── Producing ─────────────────────────────────────────────────────────────────────────────
DELIVERY_ERRORS: list[str] = []


def on_delivery(err, msg):
    # flush() returning 0 proves only that the local queue drained, never that the broker
    # accepted anything — a failed produce leaves the queue exactly like a successful one.
    if err is not None:
        DELIVERY_ERRORS.append(f"{msg.topic()}: {err}")


def producer():
    return Producer({"bootstrap.servers": BROKER, "linger.ms": 50,
                     "batch.num.messages": 10_000, "queue.buffering.max.messages": 1_000_000})


def send(p, topic, key, value):
    while True:
        try:
            p.produce(topic, key=key, value=value, on_delivery=on_delivery)
            return
        except BufferError:
            p.poll(0.5)


def j(obj):
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def fill(p):
    for r in PARTNERS:
        send(p, "acme.partners", str(r["partner_id"]), j(r))
    for n in range(24_000):
        o = order(n)
        send(p, "acme.orders", str(o["partner_id"]), j(o))
    # drift: the producer added `vat_number` and `segment` after the first 1 200 messages
    for n, r in enumerate(PARTNERS):
        row = dict(r)
        if n >= 1_200:
            row["vat_number"] = f"{r['country']}{r['partner_id'] * 7919 % 10**9:09d}"
            row["segment"] = ("key-account", "regional", "occasional")[n % 3]
        send(p, "acme.partners-drift", str(r["partner_id"]), j(row))
    for i in range(1_000):
        if i % 7 == 3:
            send(p, "acme.partners-poison", str(i), POISON[i % 3])
        else:
            send(p, "acme.partners-poison", str(i), j(PARTNERS[i]))
    for i in range(1_000):
        send(p, "acme.partners-compacted", str(PARTNERS[i]["partner_id"]), j(PARTNERS[i]))
    for i in range(300):   # then the producer deletes the first 300 keys
        send(p, "acme.partners-compacted", str(PARTNERS[i]["partner_id"]), None)
    for i in range(500):   # the REST demo's record, `id` and all
        send(p, "acme.partners-reserved", str(i), j({"id": PARTNERS[i]["partner_id"], **{
            k: v for k, v in PARTNERS[i].items() if k != "partner_id"}}))
    for i in range(500):
        r = PARTNERS[i]
        send(p, "acme.partners-raw", str(i), j({
            "partner_id": r["partner_id"],
            "Partner Name": r["name"],
            "credit.limit": r["credit_limit"],
            "MONTANT_€": round(i * 1.5, 2),
            "address": {"street": f"{i} rue de l'Usine", "city": r["city"]},
            "tags": ["erp", "partner", f"batch-{i % 5}"],
        }))
    for i in range(5_000):
        r = PARTNERS[i % 2_400]
        send(p, "acme.partners-trimmed", str(i), j({"seq": i, "partner_id": r["partner_id"],
                                                   "name": r["name"]}))
    # acme.empty and acme.live are filled by nobody here, on purpose.


# ── Admin ─────────────────────────────────────────────────────────────────────────────────
def admin():
    return AdminClient({"bootstrap.servers": BROKER})


def existing():
    return set(admin().list_topics(timeout=10).topics)


def create():
    have = existing()
    wanted = [t for t in TOPICS if t.create and t.name not in have]
    if not wanted:
        print("  ▪ all topics already present")
        return False
    a = admin()   # held until the futures resolve — a temporary is destroyed under them
    for name, fut in a.create_topics([
            NewTopic(t.name, num_partitions=t.partitions, replication_factor=1, config=t.configs)
            for t in wanted]).items():
        fut.result()
        print(f"  ▸ created {name}")
    return True


def drop():
    doomed = [t.name for t in TOPICS if t.name in existing()]
    if not doomed:
        print("  ▪ nothing to drop")
        return
    a = admin()
    for name, fut in a.delete_topics(doomed).items():
        fut.result()
        print(f"  ▪ dropped {name}")


def consumer():
    return Consumer({"bootstrap.servers": BROKER, "group.id": "acme-topics-verify",
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})


def count(c, name, partitions):
    return sum(hi - lo for lo, hi in (
        c.get_watermark_offsets(TopicPartition(name, p), timeout=10) for p in range(partitions)))


def watermarks(c, name, p=0):
    return c.get_watermark_offsets(TopicPartition(name, p), timeout=10)


# ── Commands ──────────────────────────────────────────────────────────────────────────────
def cmd_verify():
    """Counts come from watermarks, not from consuming; a bench that lies must fail loudly."""
    failures = []
    have = existing()
    meta = admin().list_topics(timeout=10).topics
    c = consumer()
    try:
        for t in TOPICS:
            if not t.create:
                if t.name in have:
                    failures.append(f"{t.name} must NOT exist, and does — something created it")
                continue
            if t.name not in have:
                failures.append(f"{t.name} is missing")
                continue
            got_p = len(meta[t.name].partitions)
            if got_p != t.partitions:
                failures.append(f"{t.name}: {got_p} partitions, expected {t.partitions}")
            if t.expected is not None and (got := count(c, t.name, t.partitions)) != t.expected:
                failures.append(f"{t.name}: {got} messages, expected {t.expected}")
    finally:
        c.close()
    for f in failures:
        print(f"  ✖ {f}")
    if failures:
        print(f"\n  {len(failures)} problem(s) — do not run a manifest against this broker")
        return 1
    print("  ✓ eleven topics as declared (acme.absent is absent)")
    return 0


def cmd_seed():
    if create():
        p = producer()
        fill(p)
        left = p.flush(120)
        if left or DELIVERY_ERRORS:
            print(f"  ✖ {left} message(s) never left the queue; {len(DELIVERY_ERRORS)} delivery "
                  f"failure(s){': ' + DELIVERY_ERRORS[0] if DELIVERY_ERRORS else ''}")
            return 1
        print("  ✓ filled")
    return cmd_verify()


def cmd_reset():
    drop()
    time.sleep(1)
    return cmd_seed()


def cmd_feed(rate, seconds, topic):
    """Keeps a topic busy while a run reads it. The only command that races the hub."""
    p = Producer({"bootstrap.servers": BROKER, "linger.ms": 0})
    deadline = time.monotonic() + seconds
    i = 0
    while time.monotonic() < deadline:
        p.produce(topic, key=str(i), value=j({"seq": i, "at": time.time()}))
        p.poll(0)
        i += 1
        time.sleep(1.0 / rate)
    p.flush(30)
    print(f"  ▪ fed {i} messages at {rate}/s into {topic}")
    return 0


def cmd_trim(offset):
    """Moves acme.partners-trimmed's log start to `offset` — what retention does, exactly and now.

    Through rpk, because the client API has no such call. And read back, because rpk prints a
    perfect-looking result table BEFORE asking for confirmation: from a non-tty without
    --no-confirm it aborts after printing it, and moves nothing.
    """
    out = subprocess.run(["docker", "exec", CONTAINER, "rpk", "topic", "trim-prefix",
                          "acme.partners-trimmed", "-p", "0", "-o", str(offset), "--no-confirm",
                          "--brokers", "localhost:9092"], capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stderr.strip() or out.stdout.strip())
        return 1
    c = consumer()
    try:
        lo, hi = watermarks(c, "acme.partners-trimmed")
    finally:
        c.close()
    if lo != offset:
        print(f"  ✖ log start is {lo}, not {offset} — the trim did not happen")
        return 1
    print(f"  ✓ acme.partners-trimmed now starts at offset {lo} (high watermark {hi}): "
          f"offsets 0..{offset - 1} are gone")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("seed")
    sub.add_parser("verify")
    sub.add_parser("reset")
    f = sub.add_parser("feed")
    f.add_argument("--rate", type=float, default=50.0)
    f.add_argument("--seconds", type=float, default=60.0)
    f.add_argument("--topic", default="acme.live")
    t = sub.add_parser("trim")
    t.add_argument("offset", type=int)
    a = ap.parse_args()
    if a.cmd in (None, "seed"):
        return cmd_seed()
    if a.cmd == "verify":
        return cmd_verify()
    if a.cmd == "reset":
        return cmd_reset()
    if a.cmd == "feed":
        return cmd_feed(a.rate, a.seconds, a.topic)
    if a.cmd == "trim":
        return cmd_trim(a.offset)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
