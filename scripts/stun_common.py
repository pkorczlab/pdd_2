import heapq
import json
import os
import random
import time
from collections import defaultdict, deque
from threading import Lock

from kafka import KafkaConsumer, KafkaProducer


STUN_SERVERS = [
    ("stun.l.google.com", 19302),
    ("global.stun.twilio.com", 3478),
    ("stun.schlund.de", 3478),
    ("stun.sipgate.net", 10000),
    ("stun.voip.blackberry.com", 3478),
    ("stun.ekiga.net", 3478),
    ("stun.freeswitch.org", 3478),
    ("stun.sip.us", 3478),
    ("stun.voipbuster.com", 3478),
    ("stun.counterpath.com", 3478),
]


def server_id(host, port):
    return f"{host}:{port}"


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class JsonLogger:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._file = open(path, "a", buffering=1, encoding="utf-8")
        self._lock = Lock()

    def write(self, record):
        envelope = {
            "logged_at": time.time(),
            "logged_at_iso": now_iso(),
            **record,
        }
        line = json.dumps(envelope, sort_keys=True)
        with self._lock:
            self._file.write(line + "\n")

    def close(self):
        with self._lock:
            self._file.close()


class PeriodicPlotLogger:
    def __init__(self, logger, interval_seconds):
        self.logger = logger
        self.interval_seconds = interval_seconds
        self.next_log_at = {}
        self.lock = Lock()

    def maybe_write(self, topic, record, field):
        value = record.get(field)
        if value is None:
            return
        key = (topic, record.get("node"), record.get("server"))
        now = time.time()
        with self.lock:
            if now < self.next_log_at.get(key, 0.0):
                return
            self.next_log_at[key] = now + self.interval_seconds
        self.logger.write(
            {
                "topic": topic,
                "node": record.get("node"),
                "server": record.get("server"),
                "field": field,
                "value": value,
                "record": record,
            }
        )


def _serialize_key(value):
    if value is None:
        return None
    return str(value).encode("utf-8")


def _deserialize_key(value):
    if value is None:
        return None
    return value.decode("utf-8")


def make_producer(bootstrap_server):
    return KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        retries=2,
        key_serializer=_serialize_key,
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
    )


def make_consumer(topic, bootstrap_server, group_id, from_beginning=False):
    return KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest" if from_beginning else "latest",
        consumer_timeout_ms=200,
        session_timeout_ms=6000,
        heartbeat_interval_ms=300,
        key_deserializer=_deserialize_key,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def send_record(producer, logger, topic, record, key=None):
    producer.send(topic, key=key, value=record)
    if logger is not None:
        logger.write({"topic": topic, "key": key, "record": record})


class TimeWindowAverage:
    def __init__(self, window_seconds):
        self.window_seconds = window_seconds
        self.values = defaultdict(deque)
        self.sums = defaultdict(float)

    def add(self, key, timestamp, value):
        queue = self.values[key]
        queue.append((timestamp, value))
        self.sums[key] += value
        self._drop_old(key, timestamp)
        if not queue:
            return None, 0
        return self.sums[key] / len(queue), len(queue)

    def _drop_old(self, key, current_time):
        queue = self.values[key]
        threshold = current_time - self.window_seconds
        while queue and queue[0][0] < threshold:
            _, value = queue.popleft()
            self.sums[key] -= value


class DGIMCounter:
    def __init__(self, window_size):
        self.window_size = window_size
        self.time = 0
        self.buckets = []

    def add(self, bit):
        self.time += 1
        if bit:
            self.buckets.insert(0, (self.time, 1))
            self._compress()
        self._drop_old()

    def count(self, window_size=None):
        window_size = window_size or self.window_size
        window_start = self.time - window_size + 1
        inside = [bucket for bucket in self.buckets if bucket[0] >= window_start]
        if not inside:
            return 0.0
        total = sum(size for _, size in inside[:-1])
        total += inside[-1][1] / 2.0
        return total

    def seen_in_window(self):
        return min(self.time, self.window_size)

    def _drop_old(self):
        threshold = self.time - self.window_size
        self.buckets = [bucket for bucket in self.buckets if bucket[0] > threshold]

    def _compress(self):
        changed = True
        while changed:
            changed = False
            sizes = defaultdict(list)
            for index, (_, size) in enumerate(self.buckets):
                sizes[size].append(index)
            for size, indices in sizes.items():
                if len(indices) > 2:
                    newer_old_index = indices[-2]
                    oldest_index = indices[-1]
                    new_end = self.buckets[newer_old_index][0]
                    for index in sorted([newer_old_index, oldest_index], reverse=True):
                        self.buckets.pop(index)
                    self.buckets.append((new_end, size * 2))
                    self.buckets.sort(key=lambda bucket: bucket[0], reverse=True)
                    changed = True
                    break


class ReservoirSampler:
    def __init__(self, sample_size, kth_highest, seed=None):
        self.sample_size = sample_size
        self.kth_highest = kth_highest
        self.sample = []
        self.seen = 0
        self.random = random.Random(seed)

    def add(self, value):
        self.seen += 1
        if len(self.sample) < self.sample_size:
            self.sample.append(value)
            return
        selected = self.random.randint(0, self.seen - 1)
        if selected < self.sample_size:
            self.sample[selected] = value

    def kth_value(self):
        if len(self.sample) < self.kth_highest:
            return None
        top = []
        for value in self.sample:
            if len(top) < self.kth_highest:
                heapq.heappush(top, value)
            elif value > top[0]:
                heapq.heapreplace(top, value)
        return top[0]
