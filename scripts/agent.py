import argparse
import os
import socket
import time
from collections import defaultdict
from threading import Event, Thread

from stun_common import (
    DGIMCounter,
    JsonLogger,
    PeriodicPlotLogger,
    ReservoirSampler,
    STUN_SERVERS,
    TimeWindowAverage,
    make_consumer,
    make_producer,
    send_record,
    server_id,
)


TOPIC_METRICS = "stun_metrics"
TOPIC_AVAILABILITY = "stun_availability"
TOPIC_AVG_LATENCY = "stun_avg_latency"
TOPIC_AVG_AVAILABILITY = "stun_avg_availability"
TOPIC_BOTTOM_LATENCY = "stun_bottom_ten_percent_latency"


def stun_ping(host, port, timeout_seconds):
    request = b"\x00\x01\x00\x00\x21\x12\xa4\x42" + os.urandom(12)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_seconds)
    start = time.time()
    try:
        sock.sendto(request, (host, port))
        sock.recvfrom(2048)
        return 1, round((time.time() - start) * 1000.0, 2)
    except (socket.timeout, socket.gaierror, OSError):
        return 0, round(timeout_seconds * 1000.0, 2)
    finally:
        sock.close()


def collect_metrics(args, finishing, topic_logger):
    producer = make_producer(args.bootstrap_server)
    try:
        while not finishing.is_set():
            cycle_started = time.time()
            for host, port in STUN_SERVERS:
                responded, latency_ms = stun_ping(host, port, args.timeout_ms / 1000.0)
                record = {
                    "timestamp": time.time(),
                    "node": args.node_name,
                    "server": server_id(host, port),
                    "host": host,
                    "port": port,
                    "responded": responded,
                    "latency_ms": latency_ms,
                }
                send_record(producer, topic_logger, TOPIC_METRICS, record, key=record["server"])
            producer.flush()
            elapsed = time.time() - cycle_started
            finishing.wait(max(0.0, args.period_seconds - elapsed))
    finally:
        producer.close(2)


def availability_worker(args, finishing, topic_logger):
    consumer = make_consumer(
        TOPIC_METRICS,
        args.bootstrap_server,
        group_id=f"{args.node_name}-availability",
        from_beginning=args.from_beginning,
    )
    producer = make_producer(args.bootstrap_server)
    try:
        while not finishing.is_set():
            for msg in consumer:
                metric = msg.value
                availability = 1 if float(metric["latency_ms"]) <= args.sla_ms else 0
                record = {
                    "timestamp": metric["timestamp"],
                    "processed_at": time.time(),
                    "node": args.node_name,
                    "server": metric["server"],
                    "availability": availability,
                    "latency_ms": metric["latency_ms"],
                }
                send_record(
                    producer,
                    topic_logger,
                    TOPIC_AVAILABILITY,
                    record,
                    key=record["server"],
                )
            time.sleep(0.05)
    finally:
        consumer.close()
        producer.close(2)


def avg_latency_worker(args, finishing, topic_logger, plot_logger):
    consumer = make_consumer(
        TOPIC_METRICS,
        args.bootstrap_server,
        group_id=f"{args.node_name}-avg-latency",
        from_beginning=args.from_beginning,
    )
    producer = make_producer(args.bootstrap_server)
    averages = TimeWindowAverage(args.latency_window_seconds)
    try:
        while not finishing.is_set():
            for msg in consumer:
                metric = msg.value
                avg, count = averages.add(
                    metric["server"],
                    float(metric["timestamp"]),
                    float(metric["latency_ms"]),
                )
                record = {
                    "timestamp": time.time(),
                    "window_seconds": args.latency_window_seconds,
                    "node": args.node_name,
                    "server": metric["server"],
                    "avg_latency_ms": round(avg, 2),
                    "count": count,
                }
                send_record(
                    producer,
                    topic_logger,
                    TOPIC_AVG_LATENCY,
                    record,
                    key=record["server"],
                )
                plot_logger.maybe_write(TOPIC_AVG_LATENCY, record, "avg_latency_ms")
            time.sleep(0.05)
    finally:
        consumer.close()
        producer.close(2)


def avg_availability_worker(args, finishing, topic_logger, plot_logger):
    consumer = make_consumer(
        TOPIC_AVAILABILITY,
        args.bootstrap_server,
        group_id=f"{args.node_name}-avg-availability",
        from_beginning=args.from_beginning,
    )
    producer = make_producer(args.bootstrap_server)
    window_size = max(1, int(args.availability_window_seconds / args.period_seconds))
    counters = defaultdict(lambda: DGIMCounter(window_size))
    try:
        while not finishing.is_set():
            for msg in consumer:
                availability_record = msg.value
                server = availability_record["server"]
                zero_bit = 1 if int(availability_record["availability"]) == 0 else 0
                counters[server].add(zero_bit)
                seen = counters[server].seen_in_window()
                zero_count = counters[server].count()
                avg_availability = 1.0 - (zero_count / seen if seen else 0.0)
                avg_availability = max(0.0, min(1.0, avg_availability))
                record = {
                    "timestamp": time.time(),
                    "window_seconds": args.availability_window_seconds,
                    "dgim_window_size": window_size,
                    "node": args.node_name,
                    "server": server,
                    "avg_availability": round(avg_availability, 4),
                    "estimated_zeros": round(zero_count, 2),
                    "count": seen,
                }
                send_record(
                    producer,
                    topic_logger,
                    TOPIC_AVG_AVAILABILITY,
                    record,
                    key=record["server"],
                )
                plot_logger.maybe_write(TOPIC_AVG_AVAILABILITY, record, "avg_availability")
            time.sleep(0.05)
    finally:
        consumer.close()
        producer.close(2)


def reservoir_worker(args, finishing, topic_logger, plot_logger):
    consumer = make_consumer(
        TOPIC_METRICS,
        args.bootstrap_server,
        group_id=f"{args.node_name}-reservoir",
        from_beginning=args.from_beginning,
    )
    producer = make_producer(args.bootstrap_server)
    samplers = defaultdict(
        lambda: ReservoirSampler(args.reservoir_size, args.reservoir_kth_highest)
    )
    try:
        while not finishing.is_set():
            for msg in consumer:
                metric = msg.value
                server = metric["server"]
                samplers[server].add(float(metric["latency_ms"]))
                kth = samplers[server].kth_value()
                if kth is None:
                    continue
                record = {
                    "timestamp": time.time(),
                    "node": args.node_name,
                    "server": server,
                    "sample_size": len(samplers[server].sample),
                    "seen": samplers[server].seen,
                    "rank_highest": args.reservoir_kth_highest,
                    "bottom_ten_percent_latency_ms": round(kth, 2),
                }
                send_record(
                    producer,
                    topic_logger,
                    TOPIC_BOTTOM_LATENCY,
                    record,
                    key=record["server"],
                )
                plot_logger.maybe_write(
                    TOPIC_BOTTOM_LATENCY,
                    record,
                    "bottom_ten_percent_latency_ms",
                )
            time.sleep(0.05)
    finally:
        consumer.close()
        producer.close(2)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-name", required=True)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--period-seconds", type=float, default=20.0)
    parser.add_argument("--timeout-ms", type=float, default=500.0)
    parser.add_argument("--sla-ms", type=float, default=200.0)
    parser.add_argument("--latency-window-seconds", type=float, default=10 * 60)
    parser.add_argument("--availability-window-seconds", type=float, default=20 * 60)
    parser.add_argument("--reservoir-size", type=int, default=100)
    parser.add_argument("--reservoir-kth-highest", type=int, default=10)
    parser.add_argument("--summary-interval-seconds", type=float, default=72.0)
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.log_dir, exist_ok=True)
    topic_logger = JsonLogger(os.path.join(args.log_dir, "agent_topics.jsonl"))
    plot_file_logger = JsonLogger(os.path.join(args.log_dir, "agent_plot_values.jsonl"))
    plot_logger = PeriodicPlotLogger(plot_file_logger, args.summary_interval_seconds)
    finishing = Event()
    workers = [
        Thread(target=availability_worker, args=(args, finishing, topic_logger), daemon=True),
        Thread(target=avg_latency_worker, args=(args, finishing, topic_logger, plot_logger), daemon=True),
        Thread(target=avg_availability_worker, args=(args, finishing, topic_logger, plot_logger), daemon=True),
        Thread(target=reservoir_worker, args=(args, finishing, topic_logger, plot_logger), daemon=True),
    ]
    for worker in workers:
        worker.start()
    try:
        collect_metrics(args, finishing, topic_logger)
    except KeyboardInterrupt:
        finishing.set()
    finally:
        finishing.set()
        for worker in workers:
            worker.join(timeout=3)
        topic_logger.close()
        plot_file_logger.close()


if __name__ == "__main__":
    main()
