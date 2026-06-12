import argparse
import json
import os
import queue
import time
from threading import Event, Thread

from stun_common import (
    JsonLogger,
    PeriodicPlotLogger,
    TimeWindowAverage,
    make_consumer,
    make_producer,
    send_record,
)


TOPIC_AVAILABILITY = "stun_availability"
TOPIC_GLOBAL_AVAILABILITY = "stun_global_availability"


def consume_agent(agent, args, finishing, output_queue):
    consumer = make_consumer(
        TOPIC_AVAILABILITY,
        agent["bootstrap_server"],
        group_id=f"{args.node_name}-central-{agent['node']}",
        from_beginning=args.from_beginning,
    )
    try:
        while not finishing.is_set():
            for msg in consumer:
                record = msg.value
                record["agent_node"] = agent["node"]
                output_queue.put(record)
            time.sleep(0.05)
    finally:
        consumer.close()


def aggregate(args, finishing, input_queue, topic_logger, plot_logger):
    producer = make_producer(args.local_bootstrap)
    averages = TimeWindowAverage(args.window_seconds)
    try:
        while not finishing.is_set():
            try:
                record = input_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            server = record["server"]
            timestamp = float(record.get("timestamp", time.time()))
            availability = float(record["availability"])
            avg, count = averages.add(server, timestamp, availability)
            output = {
                "timestamp": time.time(),
                "window_seconds": args.window_seconds,
                "node": args.node_name,
                "server": server,
                "global_availability": round(avg, 4),
                "count": count,
            }
            send_record(
                producer,
                topic_logger,
                TOPIC_GLOBAL_AVAILABILITY,
                output,
                key=server,
            )
            plot_logger.maybe_write(
                TOPIC_GLOBAL_AVAILABILITY,
                output,
                "global_availability",
            )
    finally:
        producer.close(2)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-name", required=True)
    parser.add_argument("--local-bootstrap", default="localhost:9092")
    parser.add_argument("--agents-config", default="agent_nodes.json")
    parser.add_argument("--window-seconds", type=float, default=20 * 60)
    parser.add_argument("--summary-interval-seconds", type=float, default=72.0)
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.log_dir, exist_ok=True)
    with open(args.agents_config, "r", encoding="utf-8") as config_file:
        agents = json.load(config_file)

    topic_logger = JsonLogger(os.path.join(args.log_dir, "central_topics.jsonl"))
    plot_file_logger = JsonLogger(os.path.join(args.log_dir, "central_plot_values.jsonl"))
    plot_logger = PeriodicPlotLogger(plot_file_logger, args.summary_interval_seconds)
    input_queue = queue.Queue()
    finishing = Event()

    workers = [
        Thread(
            target=consume_agent,
            args=(agent, args, finishing, input_queue),
            daemon=True,
        )
        for agent in agents
    ]
    for worker in workers:
        worker.start()

    try:
        aggregate(args, finishing, input_queue, topic_logger, plot_logger)
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
