import argparse
import glob
import json
import os
from collections import defaultdict

import matplotlib.pyplot as plt


def read_plot_records(input_dir):
    pattern = os.path.join(input_dir, "**", "*plot_values.jsonl")
    for path in glob.glob(pattern, recursive=True):
        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                record["_source_file"] = path
                yield record


def safe_name(value):
    return (
        str(value)
        .replace("/", "_")
        .replace(":", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="run_logs")
    parser.add_argument("--output", default="plots")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    grouped = defaultdict(lambda: defaultdict(list))
    for record in read_plot_records(args.input):
        node = record.get("node") or "unknown-node"
        topic = record.get("topic") or "unknown-topic"
        server = record.get("server") or "unknown-server"
        grouped[(node, topic, record.get("field", "value"))][server].append(
            (record["logged_at"], record["value"])
        )

    for (node, topic, field), by_server in grouped.items():
        plt.figure(figsize=(12, 7))
        for server, points in sorted(by_server.items()):
            points.sort()
            start = points[0][0]
            xs = [(timestamp - start) / 60.0 for timestamp, _ in points]
            ys = [value for _, value in points]
            plt.plot(xs, ys, label=server)
        plt.title(f"{node} {topic}")
        plt.xlabel("minutes from first logged value")
        plt.ylabel(field)
        plt.legend(fontsize="small")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        filename = f"{safe_name(node)}_{safe_name(topic)}.png"
        plt.savefig(os.path.join(args.output, filename), dpi=160)
        plt.close()


if __name__ == "__main__":
    main()
