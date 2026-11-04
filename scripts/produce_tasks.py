#!/usr/bin/env python3
"""
Produce sample task events to the tasks.events Kafka topic.

Usage:
    python scripts/produce_tasks.py                   # send built-in 3-task sample
    python scripts/produce_tasks.py --count 100       # generate a 100-task chain
    python scripts/produce_tasks.py --bootstrap localhost:9092
"""
import argparse
import asyncio
import json

from aiokafka import AIOKafkaProducer

from app.config import settings

SAMPLE_TASKS = [
    {"id": "t1", "title": "Root Task",   "parent_task_id": None},
    {"id": "t2", "title": "Middle Task", "parent_task_id": "t1"},
    {"id": "t3", "title": "Leaf Task",   "parent_task_id": "t2"},
]


def generate_tasks(n: int) -> list[dict]:
    """Generate n tasks as a linear chain: t1 -> t2 -> ... -> tn."""
    if n < 1:
        raise ValueError(f"count must be >= 1, got {n}")
    tasks = [{"id": "t1", "title": "Task 1", "parent_task_id": None}]
    for i in range(2, n + 1):
        tasks.append({"id": f"t{i}", "title": f"Task {i}", "parent_task_id": f"t{i - 1}"})
    return tasks


async def produce(bootstrap_servers: str, tasks: list[dict], topic: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for task in tasks:
            value = json.dumps(task).encode()
            await producer.send_and_wait(topic, value)
            print(f"Sent: {task}")
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce task events to Kafka")
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default=settings.kafka_topic,
        help=f"Kafka topic to produce to (default: {settings.kafka_topic!r})",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of tasks to generate; omit for 3-task named sample",
    )
    args = parser.parse_args()
    tasks = SAMPLE_TASKS if args.count is None else generate_tasks(args.count)
    asyncio.run(produce(args.bootstrap, tasks, args.topic))


if __name__ == "__main__":
    main()