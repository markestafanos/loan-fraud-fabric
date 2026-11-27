"""
producer.py
-----------
Streams loan-application events to Azure Event Hubs through its KAFKA endpoint
using confluent-kafka (Darshil's version used the azure-eventhub SDK instead).
Examples
  python producer/producer.py --dry-run --minutes 1      # print events, no Azure needed
  python producer/producer.py --rate 2 --minutes 60      # real run: ~7,200 applications
Settings come from a .env file in the project root (never commit it):
  EH_NAMESPACE=ehns-loanfraud-mark
  EH_TOPIC=loan_events
  EH_CONNECTION_STRING=Endpoint=sb://....;SharedAccessKeyName=send-policy;SharedAccessKey=....
"""

import argparse
import json                              # dictionary -> JSON text
import os                                # os.environ = the settings loaded from .env
import time                              # time.time() = now in seconds, time.sleep() = pause
from datetime import datetime, timezone
from dotenv import load_dotenv           # reads .env into os.environ
from generator import EventGenerator     # the class you wrote in generator.py


def build_producer():
    from confluent_kafka import Producer  # imported here so --dry-run works without it
    namespace = os.environ["EH_NAMESPACE"]   # read a setting from .env
    conf = {                                 # Kafka settings (a dictionary)
        "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",  # Kafka endpoint, port 9093
        "security.protocol": "SASL_SSL",                                   # encrypted + logged in
        "sasl.mechanisms": "PLAIN",                                        # login = username + password
        "sasl.username": "$ConnectionString",                              # literally this text
        "sasl.password": os.environ["EH_CONNECTION_STRING"],               # your send-policy key
        "client.id": "loan-fraud-producer",                                # a name for this program
        "linger.ms": 20,                    # small batching window, keeps latency low
        "request.timeout.ms": 60000,        # values Microsoft recommends for Event Hubs
        "socket.keepalive.enable": True,
        "metadata.max.age.ms": 180000,
    }
    return Producer(conf)                    # the connected Kafka producer


def main():
    load_dotenv()                            # load .env so os.environ has your settings
    p = argparse.ArgumentParser()            # options you can type after the command
    p.add_argument("--rate", type=float, default=2.0, help="send slots per second")
    p.add_argument("--minutes", type=float, default=60)
    p.add_argument("--fraud-rate", type=float, default=0.06)
    p.add_argument("--seed", type=int, default=int(time.time()))   # different random data each run
    p.add_argument("--dry-run", action="store_true")                # just a switch: on if typed
    args = p.parse_args()
    gen = EventGenerator(fraud_rate=args.fraud_rate, seed=args.seed)   # create ONE generator
    topic = os.getenv("EH_TOPIC", "loan_events")                       # getenv = read, with a default
    producer = None if args.dry_run else build_producer()              # no Kafka needed in a dry run
    stats = {"sent": 0, "planted": 0, "failed": 0}                     # counters

    def on_delivery(err, msg):
        # a "callback": Kafka calls this later for every message, err is None if it arrived
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed: {err}")

    end = time.time() + args.minutes * 60    # the time to stop (now + minutes in seconds)
    try:                                     # try ... finally: the finally part ALWAYS runs
        while time.time() < end:             # keep going until the time is up
            for event, scenario in gen.tick(datetime.now(timezone.utc)):   # everything due now
                payload = json.dumps(event)  # dictionary -> JSON text
                if args.dry_run:
                    print(payload if scenario is None else f"[PLANTED {scenario}] {payload}")
                else:
                    # key = customer_id -> all events for one customer land in the same partition (keeps order)
                    producer.produce(topic, key=event["customer"]["customer_id"], value=payload,
                                     on_delivery=on_delivery)
                    producer.poll(0)         # give Kafka a moment to run the delivery callbacks
                stats["sent"] += 1
                stats["planted"] += scenario is not None       # True counts as 1, False as 0
            if stats["sent"] and stats["sent"] % 500 == 0:     # % = remainder: print every 500 events
                print(f"{stats['sent']:,} sent | {stats['planted']:,} planted fraud | {stats['failed']} failed")
            time.sleep(1 / args.rate)        # rate 2 -> wait 0.5 seconds between sends
    except KeyboardInterrupt:                # you pressed Ctrl+C
        print("Stopping...")
    finally:
        if producer:
            producer.flush(30)               # wait up to 30 s for unsent messages to go
        print(f"Done. {stats['sent']:,} events sent, {stats['planted']:,} planted fraud, "
              f"{stats['failed']} failed. Answer key: data/planted_fraud.csv")


if __name__ == "__main__":
    main()