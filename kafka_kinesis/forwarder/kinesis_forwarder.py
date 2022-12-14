import concurrent.futures
import logging
import sys
from typing import Callable, Generic, TypeVar, Optional, List
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import atexit
import time
import threading
import uuid
import boto3

from kafka_kinesis.serdes.serializers import DataclassSerializer
from kafka_kinesis.config import config

logging.getLogger("boto").setLevel(logging.DEBUG)
logger = config.logger

T = TypeVar("T")


# pylint: disable=too-many-instance-attributes,expression-not-assigned
class KinesisForwarder(Generic[T]):
    """Generic class for producing to Kinesis"""

    def __init__(
        self,
        stream_name: str,
        batch_size: int,
        batch_time: int,
        max_retries: int,
        threads: int,
        kinesis_client: boto3.client,
        flush_callback: Callable[[int, float], None],
    ):
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.batch_time = batch_time
        self.max_retries = max_retries
        self.kinesis_client = kinesis_client if kinesis_client else boto3.client("kinesis")
        self.flush_callback = flush_callback
        # internal attributes
        self.queue = Queue()  # thread-safe
        self.lock = threading.Lock()
        self.queue_size_bytes = 0
        self.last_flush = time.time()
        self.pool = ThreadPoolExecutor(threads)
        self.monitor_batch = threading.Event()
        self.monitor_batch.set()
        self.pool.submit(self.monitor).add_done_callback(self.thread_callback)

        # Flush queue and release resources atexit
        atexit.register(self.close)

    def monitor(self):
        """Monitor batch time"""
        while self.monitor_batch.is_set():
            if time.time() - self.last_flush > self.batch_time:
                if not self.queue.empty():
                    logger.info("Flushing queue. Batch time exceeded.")
                    self.flush_queue()
            time.sleep(self.batch_time)

    def put_records(self, records: List[T], partition_key: Optional[str] = None):
        """
        If we forward as bytes, random UUID would be used
        to distribute records between shards.
        """
        logger.info(f"Received {len(records)} records")
        for record in records:
            (
                self.put_record(record, partition_key=getattr(record, partition_key))
                if partition_key
                else self.put_record(record)
            )

    # TODO: choose serializer based on type
    def put_record(self, data: T, partition_key: Optional[str] = None):
        """Put a single record"""
        logger.info(f"Received record: {data}")
        if not isinstance(data, bytes):
            serializer = DataclassSerializer[T]()
            data = serializer.serialize(data)

        if not partition_key:
            partition_key = uuid.uuid4().hex

        record = {"Data": data, "PartitionKey": partition_key}
        logger.info(f"Final kinesis record: {record}")
        record_size_bytes = sys.getsizeof(record)

        # Check if queue has reached batch size (number of records)
        if self.queue.qsize() >= self.batch_size:
            logger.info("Flushing queue. Batch size reached.")
            self.pool.submit(self.flush_queue).add_done_callback(self.thread_callback)

        # add the last record
        logger.debug(f"Putting record {record['Data']}")
        self.queue.put(record)
        self.queue_size_bytes += record_size_bytes

    @staticmethod
    def thread_callback(future: concurrent.futures.Future):
        """Callback for ThreadPool futures"""
        exc = future.exception()
        if exc:
            raise exc
        logger.info("Future successful.")

    def close(self):
        """Cleanup resources at exit"""
        logger.info("Closing Kinesis forwarder.")
        self.flush_queue()
        self.monitor_batch.clear()
        self.pool.shutdown()
        logger.info("Done.")

    def flush_queue(self):
        """Flush thread-safe queue to Kinesis"""
        records = []

        while not self.queue.empty() and len(records) < self.batch_size:
            record = self.queue.get()
            record_size = sys.getsizeof(record)
            self.queue_size_bytes -= record_size
            records.append(record)

        if records:
            logger.info(f"Flushing out {len(records)} to Kinesis")
            self.send_records(records)
            self.last_flush = time.time()
            if self.flush_callback:
                self.flush_callback(
                    len(records),
                    self.last_flush,
                )

    def send_to_dlq(self, records: list):
        """Send failed messages to DLQ"""
        with self.lock:
            with open("failed_records.dlq", "ab") as f:
                for r in records:
                    f.write(f"{r.get('Data')},{r.get('PartitionKey')}\n".encode("UTF-8"))

    def send_records(self, records, attempt=0):
        """Send messages to Kinesis"""
        if attempt > self.max_retries:
            logger.warning(f"Writing {len(records)} to SQS DLQ")
            # TODO: Setup SQS as a DLQ
            self.send_to_dlq(records)
            return

        if attempt:
            time.sleep(2**attempt * 0.1)

        try:
            response = self.kinesis_client.put_records(StreamName=self.stream_name, Records=records)
            logger.info(response)
            failed_record_count = response["FailedRecordCount"]
            logger.info(f"Failed record count: {failed_record_count}")
            # retry failed records
            if failed_record_count > 0:
                logger.warning("Retrying failed records")
                failed_records = []
                for i, record in enumerate(response["Records"]):
                    if record.get("ErrorCode"):
                        failed_records.append(records[i])
                self.send_records(failed_records, attempt + 1)
        except Exception as e:
            logger.error(f"Encountered Error: {e}")
            self.send_to_dlq(records)
            raise e


# pylint: enable=too-many-instance-attributes,expression-not-assigned
