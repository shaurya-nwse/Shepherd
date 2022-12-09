import asyncio
from datetime import timedelta
import boto3
from aiokafka import AIOKafkaConsumer
from kafka_kinesis.common.scheduler import AIOScheduler
from kafka_kinesis.consumer import KafkaConsumer
from kafka_kinesis.forwarder import KinesisForwarder
from kafka_kinesis.config import config

# boto3.setup_default_session(profile_name="saml")
kinesis_client = boto3.client("kinesis")
logger = config.logger


# Simple forwarder
async def main():
    """Main"""

    logger.info("Starting Kinesis forwarder")
    kinesis_forwarder = KinesisForwarder[bytes](
        stream_name=config.STREAM_NAME,
        batch_size=config.BATCH_SIZE,
        batch_time=config.BATCH_TIME,
        max_retries=config.MAX_RETRIES,
        threads=config.NUM_THREADS,
        kinesis_client=kinesis_client,
        flush_callback=lambda x, y: print(f"Num records: {x}\nFlush time: {y}"),
    )

    logger.info("Starting Kafka consumer")
    kafka_consumer = KafkaConsumer(
        config.TOPIC,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset=config.AUTO_OFFSET_RESET,
        group_id=config.GROUP_ID,
    ).create_consumer()

    logger.info("Waiting for the consumer...")
    await kafka_consumer.start()

    @AIOScheduler(
        freq=timedelta(seconds=10),
        args=(
            kafka_consumer,
            kinesis_forwarder,
        ),
    )
    async def poll(consumer: AIOKafkaConsumer, forwarder: KinesisForwarder):
        data = await consumer.getmany(timeout_ms=10000)
        messages = []
        for tp, message in data.items():
            logger.info(msg=f"Message: {message}\n Meta: {tp}")
            messages.extend([m.value for m in message])
        forwarder.put_records(messages)
        await consumer.commit()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
