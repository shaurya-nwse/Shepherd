Shepherd
===

Shepherd herds your Kafka messages to Kinesis.

It has an inbuilt `AIOScheduler` that schedules a poll from Kafka and forwards it to Kinesis based on the time to wait
or the number of records to send.
