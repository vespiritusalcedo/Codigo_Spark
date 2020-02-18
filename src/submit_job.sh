#!/bin/bash
spark-submit \
--class Job.StructuredStreaming \
--master local[*] \
./target/stream_avg-1.0-SNAPSHOT.jar