# producerS3
This package is part of my Insight Project [nexTop](https://github.com/rentzso/nextop)

This library runs a Kafka Producer that does the following:
- pulls data from the GDELT dataset stored in S3
- for each line creates an Avro message, with a URL and its related topics
- pushes these messages to a Kafka topic
