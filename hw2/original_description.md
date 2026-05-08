Practical task - 2. Investigation of kafka throughput

Goals:

Learn about implementation of dummy distributed application that uses kafka for communication between components
Investigate throughput of a kafka-based solution considering different number of producers, consumers, partitions and replicas

Instructions:

- Input dataset: file `hw2/input.mp4` (should be configurable). 
- Prepare a kafka environment.
- Implement a “generator” microservice that splits the dataset to messages (videoframes, reddit comments), sends them to kafka as a message.
- Implement a microservice that receive messages (videoframes), imitates processing by sleeping for 1 sec and saves timestamps to a file, e.g. csv. Consumers should log frame numbers that processed by a consumer
- Implement a microservice that aggregates logs from consumers and generates a report:
a) throughput of the system in Mbps
b) latency of message processing: max time processing that includes period of time between the message sending time and finishing time of message processing.

Do experiments with different configuration of producers, consumers and topics
a) One producer, a topic with one partition, one consumer
b) One producer, a topic with one partition, 2 consumers
c) One producer, a topic with 2 partitions, 2 consumers
d) One producer, a topic with 5 partitions, 5 consumers
e) One producer, a topic with 10 partitions, 1 consumers
f) One producer, a topic with 10 partitions, 5 consumers
g) One producer, a topic with 10 partitions, 10 consumers
h) 2 producers (input data should be split into 2 parts somehow), a topic with 10 partitions, 10 consumers

