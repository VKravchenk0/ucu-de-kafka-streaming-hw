Task: investigation of kafka throughtput.
Original task description: file `hw2/original_description.md4`. Make sure that your work conforms with the requirements in that file.

- Implement microservices
    - hw2/producer
    - hw2/consumer
    - hw2/stat-aggregator (if it has to be a separate service)
- Use plain java (java 25). No spring framework
- Use the latest version of org.apache.kafka:kafka-clients library
- Take `hw2/docker-compose.yaml` as a base for docker compose. Add "infra" profiles to the kafka-related services. I want this because I want to be able to run the app in two configurations.
Configuration 1: Kafka runs in docker, producers and consumers runs manually
Configuration 2: Everything runs in docker (kafka and producers/consumers)
- Don't overengineer. Create only the code that is absolutely necessary to complete the task
- Don't update the existing kafka docker-compose config, until absolutely necessary (you can add profiles, add the microservices that need to be implemented; for the rest of the updates - ask for permission)
- The number of producers/partitions/consumers should be passed as a configuration (in docker .env files and as cli parameters to java -jar command)
- Use maven with maven wrapper
- Use lombok
- In dockerfiles, use eclips temurin 25 as base image
- Make sure to test running the app yourself. Both in "docker + standalone" version as well as fully docker version. If docker fails for some reason - you can delete the containers and related docker volumes to start from scratch
- Don't do the experiments yourself (you can do just to test that parameters are passed correctly; but I'll do the complete experiment myself)
- Multiple producers and consumers must start in different java threads (but as a part of a single instance of a microservice)