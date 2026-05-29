- You will work only in the directory `capstone`
- Build with python 3.12
- Take the `capstone/docker-compose.yaml` as a base
- Apps (producers/consumers/etc.) should be added to the docker-compose with the "app" profile
- The app should be able to be executed in two configurations:
    1. Everything runs by docker compose (maybe with different steps, first infra, then apps, but still)
    2. Infra is started with docker compose, the rest is started manually
- Input video file is the following: `capstone/input.mp4`
- First, propose a plan and show me an architecture diagram. Advise on the NN decision.

# Task description
Topic: E2E data processing pipeline - processing video streams
Goals: Learn about implementation of E2E data processing pipelines using kafka for processing video streams

Instructions:
    1) Prepare a datatset - a videofile that consists of cars and people.
    2) Prepare a kafka environment.
    3) Implement a “generator” microservice that splits the video file to frames, sends them to kafka as messages.
    4) Implement a microservice that preprocesses video frames if required
    5) Implement a microservice that detects cars in video frames (any pretrained NN)
    6) Implement a microservice that detects a person in video frames (any pretrained NN)
    7) Implement a microservice that tracks a person in video frames (e.g. DeepSearch or centroid tracking with OpenCV)
    8) Implement a microservice that tracks a car in video frames (e.g. DeepSearch or centroid tracking with OpenCV)
    9) Implement a microservice that generates and displays statistics :
                    a) number of detected unique cars
                    b) number of detected unique people