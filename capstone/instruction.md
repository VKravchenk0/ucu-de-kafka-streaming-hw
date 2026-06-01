- You will work only in the directory `capstone`
- Build with python 3.12
- Ignore the previous implementation that can be found in git files
- Take the `capstone/docker-compose.yaml` as a base
- Apps (producers/consumers/etc.) should be added to the docker-compose with the "app" profile
- The app should be able to be executed in two configurations:
    1. Everything runs by docker compose (maybe with different steps, first infra, then apps, but still)
    2. Infra is started with docker compose, the rest is started manually
- test video file is the following: `capstone/input.mp4`
- First, propose a plan and show me an architecture diagram. Advise on the NN decision.

# Task description
Topic: E2E data processing pipeline - processing video streams
Goals: Learn about implementation of E2E data processing pipelines using kafka for processing video streams

Instructions:
    1) Prepare a datatset - a videofile that consists of cars and people.
    2) Prepare a kafka environment.
    3) Implement a webpage on which the user can upload the video. The backend then splits the video file to frames, sends them to kafka as messages.
    4) Implement a microservice that preprocesses video frames if required
    5) Implement a microservice that detects cars in video frames (any pretrained NN)
    6) Implement a microservice that detects a person in video frames (any pretrained NN)
    7) Implement a microservice that tracks a person in video frames (e.g. DeepSearch or centroid tracking with OpenCV)
    8) Implement a microservice that tracks a car in video frames (e.g. DeepSearch or centroid tracking with OpenCV)
    9) The user should be able to watch the video on the web page. Implement a microservice for that if needed.
        - the bounding boxes with object ids (Car 11) should be also visible on the video
        - The page should contain:
            a) number of detected unique cars in the frame
            b) number of detected unique cars total
            c) number of detected unique people in the frame
            d) number of detected unique people total
    10) Implement a microservice that generates and displays statistics :
                    a) number of detected unique cars
                    b) number of detected unique people
    11) The implementation should use kafka streams
    12) Write all the required info into README.md
    13) Create a memory bank. Make sure to update the memory bank when the project is being updated (save the instruction into CLAUDE.md?)
    14) Multiple users should be able to work in the system simultaneously
    15) Make sure to partition the streams correctly to ensure the parallelism
    16) Aim for the near-real time processing (don't wait for the whole video to be processed. I want to start watching the video right away)
    17) Use context7 mcp for the actual libraries
    18) Use python 3.12

I'm not an expert in this kind of stuff. But I guess we can join the main video stream with the streams of bounding boxes

