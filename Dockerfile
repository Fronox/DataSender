FROM openjdk:8
WORKDIR /datasender
COPY /target/scala-2.12/task.jar /datasender/task.jar
COPY ./config.json /datasender/
COPY ./T1_Final.csv /datasender/
COPY ./EURUSD.csv /datasender/
ENTRYPOINT java -jar task.jar