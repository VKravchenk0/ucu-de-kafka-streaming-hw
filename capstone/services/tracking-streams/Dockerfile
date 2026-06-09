FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build
COPY tracking-streams/pom.xml .
RUN mvn -B -q dependency:go-offline

COPY tracking-streams/src ./src
RUN mvn -B -q package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /build/target/tracking-streams-jar-with-dependencies.jar app.jar

CMD ["java", "-jar", "app.jar"]
