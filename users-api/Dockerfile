# Stage 1: Build using a maintained Maven image with JDK 8
FROM maven:3.9-eclipse-temurin-8 AS builder

WORKDIR /app

COPY pom.xml .

RUN mvn dependency:go-offline -B

COPY src/ ./src/

RUN mvn clean package -DskipTests -B

# Stage 2: Runtime using a maintained JRE 8 image
FROM eclipse-temurin:8-jre

WORKDIR /app

# Ensure the JAR name matches your pom.xml definition
COPY --from=builder /app/target/users-api-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8083

CMD ["java", "-jar", "app.jar"]
