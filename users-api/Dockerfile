# Stage 1: Build
FROM maven:3.8-openjdk-8 AS build
WORKDIR /app

# Copy only pom.xml and download dependencies for caching
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code and build
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Production
FROM openjdk:8-jdk-slim
WORKDIR /app

# Create non-root user
RUN addgroup --system appgroup && adduser --system appuser --ingroup appgroup
USER appuser

# Copy the built JAR
COPY --from=build /app/target/*.jar app.jar

# Expose port
EXPOSE 8083

# Run the app
CMD ["java", "-jar", "app.jar"]
