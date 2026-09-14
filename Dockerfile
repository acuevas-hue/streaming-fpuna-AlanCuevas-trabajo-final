FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -ntp -DskipTests package
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/target/payments.jar /app/payments.jar
ENV JAVA_TOOL_OPTIONS="-Xmx1024m -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -Dorg.slf4j.simpleLogger.log.py.fpuna.streaming=info"
ENTRYPOINT ["java", "-jar", "/app/payments.jar"]
CMD ["pipeline"]
