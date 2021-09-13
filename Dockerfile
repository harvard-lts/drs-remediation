# build base image
FROM maven:3-openjdk-11-slim as maven

# copy pom.xml
COPY ./pom.xml ./pom.xml

# copy src files
COPY ./src ./src

# build
RUN mvn package -DskipTests

# final base image
FROM openjdk:11-jre-slim

# external volume
VOLUME /external

# set deployment directory
WORKDIR /s3

# copy over the built artifact from the maven image
COPY  --from=maven ./target/drs-remedaite-jar-with-dependencies.jar ./drs-remedaite.jar

# run java command
CMD ["java", "-jar", "-Xmx8192m", "./drs-remedaite.jar"]
