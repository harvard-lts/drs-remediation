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

ENV APP_ID_NUMBER=200
ENV APP_ID_NAME=appuser
ENV GROUP_ID_NUMBER=199
ENV GROUP_ID_NAME=appadmin

# external volume
VOLUME /external

# set deployment directory
WORKDIR /s3

# copy over the built artifact from the maven image
COPY  --from=maven ./target/drs-remediate-jar-with-dependencies.jar ./drs-remediate.jar

RUN groupadd -g ${GROUP_ID_NUMBER} ${GROUP_ID_NAME} && \
    useradd -u ${APP_ID_NUMBER} -g ${GROUP_ID_NUMBER} -s /bin/bash ${APP_ID_NAME} && \ 
    chown appuser:appadmin -R /s3

USER appuser

# run java command
CMD ["java", "-jar", "-Xmx8192m", "./drs-remediate.jar"]
