FROM openjdk:8-jdk-alpine AS builder

WORKDIR /root
RUN apk add busybox-extras maven nano openssh gradle

COPY settings.gradle .
COPY build.gradle .
COPY src src/

RUN gradle buildJar

FROM openjdk:8-jdk-alpine
WORKDIR /root
COPY --from=builder /root/build/libs/cue2-show-data-migration-*.jar data-migration.jar
ENTRYPOINT ["java", "-jar", "-Xms3072m", "-Xmx3072m",  "data-migration.jar"]
