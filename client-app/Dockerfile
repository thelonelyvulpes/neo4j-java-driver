FROM eclipse-temurin:17
RUN mkdir /opt/app
COPY target/open-tel-exp-1.0-SNAPSHOT.jar /opt/app


ENTRYPOINT ["java", "-jar", "/opt/app/open-tel-exp-1.0-SNAPSHOT.jar"]