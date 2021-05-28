FROM openjdk:8u181-alpine3.8

ADD solarfile.jar /srv/solarfile.jar

EXPOSE 3000

CMD ["java", "-cp", "/srv/solarfile.jar", "clojure.main", "-m", "solarfile.main"]
