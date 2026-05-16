FROM flink:1.20.3-java17

COPY conf/log4j-console.properties /opt/flink/conf/log4j-console.properties
COPY conf/logTemplate.json /opt/flink/conf/logTemplate.json
COPY conf/log4j-layout-template-json-2.24.3.jar /opt/flink/lib/log4j-layout-template-json-2.24.3.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop
COPY plugins/flink-s3-fs-hadoop-1.20.3.jar /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.20.3.jar
