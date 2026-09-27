FROM amazoncorretto:25.0.2@sha256:4110fa04592ff5ae7431e58472226dd4573fdd752df294445682ba929949b809
WORKDIR /probe
COPY spikes/bm25_overlay /probe
RUN mkdir deps classes \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/lucene/lucene-core/10.4.0/lucene-core-10.4.0.jar -o deps/lucene-core-10.4.0.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/apache/lucene/lucene-analysis-common/10.4.0/lucene-analysis-common-10.4.0.jar -o deps/lucene-analysis-common-10.4.0.jar \
    && sha256sum -c SHA256SUMS \
    && javac -Xlint:all -Werror -cp 'deps/*' -d classes OverlayProbe.java
ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-cp", "classes:deps/*", "OverlayProbe"]
