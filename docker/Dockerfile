FROM maven:3.9.9-eclipse-temurin-17

WORKDIR /src
COPY . .

ARG SKIP_UNIT_TESTS=false
ENV SKIP_UNIT_TESTS=${SKIP_UNIT_TESTS}

RUN --mount=type=cache,target=/root/.m2 \
    sh -c 'if [ "$SKIP_UNIT_TESTS" = "true" ]; then mvn -B package -DskipTests -DskipITs; else mvn -B package -DskipITs; fi'
