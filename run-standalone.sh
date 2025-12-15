#!/bin/bash

# Run directly with java command (requires mvn compile first)

# Default main class (can be overridden via CLI or MAIN_CLASS env var)
MAIN_CLASS="${MAIN_CLASS:-com.dq.pipeline.DQMain}"

case "$1" in
  varsvar|VarSvar)
    MAIN_CLASS="com.var.pipeline.VarSvarMain"
    shift
    ;;
  dqmain|DQMain)
    MAIN_CLASS="com.dq.pipeline.DQMain"
    shift
    ;;
esac

# Compile if needed (or if chosen main class is missing)
CLASS_FILE="target/classes/${MAIN_CLASS//./\/}.class"
if [ ! -f "${CLASS_FILE}" ]; then
  echo "Compiling project... (missing ${CLASS_FILE})"
  mvn compile
fi

echo "Running application with main: ${MAIN_CLASS}"

# Run with Java 17+ compatibility flags
java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  -cp "target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  "${MAIN_CLASS}" "$@"
