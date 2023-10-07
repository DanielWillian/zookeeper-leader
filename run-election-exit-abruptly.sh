#!/bin/sh

main() {
  jbang run ZookeeperLeader.java --node-name=first &
  FIRST_PID=$!
  sleep 1
  jbang run ZookeeperLeader.java --node-name=second &
  SECOND_PID=$!
  sleep 2
  echo "Killing first process!"
  kill ${FIRST_PID}
  wait ${SECOND_PID}
}

main
