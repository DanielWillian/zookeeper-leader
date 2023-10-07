#!/bin/sh

main() {
  jbang run ZookeeperLeader.java --node-name=first &
  FIRST_PID=$!
  sleep 2
  jbang run ZookeeperLeader.java --node-name=second &
  SECOND_PID=$!
  wait ${FIRST_PID}
  wait ${SECOND_PID}
}

main
