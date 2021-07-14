#!/bin/sh
./master -a "$MASTER_ADDR_FOR_CLIENT" -dngroups "$DATANODE_GROUPS" -elack "$ELECTION_ACK" -ekznode "$ELECTION_ZNODE" \
-i "$NODE_ID" -kfserver "$KAFKA_SERVER" -kftopic "$KAFKA_TOPIC" -p "$MASTER_PORT" -zkservers "$ZOOKEEPER_SERVERS"