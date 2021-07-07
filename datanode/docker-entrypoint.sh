#!/bin/sh
./datanode -a "$DATANODE_ADDR_FOR_CLIENT" -m "$MASTER_ADDR" -d "/tmp" -p "$DATANODE_PORT"