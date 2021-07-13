package config

import (
	"time"
)

const (
	ElectionZnode      = "/datanode_election"
	ElectionAck        = "/datanode_election_ack"
	MasterAck          = "/master-ack"
	ElectionPrefix     = "4da1fce7-d3f8-42dd-965d-4c3311661202-n_"
	ElectionTimeout    = 1 * time.Second
	CheckpointInterval = 1 * time.Minute
)

var ElectionServers = []string{
	"127.0.0.1:2181",
	"127.0.0.1:2182",
	"127.0.0.1:2183",
}
var KafkaServer = "127.0.0.1:9093"
var KafkaTopic = "datanode_journal"
