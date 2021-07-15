package config

import (
	"time"
)

const (
	BLOCK_SIZE          = 2 << 10
	DIR_DATA_PATH       = "../data/"
	ElectionZnodePrefix = "/datanode_election_"
	ElectionAckPrefix   = "/datanode_election_ack_"
	MasterAck           = "/master_election_ack"
	ElectionPrefix      = "4da1fce7-d3f8-42dd-965d-4c3311661202-n_"
	ElectionTimeout     = 1 * time.Second
)

var ElectionServers = []string{
	"127.0.0.1:2181",
	"127.0.0.1:2182",
	"127.0.0.1:2183",
}
var KafkaServer = "127.0.0.1:9093"
var KafkaTopicPrefix = "datanode_journal_"
