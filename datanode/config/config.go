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
	ElectionTimeout     = 1 * time.Second
)

var KafkaServer = "127.0.0.1:9093"
var KafkaTopicPrefix = "datanode_journal_"
