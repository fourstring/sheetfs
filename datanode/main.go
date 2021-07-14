package main

import (
	"flag"
	"github.com/fourstring/sheetfs/datanode/config"
	"github.com/fourstring/sheetfs/datanode/node"
	"log"
)

var port = flag.Uint("p", 0, "port to listen on")
var forClientAddress = flag.String("a", "", "address for client to connect to this node")
var nodeId = flag.String("i", "", "ID of this node")
var nodeName = flag.String("n", "", "name of this node, e.g node1")

func main() {
	flag.Parse()

	cfg := &node.DataNodeConfig{
		NodeID:           *nodeId,
		Port:             *port,
		ForClientAddr:    *forClientAddress,
		ZookeeperServers: config.ElectionServers,
		ZookeeperTimeout: config.ElectionTimeout,
		ElectionZnode:    config.ElectionZnodePrefix + *nodeName,
		ElectionPrefix:   config.ElectionPrefix,
		ElectionAck:      config.ElectionAckPrefix + *nodeName,
		KafkaServer:      config.KafkaServer,
		KafkaTopic:       config.KafkaTopicPrefix + *nodeName,
	}

	mnode, err := node.NewDataNode(cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = mnode.Run()
	if err != nil {
		log.Fatal(err)
	}
}
