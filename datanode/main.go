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

func main() {
	flag.Parse()

	cfg := &node.DataNodeConfig{
		NodeID:           *nodeId,
		Port:             *port,
		ForClientAddr:    *forClientAddress,
		ZookeeperServers: config.ElectionServers,
		ZookeeperTimeout: config.ElectionTimeout,
		ElectionZnode:    config.ElectionZnode,
		ElectionPrefix:   config.ElectionPrefix,
		ElectionAck:      config.ElectionAck,
		KafkaServer:      config.KafkaServer,
		KafkaTopic:       config.KafkaTopic,
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
