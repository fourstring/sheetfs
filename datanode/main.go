package main

import (
	"flag"
	"github.com/fourstring/sheetfs/datanode/config"
	"github.com/fourstring/sheetfs/datanode/node"
	"log"
	"strings"
)

var port = flag.Uint("p", 0, "port to listen on")
var forClientAddress = flag.String("a", "", "address for client to connect to this node")
var nodeId = flag.String("i", "", "ID of this node")
var nodeGroupName = flag.String("gn", "", "name of the node group, e.g node1")
var zkServerList = flag.String("sl", "", "server address list split by ';', e.g addr1;addr2;addr3")
var electionPrefix = flag.String("ep", "", "election prefix")

func main() {
	flag.Parse()

	cfg := &node.DataNodeConfig{
		NodeID:           *nodeId,
		Port:             *port,
		ForClientAddr:    *forClientAddress,
		ElectionPrefix:   *electionPrefix,
		DataDirPath:      config.DIR_DATA_PATH,
		ZookeeperServers: strings.Split(*zkServerList, ";"),
		ZookeeperTimeout: config.ElectionTimeout,
		ElectionZnode:    config.ElectionZnodePrefix + *nodeGroupName,
		ElectionAck:      config.ElectionAckPrefix + *nodeGroupName,
		KafkaServer:      config.KafkaServer,
		KafkaTopic:       config.KafkaTopicPrefix + *nodeGroupName,
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
