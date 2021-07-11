package main

import (
	"flag"
	"fmt"
	"github.com/fourstring/sheetfs/common_journal"
	"github.com/fourstring/sheetfs/election"
	"github.com/fourstring/sheetfs/master/config"
	"github.com/fourstring/sheetfs/master/datanode_alloc"
	"github.com/fourstring/sheetfs/master/filemgr"
	"github.com/fourstring/sheetfs/master/journal"
	"github.com/fourstring/sheetfs/master/server"
	fs_rpc "github.com/fourstring/sheetfs/protocol"
	"google.golang.org/grpc"
	"log"
	"net"
)

var port = flag.Uint("p", 0, "port to listen on")
var forClientAddress = flag.String("a", "", "address for client to connect to this node")
var nodeId = flag.String("i", "", "ID of this node")

func main() {
	flag.Parse()
	db, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	elector, err := election.NewElector(config.ElectionServers, config.ElectionTimeout, config.ElectionZnode, config.ElectionPrefix, config.ElectionAck)
	if err != nil {
		log.Fatal(err)
	}
	_, err = elector.CreateProposal()
	if err != nil {
		log.Fatal(err)
	}
	journalWriter, err := common_journal.NewWriter(config.KafkaServer, config.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
	alloc := datanode_alloc.NewDataNodeAllocator()
	fm := filemgr.LoadFileManager(db, alloc, journalWriter)
	journalListener, err := journal.NewListener(&journal.ListenerConfig{
		NodeID:      *nodeId,
		Elector:     elector,
		KafkaServer: config.KafkaServer,
		KafkaTopic:  config.KafkaTopic,
		FileManager: fm,
		DB:          db,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = journalListener.RunAsSecondary()
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal(err)
	}
	master, err := server.NewServer(fm, datanode_alloc.NewDataNodeAllocator())
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	fs_rpc.RegisterMasterNodeServer(s, master)

	err = elector.AckLeader(*forClientAddress)
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
