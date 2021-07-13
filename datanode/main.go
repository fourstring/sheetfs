package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/fourstring/sheetfs/common_journal"
	journal_example "github.com/fourstring/sheetfs/common_journal/example"
	"github.com/fourstring/sheetfs/datanode/config"
	"github.com/fourstring/sheetfs/datanode/server"
	"github.com/fourstring/sheetfs/election"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"github.com/go-zookeeper/zk"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

var port = flag.Uint("p", 0, "port to listen on")
var addr = flag.String("m", "", "addr of datanode")
var dataPath = flag.String("d", "", "path of data files")
var nextEntryOffset = flag.Int64("o", 0, "offset of next entry to be consumed")

func main() {
	flag.Parse()

	// Elect
	e, err := election.NewElector(config.ElectionServers, 1*time.Second,
		config.ElectionZnode, config.ElectionPrefix, config.ElectionAck)
	if err != nil {
		log.Fatal(err)
	}
	_, err = e.CreateProposal()
	if err != nil {
		log.Fatal(err)
	}
	receiver, err := common_journal.NewReceiver(config.KafkaServer, config.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
	err = receiver.SetOffset(*nextEntryOffset)
	if err != nil {
		log.Fatal(err)
	}
	for {
		success, _, notify, err := e.TryBeLeader()
		if err != nil {
			log.Fatal(err)
		}
		if success {
			break
		}
		ctx := common_journal.NewZKEventCancelContext(context.Background(), notify)

		// init a writer
		writer, err := common_journal.NewWriter(journal_example.KafkaServer, journal_example.KafkaTopic)
		if err != nil {
			log.Fatal(err)
		}
		s := server.NewServer(*dataPath, writer)
		/*
			Generally, a secondary node should invoke receiver.FetchEntry to blocking fetch and
			applies entries until it realized that it has become a primary node.
		*/
		for {
			msg, ckpt, err := receiver.FetchEntry(ctx)
			// Sleep intentionally here to show forwarding all remaining entries when become a primary node.
			time.Sleep(1 * time.Second)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					break
				} else {
					log.Fatal(err)
				}
			}
			s.HandleMsg(msg, ckpt)
		}
	}

	/*
		MUST complete all preparation required to serve requests before AckLeader!
	*/
	// init a writer
	writer, err := common_journal.NewWriter(config.KafkaServer, config.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
	s := server.NewServer(*dataPath, writer)
	for {
		msg, ckpt, err := receiver.TryFetchEntry(context.Background())
		if err != nil {
			// New primary has consumed all remaining messages.
			if errors.Is(err, &common_journal.NoMoreMessageError{}) {
				break
			} else {
				log.Fatal(err)
			}
		}
		s.HandleMsg(msg, ckpt)
	}

	err = e.AckLeader(*addr)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}
	/*
		Do primary works here
	*/
	ser := grpc.NewServer()
	fsrpc.RegisterDataNodeServer(ser, s)

	// TODO: ask what to get master node
	connect, _, err := zk.Connect(config.ElectionServers, 1*time.Second)
	if err != nil {
		log.Fatalf("Connect masternode server failed.")
	}
	masterAddr, _, err := connect.Get(config.MasterAck)
	if err != nil {
		log.Fatalf("Get master address from commection failed.")
	}
	conn, _ := grpc.Dial(string(masterAddr))
	var masterClient = fsrpc.NewMasterNodeClient(conn)

	rep, err := masterClient.RegisterDataNode(context.Background(),
		&fsrpc.RegisterDataNodeRequest{Addr: *addr})
	if err != nil {
		log.Fatalf("%s", err)
	}
	if rep.Status != fsrpc.Status_OK {
		log.Fatalf("Register failed.")
	}
	if err := ser.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
