package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/fourstring/sheetfs/common_journal"
	journal_example "github.com/fourstring/sheetfs/common_journal/example"
	"github.com/fourstring/sheetfs/datanode/server"
	"github.com/fourstring/sheetfs/election"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"google.golang.org/grpc"
	"log"
	"net"
	"strings"
	"time"
)

var port = flag.Uint("p", 0, "port to listen on")
var masterZnode = flag.String("m", "", "ID of master node")
var dataPath = flag.String("d", "", "path of data files")
var serverList = flag.String("sl", "", "list of datanode address, split by ; e.g \"127.0.0.1:2181;127.0.0.1:2182;127.0.0.1:2183\"")
var proposerId = flag.String("i", "", "ID of proposer")
var num = flag.Int("n", 5, "Number of messages to produce if this node become primary")
var ckpt = flag.Bool("c", false, "whether to make a checkpoint after produce {num} messages")
var nextEntryOffset = flag.Int64("o", 0, "offset of next entry to be consumed")

var electionZnode = "/datanode_election"
var electionPrefix = "4da1fce7-d3f8-42dd-965d-4c3311661202-n_"
var electionAck = "/datanode_election_ack"

func main() {
	flag.Parse()
	var electionServers = strings.Split(*serverList, ";")

	// Elect
	e, err := election.NewElector(electionServers, 1*time.Second, electionZnode, electionPrefix, electionAck)
	proposal, err := e.CreateProposal()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: My proposal is %s\n", *proposerId, proposal)
	receiver, err := common_journal.NewReceiver(journal_example.KafkaServer, journal_example.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
	err = receiver.SetOffset(*nextEntryOffset)
	if err != nil {
		log.Fatal(err)
	}
	for {
		success, watch, notify, err := e.TryBeLeader()
		if err != nil {
			log.Fatal(err)
		}
		if success {
			break
		}
		fmt.Printf("%s: I'm secondary and watching %s\n", *proposerId, watch)
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
	log.Printf("%s: I'm primary! Start fast forwarding now!\n", *proposerId)

	// init a writer
	writer, err := common_journal.NewWriter(journal_example.KafkaServer, journal_example.KafkaTopic)
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
	log.Printf("%s: I have done all preparations! Start acking as a primary.", *proposerId)
	err = e.AckLeader(*proposerId)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}
	if err != nil {
		log.Fatal(err)
	}
	/*
		Do primary works here
	*/
	ser := grpc.NewServer()
	fsrpc.RegisterDataNodeServer(ser, s)

	var masterClient fsrpc.MasterNodeClient // TODO: ask what to get master node
	rep, err := masterClient.RegisterDataNode(context.Background(), &fsrpc.RegisterDataNodeRequest{Addr: *proposerId})
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
