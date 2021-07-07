package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/fourstring/sheetfs/datanode/server"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"google.golang.org/grpc"
	"log"
	"net"
)

var port = flag.Uint("p", 0, "port to listen on")
var forClientAddress = flag.String("a", "", "address to which the datanode listens")
var masterAddr = flag.String("m", "", "address of master node")
var dataPath = flag.String("d", "", "path of data files")

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}

	s := grpc.NewServer()
	fsrpc.RegisterDataNodeServer(s, server.NewServer(*dataPath))

	conn, err := grpc.Dial(*masterAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("%s", err)
	}
	masterClient := fsrpc.NewMasterNodeClient(conn)
	rep, err := masterClient.RegisterDataNode(context.Background(), &fsrpc.RegisterDataNodeRequest{Addr: *forClientAddress})
	if err != nil {
		log.Fatalf("%s", err)
	}
	if rep.Status != fsrpc.Status_OK {
		log.Fatalf("Register failed.")
	}
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
