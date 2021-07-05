package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	"log"
	"net"
	fsrpc "sheetfs/protocol"
)

var address = flag.String("a", "", "address to which the datanode listens")
var masterAddr = flag.String("m", "", "address of master node")

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}

	s := grpc.NewServer()
	fsrpc.RegisterDataNodeServer(s, &server{})

	conn, err := grpc.Dial(*masterAddr)
	if err != nil {
		log.Fatalf("%s", err)
	}
	masterClient := fsrpc.NewMasterNodeClient(conn)
	rep, err := masterClient.RegisterDataNode(context.Background(), &fsrpc.RegisterDataNodeRequest{Addr: *address})
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
