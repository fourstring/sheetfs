package main

import (
	"flag"
	"google.golang.org/grpc"
	"log"
	"net"
	fsrpc "sheetfs/protocol"
)

var address = flag.String("a", "", "address to which the datanode listens")

func main() {
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}

	s := grpc.NewServer()
	fsrpc.RegisterDataNodeServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
