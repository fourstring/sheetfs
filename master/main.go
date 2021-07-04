package main

import (
	"flag"
	"google.golang.org/grpc"
	"log"
	"net"
	fs_rpc "sheetfs/protocol"
)

var address = flag.String("a", "", "address to which the master listens")

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	db, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}
	master, err := NewServer(db)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	fs_rpc.RegisterMasterNodeServer(s, master)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
