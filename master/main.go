package main

import (
	"flag"
	"github.com/fourstring/sheetfs/master/datanode_alloc"
	"github.com/fourstring/sheetfs/master/server"
	fs_rpc "github.com/fourstring/sheetfs/protocol"
	"google.golang.org/grpc"
	"log"
	"net"
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
	master, err := server.NewServer(db, datanode_alloc.NewDataNodeAllocator())
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	fs_rpc.RegisterMasterNodeServer(s, master)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
