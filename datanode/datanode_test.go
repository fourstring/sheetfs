package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	fsrpc "sheetfs/protocol"
	"testing"
)

func TestDatanode(t *testing.T) {
	s := server{}

	dir, _ := ioutil.ReadDir("../data")
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"../data", d.Name()}...))
	}

	// first create
	testString := "this is the test data"
	data := []byte(testString)
	size := len(testString)
	req := fsrpc.WriteChunkRequest{Id: 1, Data: data, Size: uint64(size), Padding: " ", Version: 0}
	res, _ := s.WriteChunk(context.Background(), &req)
	if res.Status != fsrpc.Status_OK {
		t.Error("wrong")
	}

	// wrong version
	res, _ = s.WriteChunk(context.Background(), &req)
	if res.Status != fsrpc.Status_WrongVersion {
		t.Error("wrong")
	}

	readReq := fsrpc.ReadChunkRequest{Id: 1, Size: 21, Version: 0}
	readRes, _ := s.ReadChunk(context.Background(), &readReq)
	if readRes.Status != fsrpc.Status_WrongVersion {
		t.Error("wrong")
	}

	readReq = fsrpc.ReadChunkRequest{Id: 1, Size: 21, Version: 1}
	readRes, _ = s.ReadChunk(context.Background(), &readReq)
	if string(readRes.Data) != "this is the test data" {
		t.Error("wrong")
	}

	// correct version new data
	testString = "this is the new test data"
	data = []byte(testString)
	size = len(testString)
	req = fsrpc.WriteChunkRequest{Id: 1, Data: data, Size: uint64(size), Padding: " ", Version: 1}
	_, _ = s.WriteChunk(context.Background(), &req)

	readReq = fsrpc.ReadChunkRequest{Id: 1, Size: 25, Version: 2}
	readRes, _ = s.ReadChunk(context.Background(), &readReq)
	if string(readRes.Data) != "this is the new test data" {
		t.Error("wrong")
	}

	deleteReq := fsrpc.DeleteChunkRequest{Id: 1}
	deleteRes, _ := s.DeleteChunk(context.Background(), &deleteReq)
	if deleteRes.Status != fsrpc.Status_OK {
		t.Error("wrong")
	}

	readReq = fsrpc.ReadChunkRequest{Id: 1, Size: 25, Version: 2}
	readRes, _ = s.ReadChunk(context.Background(), &readReq)
	if readRes.Status != fsrpc.Status_NotFound {
		t.Error("wrong")
	}
}

func TestDatanodeParallel(t *testing.T) {
	//s := server{}
	//
	//dir, _ := ioutil.ReadDir("../data")
	//for _, d := range dir {
	//	os.RemoveAll(path.Join([]string{"../data", d.Name()}...))
	//}

	// TODO
}
