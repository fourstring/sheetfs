package client

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"log"
	fsrpc "sheetfs/protocol"
	"sync"
)

func connect(data []byte, metadata []byte) []byte {
	var totalData []byte = []byte("{\"celldata\": [") //header
	totalData = append(totalData, data...)            //data
	totalData = totalData[:len(totalData)-1]          // delete the final ","
	totalData = append(totalData, "],"...)            // add "],"
	totalData = append(totalData, metadata...)        //metadata
	totalData = append(totalData, "}"...)
	return totalData
}

func ConcurrentReadChunk(ctx context.Context, in *fsrpc.ReadChunkRequest, opts ...grpc.CallOption) (*fsrpc.ReadChunkReply, error) {
	var wg sync.WaitGroup

	// reply map
	// replyMap := make(map[string] []byte)
	replyMap := make(map[string]*fsrpc.ReadChunkReply)

	// Concurrent ReadChunk for each in map
	for name, client := range g.datanodeClientMap {
		wg.Add(1)

		// start a new goroutine
		go func() {
			// get the whole chunk data
			reply, err := client.ReadChunk(ctx, in, opts...)
			if err != nil || reply.Status != fsrpc.Status_OK {
				return
			}
			replyMap[name] = reply

			defer wg.Done()
		}()
	}
	// wait for all tasks finish
	wg.Wait()

	for _, data := range replyMap {
		return data, nil
	}
	return nil, errors.New("no data")
}

func ConcurrentWriteChunk(ctx context.Context, in *fsrpc.WriteChunkRequest, opts ...grpc.CallOption) (*fsrpc.WriteChunkReply, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	// reply map
	replyMap := make(map[string]*fsrpc.WriteChunkReply)

	// Concurrent ReadChunk for each in map
	for name, client := range g.datanodeClientMap {
		// start a new goroutine
		go func() {
			// get the whole chunk data
			reply, err := client.WriteChunk(ctx, in, opts...)
			if err != nil {
				return
			}
			replyMap[name] = reply

			defer wg.Done()
		}()
	}
	// wait for all tasks finish
	wg.Wait()

	for _, data := range replyMap {
		return data, nil
	}
	return nil, errors.New("no data")
}

func CheckNewDataNode(reply *fsrpc.ReadSheetReply) {
	chunks := reply.Chunks

	for _, chunk := range chunks {
		if _, ok := g.datanodeClientMap[chunk.Datanode]; !ok {
			conn, err := grpc.Dial(chunk.Datanode, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			g.datanodeClientMap[chunk.Datanode] = fsrpc.NewDataNodeClient(conn)
		}
	}
}
