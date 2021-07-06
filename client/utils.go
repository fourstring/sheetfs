package client

import (
	"context"
	"google.golang.org/grpc"
	"log"
	fsrpc "sheetfs/protocol"
)

func connect(data []byte, metadata []byte) []byte {
	totalData := []byte("{\"celldata\": [") //header
	if len(data) > 0 {                      // delete the final ","
		data = data[:len(data)-1]
	}
	totalData = append(totalData, data...) //data
	if len(metadata) > 0 {
		totalData = append(totalData, "],"...)     // add "]," to become "{celldata:[...], metadata: ,...}"
		totalData = append(totalData, metadata...) //metadata
	} else {
		totalData = append(totalData, "]"...) // add "]" to become "{celldata:[...]}"
	}
	totalData = append(totalData, "}"...)
	return totalData
}

func ConcurrentReadChunk(ctx context.Context, in *fsrpc.ReadChunkRequest, opts ...grpc.CallOption) (*fsrpc.ReadChunkReply, error) {
	/*var wg sync.WaitGroup

	// reply map
	// replyMap := make(map[string] []byte)
	replyMap := make(map[string]*fsrpc.ReadChunkReply)

	// Concurrent ReadChunk for each in map
	for name, client := range g.datanodeClientMap {
		wg.Add(1)

		// start a new goroutine
		client := client
		name := name
		go func() {
			// get the whole chunk data
			reply, err := client.ReadChunk(ctx, in, opts...)
			if err != nil || reply.Status != fsrpc.Status_OK {
				defer wg.Done()
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
	*/
	var c fsrpc.DataNodeClient
	for _, client := range g.datanodeClientMap {
		c = client
	}
	return c.ReadChunk(ctx, in, opts...)
}

func ConcurrentWriteChunk(ctx context.Context, in *fsrpc.WriteChunkRequest, opts ...grpc.CallOption) (*fsrpc.WriteChunkReply, error) {
	/*
		var wg sync.WaitGroup
		wg.Add(1)

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
	*/
	var c fsrpc.DataNodeClient
	for _, client := range g.datanodeClientMap {
		c = client
	}
	return c.WriteChunk(ctx, in, opts...)
}

func CheckNewDataNode(reply *fsrpc.ReadSheetReply) {
	chunks := reply.Chunks

	for _, chunk := range chunks {
		// register new client node
		address := chunk.Datanode
		if _, ok := g.datanodeClientMap[address]; !ok {
			conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			client := fsrpc.NewDataNodeClient(conn)
			g.datanodeClientMap[address] = client
		}
	}
}

func DynamicCopy(dst *[]byte, src []byte) {
	for i := 0; i < len(*dst); i++ {
		(*dst)[i] = src[i]
	}
	*dst = append(*dst, src[len(*dst):]...)
}
