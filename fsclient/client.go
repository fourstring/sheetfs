package fsclient

import (
	"context"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"google.golang.org/grpc"
	"io/fs"
	"log"
	"strings"
	"sync"
)

type Client struct {
	mu                sync.RWMutex
	masterClient      fsrpc.MasterNodeClient
	datanodeClientMap map[string]fsrpc.DataNodeClient
}

func NewClient(masterAddr string) (*Client, error) {
	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	return &Client{
		masterClient:      fsrpc.NewMasterNodeClient(conn),
		datanodeClientMap: map[string]fsrpc.DataNodeClient{},
	}, nil
}

/*
Create
@para
	name(string):  the name of the file
@return
	f(*File): fd
	error(error): nil is no error
				fs.ErrExist: already exist
				fs.ErrInvalid: wrong para
*/
func (c *Client) Create(ctx context.Context, name string) (f *File, err error) {
	// check filename
	if name == "" || strings.Contains(name, "/") ||
		strings.Contains(name, "\\") {
		return nil, fs.ErrInvalid
	}
	// create the file
	req := fsrpc.CreateSheetRequest{Filename: name}
	reply, err := c.masterClient.CreateSheet(ctx, &req)

	if err != nil {
		return nil, err
	}

	switch reply.Status {
	case fsrpc.Status_OK:
		return newFile(reply.Fd, name, c), nil
	case fsrpc.Status_Exist:
		return nil, fs.ErrExist
	default:
		panic("OpenSheet RPC return illegal Status")
	}
}

/*
Delete
@para
	name(string) : the name of the file
@return
	error(error): nil is no error
				fs.ErrExist: already exist
				fs.ErrInvalid: wrong para
*/
func (c *Client) Delete(ctx context.Context, name string) (err error) {
	// DeleteSheet
	req := fsrpc.DeleteSheetRequest{Filename: name}
	reply, err := c.masterClient.DeleteSheet(ctx, &req)

	if err != nil {
		return err
	}
	switch reply.Status {
	case fsrpc.Status_OK:
		return nil
	case fsrpc.Status_NotFound:
		return fs.ErrNotExist
	default:
		panic("OpenSheet RPC return illegal Status")
	}
}

/*
Open
@para
	name(string):  the name of the file
@return
	fd(uint64): the fd of the open file
	status(Status)
	error(error)
*/
func (c *Client) Open(ctx context.Context, name string) (f *File, err error) {
	// check filename
	if name == "" || strings.Contains(name, "/") ||
		strings.Contains(name, "\\") {
		return nil, fs.ErrInvalid
	}
	// open the required file
	req := fsrpc.OpenSheetRequest{Filename: name}
	reply, err := c.masterClient.OpenSheet(ctx, &req)

	if err != nil {
		return nil, err
	}

	switch reply.Status {
	case fsrpc.Status_OK: // open correctly
		return newFile(reply.Fd, name, c), err
	case fsrpc.Status_NotFound: // not found
		return nil, fs.ErrNotExist
	default: // should never reach here
		panic("OpenSheet RPC return illegal Status")
	}
}

func (c *Client) checkNewDataNode(chunks []*fsrpc.Chunk) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, chunk := range chunks {
		// register new client node
		address := chunk.Datanode
		if _, ok := c.datanodeClientMap[address]; !ok {
			conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			client := fsrpc.NewDataNodeClient(conn)
			c.datanodeClientMap[address] = client
		}
	}
}

func (c *Client) concurrentReadChunk(ctx context.Context, chunk *fsrpc.Chunk, in *fsrpc.ReadChunkRequest, opts ...grpc.CallOption) (*fsrpc.ReadChunkReply, error) {
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
	var dc fsrpc.DataNodeClient
	c.mu.RLock()
	for addr, client := range c.datanodeClientMap {
		if addr == chunk.Datanode {
			// When quorum is introduced, a collection of DataNodeClient should be used here.
			dc = client
			break
		}
	}
	c.mu.RUnlock()
	return dc.ReadChunk(ctx, in, opts...)
}

func (c *Client) concurrentWriteChunk(ctx context.Context, chunk *fsrpc.Chunk, in *fsrpc.WriteChunkRequest, opts ...grpc.CallOption) (*fsrpc.WriteChunkReply, error) {
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
	var dc fsrpc.DataNodeClient
	c.mu.RLock()
	for addr, client := range c.datanodeClientMap {
		if addr == chunk.Datanode {
			// When quorum is introduced, a collection of DataNodeClient should be used here.
			dc = client
			break
		}
	}
	c.mu.RUnlock()
	return dc.WriteChunk(ctx, in, opts...)
}
