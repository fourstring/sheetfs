package fsclient

import (
	"context"
	"github.com/fourstring/sheetfs/config"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"github.com/go-zookeeper/zk"
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
		// ask ZK to get correct master
		for i := 0; i < config.ACK_MOST_TIMES; i++ {
			err := c.reAskMasterNode()
			if err != nil {
				continue
			}
			reply, err = c.masterClient.CreateSheet(ctx, &req)
			if err != nil {
				continue
			}
			break // now get the correct reply
		}
	}
	// still have errors
	if reply == nil || err != nil {
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
		// ask ZK to get correct master
		for i := 0; i < config.ACK_MOST_TIMES; i++ {
			err := c.reAskMasterNode()
			if err != nil {
				continue
			}
			reply, err = c.masterClient.DeleteSheet(ctx, &req)
			if err != nil {
				continue
			}
			break // now get the correct reply
		}
	}
	// still have errors
	if reply == nil || err != nil {
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
		// ask ZK to get correct master
		for i := 0; i < config.ACK_MOST_TIMES; i++ {
			err := c.reAskMasterNode()
			if err != nil {
				continue
			}
			reply, err = c.masterClient.OpenSheet(ctx, &req)
			if err != nil {
				continue
			}
			break // now get the correct reply
		}
	}

	// still have errors
	if reply == nil || err != nil {
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

	reply, err := dc.ReadChunk(ctx, in, opts...)

	// current datanode address can not serve
	if err != nil {
		goto ask
	}

	return reply, nil

ask:
	// ask the right address, at most ACK_MOST_TIMES
	for i := 0; i < config.ACK_MOST_TIMES; i++ {
		client, err := c.reAskDataNode(chunk.Datanode)
		if err != nil {
			continue
		}
		reply, err = client.ReadChunk(ctx, in, opts...)
		if err != nil {
			continue
		}
		// now get the correct reply
		return reply, nil
	}

	return nil, err
}

func (c *Client) concurrentWriteChunk(ctx context.Context, chunk *fsrpc.Chunk, in *fsrpc.WriteChunkRequest, opts ...grpc.CallOption) (*fsrpc.WriteChunkReply, error) {
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

	reply, err := dc.WriteChunk(ctx, in, opts...)

	// current datanode address can not serve
	if err != nil {
		goto ask
	}

	return reply, nil

ask:
	// ask the right address, at most ACK_MOST_TIMES
	for i := 0; i < config.ACK_MOST_TIMES; i++ {
		client, err := c.reAskDataNode(chunk.Datanode)
		if err != nil {
			continue
		}
		reply, err = client.WriteChunk(ctx, in, opts...)
		if err != nil {
			continue
		}
		// now get the correct reply
		return reply, nil
	}

	return nil, err
}

func (c *Client) reAskMasterNode() error {
	connZK, _, err := zk.Connect(config.ElectionServer, config.AckTimeout)
	if err != nil { // retry
		return err
	}
	masterAddr, _, err := connZK.Get(config.MasterAck)
	if err != nil { // retry
		return err
	}
	conn, err := grpc.Dial(string(masterAddr), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	c.masterClient = fsrpc.NewMasterNodeClient(conn)
	return nil
}

func (c *Client) reAskDataNode(datanode string) (fsrpc.DataNodeClient, error) {
	connZK, _, err := zk.Connect(config.ElectionServer, config.AckTimeout)
	if err != nil { // retry
		return nil, err
	}
	datanodeAddr, _, err := connZK.Get(config.DataNodeAckPrefix + datanode)
	if err != nil { // retry
		return nil, err
	}
	// connect
	conn, err := grpc.Dial(string(datanodeAddr), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil { // retry
		return nil, err
	}
	client := fsrpc.NewDataNodeClient(conn)
	c.datanodeClientMap[string(datanodeAddr)] = client

	return client, nil
}
