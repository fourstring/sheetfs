package client

import (
	"context"
	"google.golang.org/grpc"
	"io/fs"
	"log"
	"sheetfs/config"
	fsrpc "sheetfs/protocol"
	"strings"
	"sync"
)

var g *Global

type Global struct {
	masterClient      fsrpc.MasterNodeClient
	datanodeClientMap map[string]fsrpc.DataNodeClient
	ctx               context.Context
}

func init() {
	// Set up a connection to the server.
	/*conn, err := grpc.Dial(*address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	master := fsrpc.NewMasterNodeClient(conn)

	if g == nil {
		g = &Global{
			masterClient: master,
			ctx:          context.Background(),
		}
	}*/
}

func Init(masterAddr string) {
	if g == nil {
		conn, err := grpc.Dial(masterAddr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		master := fsrpc.NewMasterNodeClient(conn)
		g = &Global{
			masterClient:      master,
			datanodeClientMap: make(map[string]fsrpc.DataNodeClient),
			ctx:               context.Background(),
		}
	}
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
func Create(name string) (f *File, err error) {
	// check filename
	if name == "" || strings.Contains(name, "/") ||
		strings.Contains(name, "\\") {
		return nil, fs.ErrInvalid
	}
	// create the file
	req := fsrpc.CreateSheetRequest{Filename: name}
	reply, err := g.masterClient.CreateSheet(g.ctx, &req)

	if err != nil {
		return nil, err
	}

	switch reply.Status {
	case fsrpc.Status_OK:
		return &File{Fd: reply.Fd}, nil
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
func Delete(name string) (err error) {
	// DeleteSheet
	req := fsrpc.DeleteSheetRequest{Filename: name}
	reply, err := g.masterClient.DeleteSheet(g.ctx, &req)

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
func Open(name string) (f *File, err error) {
	// check filename
	if name == "" || strings.Contains(name, "/") ||
		strings.Contains(name, "\\") {
		return nil, fs.ErrInvalid
	}
	// open the required file
	req := fsrpc.OpenSheetRequest{Filename: name}
	reply, err := g.masterClient.OpenSheet(g.ctx, &req)

	if err != nil {
		return nil, err
	}

	switch reply.Status {
	case fsrpc.Status_OK: // open correctly
		return &File{Fd: reply.Fd}, err
	case fsrpc.Status_NotFound: // not found
		return nil, fs.ErrNotExist
	default: // should never reach here
		panic("OpenSheet RPC return illegal Status")
	}
}

/*
Read
@para
	b([]byte): return the read data
@return
	n(int64): the read size, -1 if error
	error(error)
*/
func (f *File) Read(b []byte) (n int64, err error) {
	// read whole sheet
	masterReq := fsrpc.ReadSheetRequest{Fd: f.Fd}
	masterReply, err := g.masterClient.ReadSheet(g.ctx, &masterReq)

	// check read reply
	if err != nil {
		return -1, err
	}
	CheckNewDataNode(masterReply)

	if masterReply.Status != fsrpc.Status_OK {
		// have fd so not found must due to some invalid para
		return -1, fs.ErrInvalid
	}

	// read every chunk of the file
	var wg sync.WaitGroup
	data := make([]byte, 0)
	metaData := make([]byte, 0)

	for _, chunk := range masterReply.Chunks {
		wg.Add(1)

		// start a new goroutine
		chunk := chunk
		go func() {
			// get the whole chunk data
			dataReply, err := ConcurrentReadChunk(g.ctx, &fsrpc.ReadChunkRequest{
				Id:      chunk.Id,
				Offset:  0,
				Size:    config.FILE_SIZE,
				Version: chunk.Version,
			})

			if err != nil || dataReply.Status != fsrpc.Status_OK {
				print("data chunk mismatch master chunk")
				defer wg.Done()
				return
			}
			if chunk.HoldsMeta {
				copy(metaData, dataReply.Data)
			} else {
				data = append(data, dataReply.Data...)
				data = append(data, ","...)
			}

			defer wg.Done()
		}()
	}
	// wait for all tasks finish
	wg.Wait()

	// convert to JSON
	// DynamicCopy(&b, connect(data, metaData))
	//src := connect(data, metaData)
	//for i := 0; i < len(b) && i < len(src); i++ {
	//	b[i] = src[i]
	//}
	//b = append(b, src[len(b):]...)
	copy(b, connect(data, metaData))

	return int64(len(b)), nil
}

/*
ReadAt
@para
	name(string):  the name of the file
@return
	fd(uint64): the fd of the open file
	status(Status)
	error(error)
*/
func (f *File) ReadAt(b []byte, col uint32, row uint32) (n int64, err error) {
	// read cell to get metadata
	masterReq := fsrpc.ReadCellRequest{
		Fd:     f.Fd,
		Column: col,
		Row:    row,
	}
	masterReply, err := g.masterClient.ReadCell(g.ctx, &masterReq)

	if err != nil {
		return -1, err
	}
	if masterReply.Status != fsrpc.Status_OK {
		// have fd so not found must due to some invalid para
		return -1, fs.ErrInvalid
	}

	// use metadata to read chunk
	dataReq := fsrpc.ReadChunkRequest{
		Id:      masterReply.Cell.Chunk.Id,
		Offset:  masterReply.Cell.Offset,
		Size:    masterReply.Cell.Size,
		Version: masterReply.Cell.Chunk.Version,
	}
	dataReply, err := ConcurrentReadChunk(g.ctx, &dataReq)

	if err != nil {
		return -1, err
	}
	switch dataReply.Status {
	case fsrpc.Status_OK:
		// open correctly
		// DynamicCopy(&b, dataReply.Data)
		copy(b, dataReply.Data)
		// b = append(b, dataReply.Data[len(b):]...)
		return int64(masterReply.Cell.Size), nil
	case fsrpc.Status_NotFound:
		// not found
		return -1, fs.ErrInvalid
	default:
		// should never reach here
		panic("OpenSheet RPC return illegal Status")
	}
}

/*
WriteAt
@para
	name(string):  the name of the file
@return
	fd(uint64): the fd of the open file
	status(Status)
	error(error)
*/
func (f *File) WriteAt(b []byte, col uint32, row uint32, padding string) (n int64, err error) {
	// read cell to get metadata
	masterReq := fsrpc.WriteCellRequest{
		Fd:     f.Fd,
		Column: col,
		Row:    row,
	}
	masterReply, err := g.masterClient.WriteCell(g.ctx, &masterReq)

	if err != nil {
		return -1, err
	}

	// get the correct version
	var version uint64
	switch masterReply.Status {
	case fsrpc.Status_OK:
		// open correctly
		version = masterReply.Cell.Chunk.Version
	case fsrpc.Status_NotFound:
		// not found
		version = 0
	default:
		// should never reach here
		panic("WriteCell RPC return illegal Status")
	}

	// if padding is empty
	if len(padding) == 0 {
		padding = " "
	}

	// use metadata to read chunk
	dataReq := fsrpc.WriteChunkRequest{
		Id:      masterReply.Cell.Chunk.Id,
		Offset:  masterReply.Cell.Offset,
		Size:    uint64(len(b)),
		Version: version,
		Padding: padding,
		Data:    b,
	}
	dataReply, err := ConcurrentWriteChunk(g.ctx, &dataReq)

	if err != nil {
		return -1, err
	}
	switch dataReply.Status {
	case fsrpc.Status_OK:
		// open correctly
		return int64(len(b)), nil
	case fsrpc.Status_NotFound:
		// not found
		return -1, fs.ErrInvalid
	default:
		// should never reach here
		panic("OpenSheet RPC return illegal Status")
	}
}
