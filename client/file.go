package client

import (
	"context"
	"io/fs"
	"sheetfs/config"
	fsrpc "sheetfs/protocol"
	"sync"
)

type File struct {
	fd     uint64
	client *Client
}

func newFile(fd uint64, client *Client) *File {
	return &File{fd: fd, client: client}
}

/*
Read
@para
	b([]byte): return the read data
@return
	n(int64): the read size, -1 if error
	error(error)
*/
func (f *File) Read(ctx context.Context) (b []byte, n int64, err error) {
	// read whole sheet
	masterReq := fsrpc.ReadSheetRequest{Fd: f.fd}
	masterReply, err := f.client.masterClient.ReadSheet(ctx, &masterReq)

	// check read reply
	if err != nil {
		return []byte{}, -1, err
	}

	f.client.checkNewDataNode(masterReply)

	if masterReply.Status != fsrpc.Status_OK {
		// have fd so not found must due to some invalid para
		return []byte{}, -1, fs.ErrInvalid
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
			defer wg.Done()
			// get the whole chunk data
			dataReply, err := f.client.concurrentReadChunk(ctx, &fsrpc.ReadChunkRequest{
				Id:      chunk.Id,
				Offset:  0,
				Size:    config.FILE_SIZE,
				Version: chunk.Version,
			})
			if err != nil || dataReply.Status != fsrpc.Status_OK {
				print("data chunk mismatch master chunk")
				return
			}
			if chunk.HoldsMeta {
				copy(metaData, dataReply.Data)
			} else {
				data = append(data, dataReply.Data...)
				data = append(data, ","...)
			}
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
	res := connect(data, metaData)

	return res, int64(len(res)), nil
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
func (f *File) ReadAt(ctx context.Context, b []byte, col uint32, row uint32) (n int64, err error) {
	// read cell to get metadata
	masterReq := fsrpc.ReadCellRequest{
		Fd:     f.fd,
		Column: col,
		Row:    row,
	}
	masterReply, err := f.client.masterClient.ReadCell(ctx, &masterReq)

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
	dataReply, err := f.client.concurrentReadChunk(ctx, &dataReq)

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
func (f *File) WriteAt(ctx context.Context, b []byte, col uint32, row uint32, padding string) (n int64, err error) {
	// read cell to get metadata
	masterReq := fsrpc.WriteCellRequest{
		Fd:     f.fd,
		Column: col,
		Row:    row,
	}
	masterReply, err := f.client.masterClient.WriteCell(ctx, &masterReq)

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
	dataReply, err := f.client.concurrentWriteChunk(ctx, &dataReq)

	if err != nil {
		return -1, err
	}
	// fmt.Print(dataReply.String())
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
