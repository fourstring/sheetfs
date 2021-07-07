package client

import (
	"context"
	"fmt"
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
Request Master to get a list of all Chunks of a file. For each chunk, start a worker
goroutine to fetch them, and reassemble them into a complete file.

Sometimes(e.g. due to network delay), client will receive a newer Version from master
which is not actually written to DataNode by another client. If so, this function will
spin until it success or cancelled by ctx, so ctx is generally necessary.
@para
	b([]byte): return the read data. It will return partially read data if
	there are some workers failed.
@return
	n(int64): the read size, -1 if error
	error(error)
		*UnexpectedStatusError: MasterNode or DataNode returns a unexpected status
		some other errors returned by rpc
		*CancelledError: If operations(spin or rpc call) are cancelled by ctx. And only if there is no
		other errors happened and ctx cancelled, a CancelledError will be returned.
*/
func (f *File) Read(ctx context.Context) (b []byte, n int64, err error) {
	// read whole sheet
	masterReq := fsrpc.ReadSheetRequest{Fd: f.fd}
	masterReply, err := f.client.masterClient.ReadSheet(ctx, &masterReq)

	// check read reply
	if err != nil {
		return []byte{}, -1, err
	}

	f.client.checkNewDataNode(masterReply.Chunks)
	switch masterReply.Status {
	case fsrpc.Status_OK:
	case fsrpc.Status_NotFound:
		return []byte{}, -1, fs.ErrNotExist
	default:
		return []byte{}, -1, NewUnexpectedStatusError(masterReply.Status)
	}

	// read every chunk of the file
	var wg sync.WaitGroup
	data := make([]byte, 0)
	metaData := make([]byte, 0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workerErr error
	var workerMu sync.Mutex

	for _, chunk := range masterReply.Chunks {
		wg.Add(1)

		// start a new goroutine
		chunk := chunk
		go func() {
			defer wg.Done()
			// get the whole chunk data
			for {
				select {
				case <-ctx.Done():
					workerMu.Lock()
					if workerErr == nil {
						workerErr = &CancelledError{}
					}
					workerMu.Unlock()
					return
				default:
				}
				dataReply, err := f.client.concurrentReadChunk(ctx, chunk, &fsrpc.ReadChunkRequest{
					Id:      chunk.Id,
					Offset:  0,
					Size:    config.FILE_SIZE,
					Version: chunk.Version,
				})
				if err != nil {
					workerMu.Lock()
					workerErr = err
					fmt.Printf("Cancel operation due to rpc err:%s\n", err)
					cancel()
					workerMu.Unlock()
					return
				}
				switch dataReply.Status {
				case fsrpc.Status_OK:
					// Do nothing
				case fsrpc.Status_WrongVersion:
					continue
				case fsrpc.Status_NotFound:
					// a new file may with empty metadata
					if chunk.HoldsMeta {
						return
					}
					fallthrough
				default:
					workerMu.Lock()
					fmt.Printf("Cancel operation due to operation status:%s\n", dataReply.Status)
					workerErr = NewUnexpectedStatusError(dataReply.Status)
					cancel()
					workerMu.Unlock()
					return
				}

				if chunk.HoldsMeta {
					copy(metaData, dataReply.Data)
				} else {
					workerMu.Lock()
					data = append(data, dataReply.Data...)
					data = append(data, ","...)
					workerMu.Unlock()
				}
				return
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

	return res, int64(len(res)), workerErr
}

/*
ReadAt
Sometimes(e.g. due to network delay), client will receive a newer Version from master
which is not actually written to DataNode by another client. If so, this function will
spin until it success or cancelled by ctx, so ctx is generally necessary.

@para
	name(string):  the name of the file
@return
	fd(uint64): the fd of the open file
	status(Status)
	error(error)
		*UnexpectedStatusError: MasterNode or DataNode returns a unexpected status
		some other errors returned by rpc
		*CancelledError: If operations(spin or rpc call) are cancelled by ctx. And only if there is no
		other errors happened and ctx cancelled, a CancelledError will be returned.
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
		return -1, NewUnexpectedStatusError(masterReply.Status)
	}

	f.client.checkNewDataNode([]*fsrpc.Chunk{masterReply.Cell.Chunk})

	// use metadata to read chunk
	dataReq := fsrpc.ReadChunkRequest{
		Id:      masterReply.Cell.Chunk.Id,
		Offset:  masterReply.Cell.Offset,
		Size:    masterReply.Cell.Size,
		Version: masterReply.Cell.Chunk.Version,
	}
	for {
		select {
		case <-ctx.Done():
			return -1, &CancelledError{}
		default:
		}
		dataReply, err := f.client.concurrentReadChunk(ctx, masterReply.Cell.Chunk, &dataReq)

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
			return -1, fs.ErrNotExist
		case fsrpc.Status_WrongVersion:
			// spin until cancelled
			continue
		default:
			return -1, NewUnexpectedStatusError(dataReply.Status)
		}
	}
}

/*
WriteAt
Sometimes(e.g. due to network delay), client will receive a newer Version from master
which is not actually written to DataNode by another client. If so, this function will
spin until it success or cancelled by ctx, so ctx is generally necessary.

@para
	name(string):  the name of the file
@return
	fd(uint64): the fd of the open file
	status(Status)
	error(error)
		*UnexpectedStatusError: MasterNode or DataNode returns a unexpected status
		some other errors returned by rpc
		*CancelledError: If operations(spin or rpc call) are cancelled by ctx. And only if there is no
		other errors happened and ctx cancelled, a CancelledError will be returned.
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
	default:
		return -1, NewUnexpectedStatusError(masterReply.Status)
	}

	f.client.checkNewDataNode([]*fsrpc.Chunk{masterReply.Cell.Chunk})
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
	for {
		select {
		case <-ctx.Done():
			return -1, &CancelledError{}
		default:
		}
		dataReply, err := f.client.concurrentWriteChunk(ctx, masterReply.Cell.Chunk, &dataReq)

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
			return -1, fs.ErrNotExist
		case fsrpc.Status_WrongVersion:
			// spin until cancelled or success
			continue
		default:
			return -1, NewUnexpectedStatusError(dataReply.Status)
		}
	}
}
