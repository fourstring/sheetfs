package main

import (
	"context"
	"fmt"
	"os"
	fsrpc "sheetfs/protocol"
)

type server struct {
	fsrpc.UnimplementedDataNodeServer
}

func (s *server) DeleteChunk(ctx context.Context, request *fsrpc.DeleteChunkRequest) (*fsrpc.DeleteChunkReply, error) {
	reply := new(fsrpc.DeleteChunkReply)
	reply.Status = fsrpc.Status_OK
	err := os.Remove(getFilename(request.Id))
	if err != nil {
		print("not delete")
	}
	return reply, nil
}

func (s *server) ReadChunk(ctx context.Context, request *fsrpc.ReadChunkRequest) (*fsrpc.ReadChunkReply, error) {
	reply := new(fsrpc.ReadChunkReply)

	file, err := os.Open(getFilename(request.Id))

	// this file does not exist
	if err != nil {
		file.Close()
		reply.Status = fsrpc.Status_NotFound
		return reply, err
	}

	// check version
	curVersion := getVersion(file)
	if curVersion < request.Version {
		// the version is correct
		data := make([]byte, request.Size)
		_, err = file.ReadAt(data, int64(request.Offset))
		file.Close()

		// can not read data at this pos
		if err != nil {
			reply.Status = fsrpc.Status_NotFound
			return reply, err
		}
		// read the correct data
		reply.Data = data
		reply.Status = fsrpc.Status_OK
	} else {
		file.Close()
		reply.Status = fsrpc.Status_WrongVersion
		return reply, nil
	}

	return reply, nil
}

func (s *server) WriteChunk(ctx context.Context, request *fsrpc.WriteChunkRequest) (*fsrpc.WriteChunkReply, error) {
	reply := new(fsrpc.WriteChunkReply)

	file, err := os.OpenFile(getFilename(request.Id), os.O_RDWR, 0755)

	// first time
	if err != nil {
		// create the file
		file, err := os.Create(getFilename(request.Id))
		if err != nil {
			fmt.Println(err)
			return nil, nil
		}
		// write the data
		_, err = file.WriteAt(getPaddedFile(request.Data, request.Size,
			request.Padding, request.Offset), int64(request.Offset))
		if err != nil {
			return nil, err
		}

		// update the version
		SyncAndUpdateVersion(file, request.Version)
		reply.Status = fsrpc.Status_OK
		return reply, nil
	}

	curVersion := getVersion(file)
	if curVersion+1 == request.Version {
		// can update
		// write the data
		_, err := file.WriteAt(getPaddedData(request.Data, request.Size, request.Padding),
			int64(request.Offset))
		if err != nil {
			file.Close()
			reply.Status = fsrpc.Status_NotFound
			return reply, nil
		}

		// update the version
		SyncAndUpdateVersion(file, request.Version)

		reply.Status = fsrpc.Status_OK
		return reply, nil
	} else {
		// some backup write
		file.Close()
		reply.Status = fsrpc.Status_WrongVersion
		return reply, nil
	}
}
