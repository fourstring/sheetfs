package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sheetfs/datanode/utils"
	fsrpc "sheetfs/protocol"
	"strconv"
)

type server struct {
	fsrpc.UnimplementedDataNodeServer
	dataPath string
}

func NewServer(path string) *server {
	return &server{
		dataPath: path,
	}
}

func (s *server) DeleteChunk(ctx context.Context, request *fsrpc.DeleteChunkRequest) (*fsrpc.DeleteChunkReply, error) {
	reply := new(fsrpc.DeleteChunkReply)
	reply.Status = fsrpc.Status_OK
	err := os.Remove(s.getFilename(request.Id))
	if err != nil {
		print("not delete")
	}
	return reply, nil
}

func (s *server) ReadChunk(ctx context.Context, request *fsrpc.ReadChunkRequest) (*fsrpc.ReadChunkReply, error) {
	reply := new(fsrpc.ReadChunkReply)

	file, err := os.Open(s.getFilename(request.Id))

	// this file does not exist
	if err != nil {
		file.Close()
		reply.Status = fsrpc.Status_NotFound
		return reply, nil
	}

	// check version
	curVersion := utils.GetVersion(file)
	if curVersion <= request.Version {
		// the version is correct
		data := make([]byte, request.Size)
		_, err = file.ReadAt(data, int64(request.Offset))
		file.Close()

		// can not read data at this pos
		if err != nil {
			reply.Status = fsrpc.Status_NotFound
			return reply, nil
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

	file, err := os.OpenFile(s.getFilename(request.Id), os.O_RDWR, 0755)

	// first time
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// MasterNode assigns version 1 to those chunks which don't exist before.
			if request.Version != 1 {
				reply.Status = fsrpc.Status_WrongVersion
				return reply, nil
			}
			// create the file
			file, err := os.Create(s.getFilename(request.Id))
			if err != nil {
				reply.Status = fsrpc.Status_Unavailable
				fmt.Println(err)
				return reply, nil
			}
			// write the data
			_, err = file.WriteAt(utils.GetPaddedFile(request.Data, request.Size,
				request.Padding, request.Offset), int64(request.Offset))
			if err != nil {
				reply.Status = fsrpc.Status_Unavailable
				return reply, nil
			}

			// update the version
			utils.SyncAndUpdateVersion(file, request.Version)
			reply.Status = fsrpc.Status_OK
			return reply, nil
		}
		reply.Status = fsrpc.Status_Unavailable
		return reply, nil
	}

	curVersion := utils.GetVersion(file)
	// print("current version: ", curVersion, ", request version: ", request.Version)
	if curVersion+1 == request.Version {
		// can update
		// write the data
		_, err := file.WriteAt(utils.GetPaddedData(request.Data, request.Size, request.Padding),
			int64(request.Offset))
		if err != nil {
			file.Close()
			reply.Status = fsrpc.Status_NotFound
			return reply, nil
		}

		// update the version
		utils.SyncAndUpdateVersion(file, request.Version)

		reply.Status = fsrpc.Status_OK
		return reply, nil
	} else {
		// some backup write
		file.Close()
		reply.Status = fsrpc.Status_WrongVersion
		return reply, nil
	}
}

func (s *server) getFilename(id uint64) string {
	return path.Join(s.dataPath, "chunk_"+strconv.FormatUint(id, 10))
}
