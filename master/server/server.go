package server

import (
	context "context"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"sheetfs/master/datanode_alloc"
	"sheetfs/master/errors"
	"sheetfs/master/filemgr"
	"sheetfs/master/sheetfile"
	fs_rpc "sheetfs/protocol"
)

type server struct {
	fs_rpc.UnimplementedMasterNodeServer
	fileMgr *filemgr.FileManager
	alloc   *datanode_alloc.DataNodeAllocator
	logger  *zap.Logger
}

/*
NewServer
AutoMigrate sqlite table structure for MapEntry and Chunk (Tables for Cell will
be created during file creation). Then initialize a zap Logger and load FileManager
from database to create a server.

@para
	db: a gorm connection. It can't be a transaction.

@return
	*server: initialized server
	error:
		errors during auto migration.
*/
func NewServer(db *gorm.DB, alloc *datanode_alloc.DataNodeAllocator) (*server, error) {
	err := db.AutoMigrate(&filemgr.MapEntry{}, &sheetfile.Chunk{})
	if err != nil {
		return nil, err
	}
	logger, _ := zap.NewProduction()
	s := &server{fileMgr: filemgr.LoadFileManager(db, alloc), logger: logger, alloc: alloc}
	return s, nil
}

/*
defaultErrorHandler
Handle errors returned from FileManager uniformly. Setting status to be returned
to RPC client according to the kind of err, and logging the err through zap.

@para
	err: errors raised from FileManager's methods
	status: pointer to the status variable to be set
*/
func (s *server) defaultErrorHandler(err error, status *fs_rpc.Status) {
	defer s.logger.Sync()
	switch err.(type) {
	case *errors.FileExistsError:
		*status = fs_rpc.Status_Exist
	case *errors.CellNotFoundError:
		*status = fs_rpc.Status_Invalid
	case *errors.FileNotFoundError:
		*status = fs_rpc.Status_NotFound
	case *errors.FdNotFoundError:
		*status = fs_rpc.Status_NotFound
	case *errors.NoDataNodeError:
		*status = fs_rpc.Status_Unavailable
	default:
		*status = fs_rpc.Status_Unavailable
	}
	s.logger.Error("MasterNode:", zap.Error(err))
}

func (s *server) RegisterDataNode(ctx context.Context, request *fs_rpc.RegisterDataNodeRequest) (*fs_rpc.RegisterDataNodeReply, error) {
	s.alloc.AddDataNode(request.Addr)
	return &fs_rpc.RegisterDataNodeReply{Status: fs_rpc.Status_OK}, nil
}

func (s *server) ReadSheet(ctx context.Context, request *fs_rpc.ReadSheetRequest) (*fs_rpc.ReadSheetReply, error) {
	status := fs_rpc.Status_OK
	chunks, err := s.fileMgr.ReadSheet(request.Fd)

	if err != nil {
		s.defaultErrorHandler(err, &status)
		return &fs_rpc.ReadSheetReply{
			Status: status,
		}, nil
	}

	pbChunks := make([]*fs_rpc.Chunk, len(chunks))
	for i, c := range chunks {
		pbChunks[i] = &fs_rpc.Chunk{
			Id:        c.ID,
			Datanode:  c.DataNode,
			Version:   c.Version,
			HoldsMeta: len(c.Cells) == 1 && c.Cells[0].IsMeta(),
		}
	}
	reply := &fs_rpc.ReadSheetReply{
		Status: status,
		Chunks: pbChunks,
	}
	return reply, nil
}

func (s *server) CreateSheet(ctx context.Context, request *fs_rpc.CreateSheetRequest) (*fs_rpc.CreateSheetReply, error) {
	status := fs_rpc.Status_OK
	fd, err := s.fileMgr.CreateSheet(request.Filename)
	if err != nil {
		s.defaultErrorHandler(err, &status)
		return &fs_rpc.CreateSheetReply{
			Status: status,
		}, nil
	}

	return &fs_rpc.CreateSheetReply{
		Status: status,
		Fd:     fd,
	}, nil
}

func (s *server) DeleteSheet(ctx context.Context, request *fs_rpc.DeleteSheetRequest) (*fs_rpc.DeleteSheetReply, error) {
	panic("implement me")
}

func (s *server) OpenSheet(ctx context.Context, request *fs_rpc.OpenSheetRequest) (*fs_rpc.OpenSheetReply, error) {
	status := fs_rpc.Status_OK
	fd, err := s.fileMgr.OpenSheet(request.Filename)
	if err != nil {
		s.defaultErrorHandler(err, &status)
		return &fs_rpc.OpenSheetReply{
			Status: status,
		}, nil
	}

	return &fs_rpc.OpenSheetReply{
		Status: status,
		Fd:     fd,
	}, nil
}

func (s *server) RecycleSheet(ctx context.Context, request *fs_rpc.RecycleSheetRequest) (*fs_rpc.RecycleSheetReply, error) {
	status := fs_rpc.Status_OK
	s.fileMgr.RecycleSheet(request.Filename)
	return &fs_rpc.RecycleSheetReply{
		Status: status,
	}, nil
}

func (s *server) ResumeSheet(ctx context.Context, request *fs_rpc.ResumeSheetRequest) (*fs_rpc.ResumeSheetReply, error) {
	status := fs_rpc.Status_OK
	s.fileMgr.ResumeSheet(request.Filename)
	return &fs_rpc.ResumeSheetReply{
		Status: status,
	}, nil
}

func (s *server) ListSheets(ctx context.Context, empty *fs_rpc.Empty) (*fs_rpc.ListSheetsReply, error) {
	status := fs_rpc.Status_OK
	sheets := s.fileMgr.GetAllSheets()
	return &fs_rpc.ListSheetsReply{
		Status: status,
		Sheets: sheets,
	}, nil
}

func (s *server) ReadCell(ctx context.Context, request *fs_rpc.ReadCellRequest) (*fs_rpc.ReadCellReply, error) {
	status := fs_rpc.Status_OK
	cell, dataChunk, err := s.fileMgr.ReadFileCell(request.Fd, request.Row, request.Column)
	if err != nil {
		s.defaultErrorHandler(err, &status)
		return &fs_rpc.ReadCellReply{
			Status: status,
		}, nil
	}
	return &fs_rpc.ReadCellReply{
		Status: status,
		Cell: &fs_rpc.Cell{
			Chunk: &fs_rpc.Chunk{
				Id:        dataChunk.ID,
				Datanode:  dataChunk.DataNode,
				Version:   dataChunk.Version,
				HoldsMeta: len(dataChunk.Cells) == 1 && dataChunk.Cells[0].IsMeta(),
			},
			Offset: cell.Offset,
			Size:   cell.Size,
		},
	}, nil
}

func (s *server) WriteCell(ctx context.Context, request *fs_rpc.WriteCellRequest) (*fs_rpc.WriteCellReply, error) {
	status := fs_rpc.Status_OK
	cell, dataChunk, err := s.fileMgr.WriteFileCell(request.Fd, request.Row, request.Column)
	if err != nil {
		s.defaultErrorHandler(err, &status)
		return &fs_rpc.WriteCellReply{
			Status: status,
		}, nil
	}
	return &fs_rpc.WriteCellReply{
		Status: status,
		Cell: &fs_rpc.Cell{
			Chunk: &fs_rpc.Chunk{
				Id:       dataChunk.ID,
				Datanode: dataChunk.DataNode,
				Version:  dataChunk.Version,
			},
			Offset: cell.Offset,
			Size:   cell.Size,
		},
	}, nil
}
