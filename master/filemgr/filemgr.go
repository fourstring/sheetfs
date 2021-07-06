package filemgr

import (
	"gorm.io/gorm"
	"sheetfs/master/datanode_alloc"
	"sheetfs/master/errors"
	"sheetfs/master/sheetfile"
	fs_rpc "sheetfs/protocol"
	"sync"
	"time"
)

/*
MapEntry
Represents a 'directory entry' of FileManager. Every entry maps a FileName
to a CellsTableName which is the name of sqlite table storing Cells
of the mapped SheetFile.

Recycled is a flag indicates that whether the mapped file has been moved to
'recycle bin' or not. When a file is recycled, time of this operation is recorded
in the RecycledAt field. Recycled files will be permanently after a period of time,
before they are deleted, they can be resumed.
*/
type MapEntry struct {
	gorm.Model
	FileName       string `gorm:"index"`
	CellsTableName string
	Recycled       bool
	RecycledAt     time.Time
}

/*
FileManager
Represents a top-level directory of SheetFiles stored in the filesystem.
There is only one level of directory, mapping filename to SheetFile directly, or
raise errors for invalid filename.

FileManager exposes both filename-oriented and fd-oriented API at the same time.
The relationship between them is Unix-alike. In other words, applications need to
Open a file with its filename, then a fd will be returned. Subsequently operations
must be invoked by a fd. The management of fds is also Unix-alike: multiple fds can
be pointed to the same underlying SheetFile. The only difference is that there is
only a global fd namespace, not one for per client.

FileManager is responsible for implementing almost all APIs provided to outer applications.
It's a goroutine-safe data structure. To maximize concurrency, it exploits a two-level
locking strategy. It acquires itself RWMutex, mu, to lookup directory entries or fd table
safely. Then it release mu immediately, and rely on SheetFile.mu to guarantee goroutine-safe
access to specific SheetFiles.

As a directory, FileManager should be persisted during checkpointing. Besides, it manages
all SheetFiles at the same time, so it should not only persist itself, but also persist those
files' metadata as a helper. It also plays as an in-memory cache for filesystem metadata of those
files, loading them into memory on-demand. So a gorm connection is required.
*/
type FileManager struct {
	mu sync.RWMutex
	// All directory entries in the directory. All of them should be loaded into memory once.
	entries map[string]*MapEntry
	// Maps filename to a already opened SheetFile. This map is fulfilled on-demand. If a SheetFile
	// is not being opened currently, it's not presented in the map.
	opened map[string]*sheetfile.SheetFile
	// Maps a fd to a opened filename. Multiple fds are allowed to be pointed to the same file. So
	// their entries in this map will contain same filename.
	fds map[uint64]string
	// Next available fd to be allocated to respond a Open or Create file operation.
	nextFd uint64
	db     *gorm.DB
	alloc  *datanode_alloc.DataNodeAllocator
}

/*
allocFd
allocate a new fd to respond a Open or Create file operation. It simply return nextFd and increase
it by 1 currently.
*/
func (f *FileManager) allocFd() uint64 {
	fd := f.nextFd
	f.nextFd += 1
	return fd
}

/*
openFile
Open an existing SheetFile and allocate a fd which can be used to subsequently
access to this file. If this file is not loaded into memory, do it on-demand.

@para
	filename

@return
	uint64: fd to access this file if filename is valid
	error:
		*errors.FileNotFoundError if the filename is invalid or file has been
		recycled.
*/
func (f *FileManager) openFile(filename string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Lookup in directory entries to check validity of filename.
	entry, ok := f.entries[filename]
	// If there is no such an entry, or the entry has been marked as recycled,
	// the Open operation is invalid.
	if !ok || entry.Recycled {
		return 0, errors.NewFileNotFoundError(filename)
	}
	openedFile, ok := f.opened[filename]
	if !ok {
		// Load file metadata into memory from sqlite on-demand.
		openedFile = sheetfile.LoadSheetFile(f.db, f.alloc, filename)
		f.opened[filename] = openedFile
	}
	fd := f.allocFd()
	// Add new allocated fd to fd table, points to the filename of opened file.
	f.fds[fd] = filename
	return fd, nil
}

/*
getFileByFd
@para
	fd

@return
	*SheetFile: pointer of opened file
	error:
		*errors.FdNotFoundError if the fd is invalid
*/
func (f *FileManager) getFileByFd(fd uint64) (*sheetfile.SheetFile, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	filename, ok := f.fds[fd]
	if !ok {
		return nil, errors.NewFdNotFoundError(fd)
	}
	return f.opened[filename], nil
}

/*
CreateSheet
Creates a new SheetFile and open it immediately.

@para
	filename

@return
	uint64: fd of the newly created file.
	error:
		*errors.FileExistsError if there has been a file with the same filename.
		Although the existing file has been recycled, creating a file with the
		same filename is not allowed.
		errors raised during sheetfile.CreateSheetFile
*/
func (f *FileManager) CreateSheet(filename string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.entries[filename]
	if ok {
		return 0, errors.NewFileExistsError(filename)
	}
	sheet, err := sheetfile.CreateSheetFile(f.db, f.alloc, filename)
	if err != nil {
		return 0, err
	}
	f.entries[filename] = &MapEntry{
		FileName:       filename,
		CellsTableName: sheetfile.GetCellTableName(filename),
		Recycled:       false,
	}
	// Add the new file to opened table and allocate an fd right after creation.
	f.opened[filename] = sheet
	fd := f.allocFd()
	f.fds[fd] = filename
	return fd, nil
}

/*
OpenSheet
@para
	filename

@return
	uint64: fd to access this file if filename is valid
	error:
		*errors.FileNotFoundError if the filename is invalid or file has been
		recycled.
*/
func (f *FileManager) OpenSheet(filename string) (uint64, error) {
	fd, err := f.openFile(filename)
	if err != nil {
		return 0, err
	}
	return fd, nil
}

/*
RecycleSheet
Mark a file as recycled and record RecycledAt. Do nothing if the filename is invalid.

The semantic of this method is Unix-alike too. It won't prevent applications who
have opened this file from editing this file, but later Open or Create operations
will fail.
*/
func (f *FileManager) RecycleSheet(filename string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.entries[filename]
	if !ok {
		return
	}
	entry.Recycled = true
	entry.RecycledAt = time.Now()
}

/*
ResumeSheet
Mark a file as not recycled, so it can be Opened afterwards.
*/
func (f *FileManager) ResumeSheet(filename string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.entries[filename]
	if !ok {
		return
	}
	entry.Recycled = false
}

/*
Monitor
Continuously monitoring all files marked as recycled, and if some file has
exceeded configured retain time period, delete it forever.
*/
func (f *FileManager) Monitor() {
	// TODO
}

/*
ReadSheet
Read all contents of a SheetFile. This method returns all Chunk of a sheet,
applications should then contact with DataNodes to fetch actual content of
those Chunks.

@para
	fd

@returns
	[]*sheetfile.Chunk: List of all Chunks in a file.
	error:
		*errors.FdNotFoundError if the fd is invalid
*/
func (f *FileManager) ReadSheet(fd uint64) ([]*sheetfile.Chunk, error) {
	file, err := f.getFileByFd(fd)
	if err != nil {
		return nil, err
	}
	return file.GetAllChunks(), nil
}

/*
ReadFileCell
Read Cell located at (row, col) in a file pointed by fd.

@para
	fd, row, col

@return
	*Cell, *Chunk: snapshots of corresponding Cell and Chunk
	error:
		*errors.FdNotFoundError if the fd is invalid
		*errors.CellNotFoundError if row, col passed in is invalid.
*/
func (f *FileManager) ReadFileCell(fd uint64, row, col uint32) (*sheetfile.Cell, *sheetfile.Chunk, error) {
	file, err := f.getFileByFd(fd)
	if err != nil {
		return nil, nil, err
	}
	cell, dataChunk, err := file.GetCellChunk(row, col)
	if err != nil {
		return nil, nil, err
	}
	return cell, dataChunk, nil
}

/*
WriteFileCell
Write Cell located at (row, col) in a file pointed by fd. Create Cell if not existed.

@para
	fd, row, col

@return
	*Cell, *Chunk: snapshots of corresponding Cell and Chunk
	error:
		*errors.FdNotFoundError if the fd is invalid
		*errors.NoDataNodeError if there is no DataNode registered.
*/
func (f *FileManager) WriteFileCell(fd uint64, row, col uint32) (*sheetfile.Cell, *sheetfile.Chunk, error) {
	file, err := f.getFileByFd(fd)
	if err != nil {
		return nil, nil, err
	}
	cell, dataChunk, err := file.WriteCellChunk(row, col, f.db)
	if err != nil {
		return nil, nil, err
	}
	return cell, dataChunk, nil
}

/*
GetAllSheets
List all sheets stored in the directory.

@return
	[]*fs_rpc.Sheet: a slice of protobuf fs_rpc.Sheet model, contains
	filename and recycled field.
*/
func (f *FileManager) GetAllSheets() []*fs_rpc.Sheet {
	f.mu.RLock()
	defer f.mu.RUnlock()

	pbSheets := make([]*fs_rpc.Sheet, len(f.entries))
	i := 0
	for _, entry := range f.entries {
		pbSheets[i] = &fs_rpc.Sheet{
			Filename: entry.FileName,
			Recycled: entry.Recycled,
		}
		i += 1
	}
	return pbSheets
}

/*
Persistent
Flush the MapEntry and SheetFile data stored in a FileManager to sqlite in
a transaction.

@return
	error: error during the persistent transaction.
*/
func (f *FileManager) Persistent() error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	err := f.db.Transaction(func(tx *gorm.DB) error {
		for _, entry := range f.entries {
			tx.Save(entry)
		}
		for _, file := range f.opened {
			err := file.Persistent(tx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

/*
LoadFileManager
Load all MapEntry from database and construct FileManager.entries. Opened file
table and fd table may be recovered from journal.

This method should only be used to load checkpoints in sqlite.

@para
	db: a gorm connection. It can be a transaction

@return
	*FileManager
*/
func LoadFileManager(db *gorm.DB, alloc *datanode_alloc.DataNodeAllocator) *FileManager {
	fm := &FileManager{
		entries: map[string]*MapEntry{},
		opened:  map[string]*sheetfile.SheetFile{},
		fds:     map[uint64]string{},
		nextFd:  0,
		db:      db,
		alloc:   alloc,
	}
	var entries []*MapEntry
	db.Find(&entries)
	for _, entry := range entries {
		fm.entries[entry.FileName] = entry
	}
	return fm
}
