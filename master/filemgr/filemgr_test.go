package filemgr

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"gorm.io/gorm"
	"sheetfs/master/datanode_alloc"
	"sheetfs/master/errors"
	"sheetfs/master/sheetfile"
	"sheetfs/tests"
	"sort"
	"testing"
)

func shouldBeSameEntry(actual interface{}, expected ...interface{}) string {
	am, ok := actual.(*MapEntry)
	if !ok {
		return "actual not a *MapEntry!"
	}
	em, ok := expected[0].(*MapEntry)
	if !ok {
		return "expected not a *MapEntry!"
	}
	if am.FileName == em.FileName && am.CellsTableName == em.CellsTableName && am.Recycled == em.Recycled {
		return ""
	} else {
		return fmt.Sprintf("actual %v, expected %v", am, em)
	}
}

func newTestFileManager() (*FileManager, *gorm.DB, error) {
	db, err := tests.GetTestDB(&sheetfile.Chunk{}, &MapEntry{})
	if err != nil {
		return nil, nil, err
	}
	datanode_alloc.AddDataNode("node1")
	fm := &FileManager{
		entries: map[string]*MapEntry{},
		opened:  map[string]*sheetfile.SheetFile{},
		fds:     map[uint64]string{},
		nextFd:  0,
		db:      db,
	}
	return fm, db, nil
}

func TestFileManager_CreateSheet(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		Convey("Create file", func() {
			for i := 0; i < 2; i++ {
				filename := fmt.Sprintf("sheet%d", i)
				fd, err := fm.CreateSheet(filename)
				So(err, ShouldBeNil)
				So(fd, ShouldEqual, uint64(i))
				entry := fm.entries[filename]
				So(entry, shouldBeSameEntry, &MapEntry{
					FileName:       filename,
					CellsTableName: sheetfile.GetCellTableName(filename),
					Recycled:       false,
				})
			}

			Convey("Create existed file", func() {
				_, err := fm.CreateSheet("sheet0")
				So(err, ShouldBeError, errors.NewFileExistsError("sheet0"))
			})
		})
	})
}

func TestFileManager_OpenSheet(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		Convey("Open created file", func() {
			fd1, err := fm.OpenSheet("sheet0")
			So(err, ShouldBeNil)
			So(fd1, ShouldEqual, 1)
			fd2, err := fm.OpenSheet("sheet0")
			So(err, ShouldBeNil)
			So(fd2, ShouldEqual, 2)
			So(fm.fds[fd] == fm.fds[fd1] && fm.fds[fd1] == fm.fds[fd2], ShouldBeTrue)
			Convey("Open non-existed file", func() {
				_, err := fm.OpenSheet("non-existed")
				So(err, ShouldBeError, errors.NewFileNotFoundError("non-existed"))
			})
		})
	})
}

func TestFileManager_Persistent(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, db, err := newTestFileManager()
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet0")
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet1")
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet2")
		So(err, ShouldBeNil)
		Convey("Persist FileManager", func() {
			// Cells data of a newly created SheetFile is not flushed into sqlite
			// until FileManager.Persistent() is called.
			sheet0 := sheetfile.LoadSheetFile(db, "sheet0")
			So(len(sheet0.Cells), ShouldEqual, 0)
			err = fm.Persistent()
			So(err, ShouldBeNil)
			var entries []*MapEntry
			db.Find(&entries)
			So(len(entries), ShouldEqual, 3)
			for i := 0; i < 3; i++ {
				filename := fmt.Sprintf("sheet%d", i)
				sheet := sheetfile.LoadSheetFile(db, filename)
				So(len(sheet.Cells), ShouldEqual, 1)
			}
		})
	})
}

func TestLoadFileManager(t *testing.T) {
	Convey("Construct test FileManager and persist it", t, func() {
		fm, db, err := newTestFileManager()
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet0")
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet1")
		So(err, ShouldBeNil)
		_, err = fm.CreateSheet("sheet2")
		So(err, ShouldBeNil)
		err = fm.Persistent()
		So(err, ShouldBeNil)
		Convey("Load FileManager", func() {
			fm = LoadFileManager(db)
			So(len(fm.entries), ShouldEqual, 3)
			for i := 0; i < 3; i++ {
				filename := fmt.Sprintf("sheet%d", i)
				So(fm.entries[filename].FileName, ShouldEqual, filename)
			}
		})
	})
}

func TestFileManager_RecycleSheet(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		Convey("Recycle a sheet", func() {
			fm.RecycleSheet("sheet0")
			_, err := fm.OpenSheet("sheet0")
			So(err, ShouldBeError, errors.NewFileNotFoundError("sheet0"))
			_, _, err = fm.WriteFileCell(fd, 0, 0)
			So(err, ShouldBeNil)
		})
	})
}

func TestFileManager_ResumeSheet(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		Convey("Recycle a sheet", func() {
			fm.RecycleSheet("sheet0")
			_, err := fm.OpenSheet("sheet0")
			So(err, ShouldBeError, errors.NewFileNotFoundError("sheet0"))
			_, _, err = fm.WriteFileCell(fd, 0, 0)
			So(err, ShouldBeNil)
			Convey("Resume a sheet", func() {
				fm.ResumeSheet("sheet0")
				fd, err = fm.OpenSheet("sheet0")
				So(err, ShouldBeNil)
				_, _, err = fm.WriteFileCell(fd, 0, 0)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestFileManager_GetAllSheets(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		Convey("Create test files", func() {
			for i := 0; i < 10; i++ {
				filename := fmt.Sprintf("sheet%d", i)
				_, err := fm.CreateSheet(filename)
				So(err, ShouldBeNil)
				if i%2 == 0 {
					fm.RecycleSheet(filename)
				}
			}
			Convey("List test files", func() {
				sheets := fm.GetAllSheets()
				sort.Slice(sheets, func(i, j int) bool {
					return sheets[i].Filename < sheets[j].Filename
				})
				for i := 0; i < 10; i++ {
					filename := fmt.Sprintf("sheet%d", i)
					So(sheets[i].Filename, ShouldEqual, filename)
					So(sheets[i].Recycled, ShouldEqual, i%2 == 0)
				}
			})
		})
	})
}

func TestFileManager_WriteFileCell(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		So(err, ShouldBeNil)
		Convey("Write to test file", func() {
			for i := 0; i < 10; i++ {
				_, _, err := fm.WriteFileCell(fd, uint32(i), uint32(i))
				So(err, ShouldBeNil)
			}
			Convey("assert test file", func() {
				sheet := fm.opened["sheet0"]
				So(len(sheet.Cells), ShouldEqual, 11)
				So(len(sheet.Chunks), ShouldEqual, 4)
				So(sheet.LastAvailableChunk.ID, ShouldEqual, 4)
			})
		})
	})
}

func TestFileManager_ReadSheet(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		So(err, ShouldBeNil)
		Convey("Write to test file", func() {
			for i := 0; i < 10; i++ {
				_, _, err := fm.WriteFileCell(fd, uint32(i), uint32(i))
				So(err, ShouldBeNil)
			}
			Convey("Read entire test file", func() {
				_, err := fm.ReadSheet(0xdeafbeef)
				So(err, ShouldBeError, errors.NewFdNotFoundError(0xdeafbeef))
				chunks, err := fm.ReadSheet(fd)
				So(err, ShouldBeNil)
				So(len(chunks), ShouldEqual, 4)
			})
		})
	})
}

func TestFileManager_ReadFileCell(t *testing.T) {
	Convey("Construct test FileManager", t, func() {
		fm, _, err := newTestFileManager()
		So(err, ShouldBeNil)
		fd, err := fm.CreateSheet("sheet0")
		So(err, ShouldBeNil)
		Convey("Write to test file", func() {
			for i := 0; i < 10; i++ {
				_, _, err := fm.WriteFileCell(fd, uint32(i), uint32(i))
				So(err, ShouldBeNil)
			}
			Convey("Read cells in test file", func() {
				for i := uint32(0); i < 10; i++ {
					cell, chunk, err := fm.ReadFileCell(fd, i, i)
					So(err, ShouldBeNil)
					So(cell.ChunkID, ShouldEqual, chunk.ID)
				}
				_, _, err := fm.ReadFileCell(fd, 1111, 1111)
				So(err, ShouldBeError, errors.NewCellNotFoundError(1111, 1111))
			})
		})
	})
}

// TODO
func TestFileManager_Concurrency(t *testing.T) {
}
