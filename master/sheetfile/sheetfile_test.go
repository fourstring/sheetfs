package sheetfile

import (
	"github.com/smartystreets/goconvey/convey"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"sheetfs/master/config"
	"testing"
)

func getTestDB() (*gorm.DB, error) {
	return gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
}

func TestDynamicCellsTable(t *testing.T) {
	convey.Convey("Create test database", t, func() {
		db, err := getTestDB()
		convey.So(err, convey.ShouldEqual, nil)
		err = db.AutoMigrate(&Chunk{})
		convey.So(err, convey.ShouldEqual, nil)
		convey.Convey("Create test chunks", func() {
			chunk0 := &Chunk{
				DataNode: "0",
				Version:  0,
			}
			chunk1 := &Chunk{
				DataNode: "1",
				Version:  0,
			}
			db.Create(chunk0)
			db.Create(chunk1)
			convey.Convey("Test dynamic table name", func() {
				sheet0 := &SheetFile{
					Chunks: map[uint64]*Chunk{chunk0.ID: chunk0},
					Cells: map[uint64]*Cell{
						GetCellID(0, 0): {
							CellID:    GetCellID(0, 0),
							Offset:    0,
							Size:      0,
							ChunkID:   chunk0.ID,
							sheetName: "sheet0",
						},
						GetCellID(0, 1): {
							CellID:    GetCellID(0, 1),
							Offset:    config.MaxBytesPerCell,
							Size:      0,
							ChunkID:   chunk0.ID,
							sheetName: "sheet0",
						},
					},
				}
				sheet1 := &SheetFile{
					Chunks: map[uint64]*Chunk{chunk1.ID: chunk1},
					Cells: map[uint64]*Cell{
						GetCellID(0, 0): {
							CellID:    GetCellID(0, 0),
							Offset:    0,
							Size:      0,
							ChunkID:   chunk1.ID,
							sheetName: "sheet1",
						},
						GetCellID(0, 1): {
							CellID:    GetCellID(0, 1),
							Offset:    config.MaxBytesPerCell,
							Size:      0,
							ChunkID:   chunk1.ID,
							sheetName: "sheet1",
						},
					},
				}
				err = sheet0.persistentStructure(db)
				convey.So(err, convey.ShouldEqual, nil)
				err = sheet1.persistentStructure(db)
				convey.So(err, convey.ShouldEqual, nil)
				err = sheet0.Persistent(db)
				convey.So(err, convey.ShouldEqual, nil)
				err = sheet1.Persistent(db)
				convey.So(err, convey.ShouldEqual, nil)
				sheet0Cells := GetSheetCellsAll(db, "sheet0")
				convey.So(len(sheet0Cells), convey.ShouldEqual, 2)
				sheet1Cells := GetSheetCellsAll(db, "sheet1")
				convey.So(len(sheet1Cells), convey.ShouldEqual, 2)
			})
		})
		_ = os.Remove("test.db")
	})
}
