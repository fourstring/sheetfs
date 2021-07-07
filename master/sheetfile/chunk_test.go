package sheetfile

import (
	"fmt"
	"github.com/fourstring/sheetfs/master/config"
	"github.com/fourstring/sheetfs/tests"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func haveSameCells(ac *Chunk, ec *Chunk) bool {
	sameCells := len(ac.Cells) == len(ec.Cells)
	if !sameCells {
		return sameCells
	}
	for i, cell := range ac.Cells {
		sameCells = sameCells && (shouldBeSameCell(*cell, *ec.Cells[i]) == "")
		if !sameCells {
			return sameCells
		}
	}
	return sameCells
}

func shouldBeSameChunk(actual interface{}, expected ...interface{}) string {
	ac, ok := actual.(Chunk)
	if !ok {
		return "actual not a Chunk!"
	}
	ec, ok := expected[0].(Chunk)
	if !ok {
		return "expected not a Chunk!"
	}

	if ac.DataNode == ec.DataNode && ac.Version == ec.Version && haveSameCells(&ac, &ec) {
		return ""
	} else {
		return fmt.Sprintf("actual %v, expected %v", ac, ec)
	}
}

func TestChunk_Persistent(t *testing.T) {
	Convey("Get test db", t, func() {
		db, err := tests.GetTestDB()
		So(err, ShouldBeNil)
		err = db.AutoMigrate(&Chunk{})
		So(err, ShouldBeNil)
		err = CreateCellTableIfNotExists(db, "sheet0")
		So(err, ShouldBeNil)
		Convey("test persist chunk", func() {
			chunk := &Chunk{DataNode: "1", Version: 1}
			chunk.Persistent(db)
			cell1, cell2 := NewCell(0, 0, 0, chunk.ID, "sheet0"),
				NewCell(0, 0, 0, chunk.ID, "sheet0")
			chunk.Cells = []*Cell{
				cell1,
				cell2,
			}
			chunk.Persistent(db)
			c := loadChunkForFile(db, "sheet0", chunk.ID)
			// No full association enabled, save chunk should not save its cells
			So(len(c.Cells), ShouldEqual, 0)
			// Save cells manually
			cell1.Persistent(db)
			cell2.Persistent(db)
			// load again, this time cells should be loaded
			c = loadChunkForFile(db, "sheet0", chunk.ID)
			So(*chunk, shouldBeSameChunk, *c)
		})
	})
}

func TestChunk_IsAvailable(t *testing.T) {
	Convey("Construct test chunk", t, func() {
		chunk1 := &Chunk{DataNode: "1", Version: 1}
		chunk1.Cells = []*Cell{
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
		}
		chunk2 := &Chunk{DataNode: "1", Version: 1}
		chunk2.Cells = []*Cell{
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
		}
		chunk3 := &Chunk{DataNode: "1", Version: 1}
		chunk3.Cells = []*Cell{
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
			NewCell(0, 0, config.MaxBytesPerCell, 0, "sheet0"),
		}
		chunk4 := &Chunk{DataNode: "1", Version: 1}
		chunk4.Cells = []*Cell{
			NewCell(config.SheetMetaCellID, 0, config.BytesPerChunk, 0, "sheet0"),
		}
		Convey("test isAvailable", func() {
			So(chunk1.isAvailable(config.MaxBytesPerCell), ShouldEqual, true)
			So(chunk1.isAvailable(config.BytesPerChunk), ShouldEqual, false)
			So(chunk2.isAvailable(config.MaxBytesPerCell), ShouldEqual, true)
			So(chunk3.isAvailable(config.MaxBytesPerCell), ShouldEqual, false)
			So(chunk4.isAvailable(config.MaxBytesPerCell), ShouldEqual, false)
		})
	})
}

func TestChunk_Snapshot(t *testing.T) {
	Convey("Construct test chunk", t, func() {
		chunk1 := &Chunk{DataNode: "1", Version: 1}
		chunk1.Cells = []*Cell{
			NewCell(0, 0, 0, 0, "sheet0"),
			NewCell(0, 0, 0, 0, "sheet0"),
		}
		Convey("test snapshot", func() {
			c := chunk1.Snapshot()
			So(c, ShouldNotEqual, chunk1)
			So(len(c.Cells), ShouldEqual, 2)
			So(c.Version == chunk1.Version && c.DataNode == chunk1.DataNode, ShouldBeTrue)
		})
	})
}
