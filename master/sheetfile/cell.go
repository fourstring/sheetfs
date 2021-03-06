package sheetfile

import (
	"fmt"
	"github.com/fourstring/sheetfs/master/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"text/template"
)

/*
Cell
Represent a cell of a sheet.
Every Cell is stored in a Chunk, starting at a fixed offset(see SheetFile),
and every Chunk contains multiple Cells. The number of cells stored in a Chunk
is specified by config.MaxCellsPerChunk.
Because every Chunk is composed of slots for config.MaxCellsPerChunk, the Size
of a Cell is set to config.BytesPerChunk / config.MaxCellsPerChunk, except for
the special MetaCell, where the metadata of a SheetFile are stored(see SheetFile too).

Cell plays as an index from row, column number to concrete Chunk which actually stores data,
providing applications an interface to manipulate cell directly, instead of computing offset
of some cell manually.
This index is critical to API of our filesystem, and must be persistent. Cell is also a gorm
model. All Cell of a SheetFile are stored in a sqlite table, named as 'cells_{filename}'. Cell
belongs to different SheetFile will be stored in different tables, we implement this by
executing templated SQL. See also create_tmpl below.
*/
type Cell struct {
	gorm.Model
	// CellID is used to accelerate looking up cell by row and column number
	// CellID is composed of row and column number, which makes CellID a standalone sqlite index,
	// rather than maintaining a joined index on (row,col)
	// uint64 is not supported in sqlite, use int64 as a workaround
	CellID  int64 `gorm:"index"`
	Offset  uint64
	Size    uint64
	ChunkID uint64

	SheetName string `gorm:"-"`
}

func NewCell(cellID int64, offset uint64, size uint64, chunkID uint64, sheetName string) *Cell {
	return &Cell{CellID: cellID, Offset: offset, Size: size, ChunkID: chunkID, SheetName: sheetName}
}

/*
TableName
Returns the table name which contains cells of some SheetFile.
*/
func (c *Cell) TableName() string {
	return GetCellTableName(c.SheetName)
}

/*
Snapshot
Returns a *Cell points to the copy of c.
See SheetFile for the necessity of Snapshot.

@return
	*Cell points to the copy of c.
*/
func (c *Cell) Snapshot() *Cell {
	var nc Cell
	nc = *c
	return &nc
}

/*
GetCellTableName
Same as Cell.TableName, for creation of Cell.

@return
	sqlite table name to store Cell of SheetName
*/
func GetCellTableName(sheetName string) string {
	return fmt.Sprintf("cells_%s", sheetName)
}

/*
GetCellID
Compute CellID by row and column number.
It is almost impossible for a sheet to scale up to 4294967295x4294967295,
so it's enough to use an uint32 to represent row and column.
Due to this, CellID is formed by put row number in higher 32bits of an
uint64, and put column number in lower 32bits.

@return
	uint64 CellID of Cell located at (row, col)
*/
func GetCellID(row uint32, col uint32) int64 {
	return int64(row)<<32 | int64(col)
}

/*
GetSheetCellsAll
Load all Cell of a SheetFile from sqlite database.
This method should only be used to load checkpoints in sqlite.
After loading from sqlite, subsequent mutations should be conducted in memory,
and rely on journaling to tolerate failure, until checkpointing next time.

@return
	[]*Cell: All Cell stored in table corresponding to SheetName
*/
func GetSheetCellsAll(db *gorm.DB, sheetName string) []*Cell {
	cells := []*Cell{}
	db.Table(GetCellTableName(sheetName)).Find(&cells)
	return cells
}

type _tableName struct {
	Name string
}

/*
create_tmpl
create_tmpl is a SQL template used to create Cell table for a new SheetFile.
These SQLs is generated by gorm from currently definition of Cell. If Cell
are modified, remember to update the template too.

Currently there is no check against .Name in template to counter SQL injection,
applications should take care of it.
*/
var create_tmpl *template.Template

func init() {
	create_tmpl, _ = template.New("create_table").Parse("CREATE TABLE `{{ .Name}}` (`id` integer,`created_at` datetime,`updated_at` datetime,`deleted_at` datetime,`cell_id` integer,`offset` integer,`size` integer,`chunk_id` integer,PRIMARY KEY (`id`),CONSTRAINT `fk_chunks_cells` FOREIGN KEY (`chunk_id`) REFERENCES `chunks`(`id`));" +
		"CREATE INDEX `idx_{{ .Name}}_cell_id` ON `{{ .Name}}`(`cell_id`);" +
		"CREATE INDEX `idx_{{ .Name}}_deleted_at` ON `{{ .Name}}`(`deleted_at`);")
}

/*
CreateCellTableIfNotExists
Query the sqlite_master table to check whether Cell table for SheetName has existed or not.
If not, create such a table with create_tmpl. Creating a table in transactions is not allowed
in sqlite, so this function should not be called in db.Transaction.

@para
	db: a gorm connection, should not be a transaction.
	SheetName: filename of Cell belongs to.

@return
	error from execution of queries. If this function is called in a transaction, a 'database is locked'
	will be returned.
*/
func CreateCellTableIfNotExists(db *gorm.DB, sheetName string) error {
	rawdb, err := db.DB()
	if err != nil {
		return err
	}
	rows, err := rawdb.Query("SELECT name FROM sqlite_master WHERE type='table' AND name= ?;", GetCellTableName(sheetName))
	if err != nil {
		return err
	}

	if !rows.Next() {
		rows.Close()
		tn := _tableName{Name: GetCellTableName(sheetName)}

		var b strings.Builder
		err = create_tmpl.Execute(&b, tn)
		if err != nil {
			return err
		}
		createQuery := b.String()
		_, err = rawdb.Exec(createQuery)
		if err != nil {
			return err
		}
	}
	rows.Close()
	return nil
}

/*
Persistent
Flush Cell data in memory into sqlite.
This method should be used only for checkpointing, and is supposed to be called
in a transaction for atomicity.
*/
func (c *Cell) Persistent(tx *gorm.DB) {
	tx.Table(c.TableName()).Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(c)
}

/*
IsMeta
Returns true if c is the MetaCell. (See SheetFile)
*/
func (c *Cell) IsMeta() bool {
	return c.CellID == config.SheetMetaCellID
}
