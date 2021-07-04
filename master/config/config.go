package config

import (
	"math"
)

const (
	MaxCellsPerChunk = 4
	BytesPerChunk    = uint64(8192)
	MaxBytesPerCell  = BytesPerChunk / MaxCellsPerChunk
	DBName           = "master.db"
	SheetMetaCellRow = uint32(math.MaxUint32)
	SheetMetaCellCol = uint32(math.MaxUint32)
)

var SheetMetaCellID = int64(0)

func init() {
	SheetMetaCellID = -1
}
