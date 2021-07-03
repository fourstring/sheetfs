package config

import "math"

const (
	MaxCellsPerChunk = 4
	BytesPerChunk    = 8192
	MaxBytesPerCell  = BytesPerChunk / MaxCellsPerChunk
	DBName           = "master.db"
	SheetMetaCellRow = uint32(math.MaxUint32)
	SheetMetaCellCol = uint32(math.MaxUint32)
)
