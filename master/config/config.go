package config

import (
	"math"
	"time"
)

const (
	MaxCellsPerChunk = 4
	BytesPerChunk    = uint64(8192)
	MaxBytesPerCell  = BytesPerChunk / MaxCellsPerChunk
	DBName           = "master.db"
	SheetMetaCellRow = uint32(math.MaxUint32)
	SheetMetaCellCol = uint32(math.MaxUint32)
	ElectionZnode    = "/election"
	ElectionAck      = "/election_ack"
	ElectionChild    = "/election/a20ffeb5-319a-4e0b-b54d-646fb93d3158-n_"
	ElectionTimeout  = 5 * time.Second
)

var SheetMetaCellID = int64(0)
var ElectionServers = []string{
	"127.0.0.1:2181",
	"127.0.0.1:2182",
	"127.0.0.1:2183",
}

func init() {
	SheetMetaCellID = -1
}
