package journal

import (
	"github.com/fourstring/sheetfs/config"
	"github.com/fourstring/sheetfs/datanode/utils"
	fsrpc "github.com/fourstring/sheetfs/protocol"
	"hash/crc32"
)

func ConstructEntry(request *fsrpc.WriteChunkRequest) []byte {
	var entry []byte
	entry = append(entry, utils.Uint64ToBytes(request.Version)...)
	entry = append(entry, utils.Uint64ToBytes(request.Id)...)
	entry = append(entry, utils.Uint64ToBytes(request.Offset)...)
	entry = append(entry, utils.Uint64ToBytes(request.Size)...)
	checksum := crc32.Checksum(request.Data, config.Crc32q)
	entry = append(entry, utils.Uint32ToBytes(checksum)...)
	entry = append(entry, request.Data...)
	return entry
}
