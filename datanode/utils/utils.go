package utils

import (
	"encoding/binary"
	"github.com/fourstring/sheetfs/config"
	"github.com/fourstring/sheetfs/datanode/buffmgr"
	"os"
)

/* private functions */

func Uint64ToBytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func BytesToUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func Uint32ToBytes(i uint32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, i)
	return buf
}

func BytesToUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func GetPaddedData(data []byte, size uint64, padding string) []byte {
	// Fill padding with padByte.
	var paddedData []byte
	//switch padding {
	//case " ":
	//	copy(paddedData, buffermanager.blankBlock)
	//	break
	//default:
	//	getPaddedBytes(padding, config.BLOCK_SIZE)
	//}
	paddedData = buffmgr.GetPaddedBytes(padding, config.BLOCK_SIZE)

	copy(paddedData[:size+1], data)
	return paddedData
}

func GetPaddedFile(data []byte, size uint64, padding string, offset uint64) []byte {
	// Fill padding with padByte.
	var paddedData []byte
	//switch padding {
	//case " ":
	//	copy(paddedData, buffermanager.blankFile)
	//	break
	//default:
	//	getPaddedBytes(padding, config.FILE_SIZE)
	//}
	paddedData = buffmgr.GetPaddedBytes(padding, config.FILE_SIZE)

	copy(paddedData[offset:offset+size+1], data)
	return paddedData
}

func SyncAndUpdateVersion(file *os.File, version uint64) {
	//file.Sync()
	data := Uint64ToBytes(version)
	file.WriteAt(data, config.VERSION_START_LOCATION)
	//file.Sync()
	file.Close()
}

func GetVersion(file *os.File) uint64 {
	buf := make([]byte, 8)
	file.ReadAt(buf, config.VERSION_START_LOCATION)
	return BytesToUint64(buf)
}
