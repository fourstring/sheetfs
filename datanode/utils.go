package main

import (
	"encoding/binary"
	"os"
	"strconv"
)

/* private functions */

func uint64ToBytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func bytesToUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func getFilename(id uint64) string {
	return FILE_LOCATION + "chunk_" + strconv.FormatUint(id, 10)
}

func getPaddedData(data []byte, size uint64, padding string) []byte {
	// Fill padding with padByte.
	var paddedData []byte
	switch padding {
	case " ":
		paddedData = buffermanager.blankBlock
		break
	default:
		getPaddedBytes(padding, BLOCK_SIZE)
	}

	copy(paddedData[:size+1], data)
	return paddedData
}

func getPaddedFile(data []byte, size uint64, padding string, offset uint64) []byte {
	// Fill padding with padByte.
	var paddedData []byte
	switch padding {
	case " ":
		paddedData = buffermanager.blankFile
		break
	default:
		getPaddedBytes(padding, FILE_SIZE)
	}

	copy(paddedData[offset:offset+size+1], data)
	return paddedData
}

func SyncAndUpdateVersion(file *os.File, version uint64) {
	//file.Sync()
	data := uint64ToBytes(version)
	file.WriteAt(data, VERSION_START_LOCATION)
	//file.Sync()
	file.Close()
}

func getVersion(file *os.File) uint64 {
	buf := make([]byte, 8)
	file.ReadAt(buf, VERSION_START_LOCATION)
	return bytesToUint64(buf)
}
