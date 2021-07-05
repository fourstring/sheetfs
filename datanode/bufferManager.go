package main

//var buffermanager *BufferManager
//
//type BufferManager struct {
//	blankBlock []byte
//	blankFile  []byte
//}

func getPaddedBytes(s string, size int) []byte {
	// Fill padding with padByte.
	padByte := []byte(s)[0]
	paddedData := make([]byte, size)
	for i := 0; i < size; i++ {
		paddedData[i] = padByte
	}
	return paddedData
}

//func init() {
//	if buffermanager == nil {
//		buffermanager = &BufferManager{
//			getPaddedBytes(" ", config.BLOCK_SIZE),
//			getPaddedBytes(" ", config.FILE_SIZE),
//		}
//	}
//}
