package buffmgr

//var buffermanager *BufferManager
//
//type BufferManager struct {
//	blankBlock []byte
//	blankFile  []byte
//}

func GetPaddedBytes(s string, size int) []byte {
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
//			GetPaddedBytes(" ", config.BLOCK_SIZE),
//			GetPaddedBytes(" ", config.FILE_SIZE),
//		}
//	}
//}
