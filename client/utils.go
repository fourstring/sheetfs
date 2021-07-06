package client

func connect(data []byte, metadata []byte) []byte {
	totalData := []byte("{\"celldata\": [") //header
	if len(data) > 0 {                      // delete the final ","
		data = data[:len(data)-1]
	}
	totalData = append(totalData, data...) //data
	if len(metadata) > 0 {
		totalData = append(totalData, "],"...)     // add "]," to become "{celldata:[...], metadata: ,...}"
		totalData = append(totalData, metadata...) //metadata
	} else {
		totalData = append(totalData, "]"...) // add "]" to become "{celldata:[...]}"
	}
	totalData = append(totalData, "}"...)
	return totalData
}

func DynamicCopy(dst *[]byte, src []byte) {
	for i := 0; i < len(*dst); i++ {
		(*dst)[i] = src[i]
	}
	*dst = append(*dst, src[len(*dst):]...)
}
