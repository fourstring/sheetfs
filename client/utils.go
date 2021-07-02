package client

func connect(data []byte, metadata []byte) []byte {
	var totalData []byte = []byte("{\"celldata\": [") //header
	totalData = append(totalData, data...)            //data
	totalData = totalData[:len(totalData)-1]          // delete the final ","
	totalData = append(totalData, "],"...)            // add "],"
	totalData = append(totalData, metadata...)        //metadata
	totalData = append(totalData, "}"...)
	return totalData
}
