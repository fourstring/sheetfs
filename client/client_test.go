package client

import (
	"sync"
	"testing"
)

var FileLockMap sync.Map

func TestUtils(t *testing.T) {
	data := connect([]byte("{\n            \"c\": 1,\n            \"r\": 2,\n            \"v\": {\n                \"ct\": {\n                    \"fa\": \"General\",\n                    \"t\": \"g\"\n                },\n                \"m\": \"ww\",\n                \"v\": \"ww\"\n            }\n        },"),
		[]byte("\"index\": \"sheet_1\",\n    \"color\": \"\",\n    \"name\": \"1\",\n    \"order\": 0,\n    \"status\": 1"))
	print(data)
}
