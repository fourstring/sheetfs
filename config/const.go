package config

import "hash/crc32"

const FILE_LOCATION = "../data/"
const BLOCK_SIZE = 2 << 10
const FILE_SIZE = BLOCK_SIZE << 2
const VERSION_START_LOCATION = BLOCK_SIZE << 2
const SPECIAL_ID = ^uint64(0)

var Crc32q = crc32.MakeTable(0xD5828281)
