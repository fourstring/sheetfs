package config

const PORT = "8888"
const FILE_LOCATION = "../data/"
const BLOCK_SIZE = 2 << 10
const FILE_SIZE = BLOCK_SIZE << 2
const VERSION_START_LOCATION = BLOCK_SIZE << 2
const SPECIAL_ID = ^uint64(0)
