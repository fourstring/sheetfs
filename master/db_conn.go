package main

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"sheetfs/master/config"
)

func connectDB() (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(config.DBName), &gorm.Config{})
}
