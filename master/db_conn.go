package main

import (
	"github.com/fourstring/sheetfs/master/config"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func connectDB() (*gorm.DB, error) {
	return gorm.Open(sqlite.Open(config.DBName), &gorm.Config{})
}
