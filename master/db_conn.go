package main

import (
	"github.com/fourstring/sheetfs/master/config"
	"github.com/fourstring/sheetfs/master/filemgr/mgr_entry"
	"github.com/fourstring/sheetfs/master/journal"
	"github.com/fourstring/sheetfs/master/sheetfile"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func connectDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(config.DBName), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.AutoMigrate(&mgr_entry.MapEntry{}, &sheetfile.Chunk{}, &journal.Checkpoint{})
	if err != nil {
		return nil, err
	}
	return db, nil
}
