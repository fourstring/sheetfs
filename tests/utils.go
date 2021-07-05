package tests

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

func GetTestDB(automigrates ...interface{}) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.AutoMigrate(automigrates...)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func RandInt(a, b int) int {
	rand.Seed(time.Now().UnixNano())
	return a + rand.Intn(b-a)
}

func DivRoundUp(n, d int) int {
	return (n + (d - 1)) / d
}
