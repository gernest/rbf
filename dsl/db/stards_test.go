package db

import (
	"testing"

	"github.com/gernest/rbf"
)

func TestReopen(t *testing.T) {
	db := rbf.NewDB(t.TempDir(), nil)
	err := db.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}
