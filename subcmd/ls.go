package subcmd

import (
	"database/sql"
	"fmt"
	"time"
	"github.com/rami1942/glaman/model"
	"github.com/rami1942/glaman/util"
)

func Ls(config *util.Config, showComment bool, showLock bool) (err error) {
	switch {
	case showLock:
		err = lsLock(config.Database)
	case showComment:
		err = lsComment(config.Database)
	default:
		err = lsNormal(config.Database)
	}
	return err
}

func lsNormal(db *sql.DB) (err error) {
	return listNormal(model.AllEntry(db))
}

func lsLock(db *sql.DB) error {
	return listNormal(model.LockedEntry(db))
}

func listNormal(entry []model.FileEntry, err error) error {
	if err != nil {
		return err
	}
	for _, e := range entry {
		t := time.Unix(0, e.Mtime)

		fmt.Printf("%d\t%s\t%d\t%s\n", e.Id, e.Name, e.Size, t.Format("2006/01/02 15:04:05"))
	}
	return nil
}

func lsComment(db *sql.DB) (err error) {
	entry, err := model.LsComment(db)
	if err != nil {
		return err
	}
	for _, e := range entry {
		fmt.Printf("%d\t%s\t%s\n", e.Id, e.Name, e.Comment)
	}
	return nil
}
