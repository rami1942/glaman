package model

import (
	"crypto/aes"
	"database/sql"
	"github.com/pkg/errors"
)

type FileEntry struct {
	Id        int64
	Name      string
	MD5Sum    string
	Mtime     int64
	Size      int64
	ArchiveId string
	Lock      int

	Comment string
}

const (
	fromClause            = "select id, name, md5sum, mtime, size, archive_id, lock from file_entry"
	fromClauseWithComment = `select e.id, e.name, e.md5sum, e.mtime, e.size, e.archive_id, e.lock, c.comment from file_entry e
			left join comments c on e.id = c.id`
)

type scanRow interface {
	Scan(args ...interface{}) error
}

func scan(row scanRow) (entry *FileEntry, err error) {
	var id int64
	var name, md5sum, archiveId string
	var mtime, size int64
	var lock int

	err = row.Scan(&id, &name, &md5sum, &mtime, &size, &archiveId, &lock)
	if err == sql.ErrNoRows {
		return nil, err
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	entry = &FileEntry{id, name, md5sum, mtime, size, archiveId, lock, ""}
	return

}

func scanWithComment(row scanRow) (entry *FileEntry, err error) {
	var id int64
	var name, md5sum, archiveId string
	var mtime, size int64
	var lock int
	var comment sql.NullString

	err = row.Scan(&id, &name, &md5sum, &mtime, &size, &archiveId, &lock, &comment)
	if err == sql.ErrNoRows {
		return nil, err
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var c string
	if comment.Valid {
		c = comment.String
	} else {
		c = ""
	}
	entry = &FileEntry{id, name, md5sum, mtime, size, archiveId, lock, c}
	return

}

func FindEntrySingle(db *sql.DB, query string, args ...interface{}) (*FileEntry, error) {
	row := db.QueryRow(fromClause+query, args...)
	entry, err := scan(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	return entry, nil

}

func FindEntryByName(db *sql.DB, relPath string) (*FileEntry, error) {
	return FindEntrySingle(db, " where name=?", relPath)
}

func FindEntryByMD5(db *sql.DB, md5sum string) (*FileEntry, error) {
	return FindEntrySingle(db, " where md5sum=?", md5sum)
}

func FindEntryByArchiveId(db *sql.DB, archiveId string) (*FileEntry, error) {
	return FindEntrySingle(db, " where archive_id=?", archiveId)
}

func LockedEntry(db *sql.DB) ([]FileEntry, error) {
	return FindEntryByQuery(db, " where lock=1")
}

func AllEntry(db *sql.DB) ([]FileEntry, error) {
	return FindEntryByQuery(db, "")
}

func FindEntryByQuery(db *sql.DB, query string, args ...interface{}) ([]FileEntry, error) {
	rows, err := db.Query(fromClause+query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	var entry []FileEntry
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		entry = append(entry, *e)
	}
	return entry, nil
}

func LsComment(db *sql.DB) ([]FileEntry, error) {
	return FindEntryByQueryWithComment(db, "")
}

func FindEntryByQueryWithComment(db *sql.DB, query string, args ...interface{}) ([]FileEntry, error) {
	rows, err := db.Query(fromClauseWithComment+query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	var entry []FileEntry
	for rows.Next() {
		e, err := scanWithComment(rows)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		entry = append(entry, *e)
	}
	return entry, nil
}

func GetIV(db *sql.DB, id int64) ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	err := db.QueryRow("select iv from initial_vector where id=?", id).Scan(&iv)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return iv, nil
}

func UpdateName(db *sql.DB, id int64, newName string) error {
	_, err := db.Exec("update file_entry set name=? where id=?", newName, id)
	return errors.WithStack(err)
}

func UpdateLock(db *sql.DB, id int64, lock int) error {
	_, err := db.Exec("update file_entry set lock=? where id=?", lock, id)
	return errors.WithStack(err)
}
