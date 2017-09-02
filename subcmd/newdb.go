package subcmd

import (
	"database/sql"
	"log"
	"os"
)

func NewDB(logger *log.Logger, fileName, region, vault, basedir, password string) (err error) {

	// すでに存在していたらエラーにする
	_, err = os.Stat(fileName)
	if !os.IsNotExist(err) {
		logger.Printf("%s exists.", fileName)
		err = os.ErrExist
		return
	}

	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		logger.Printf("sql.Open failed.")
		return
	}

	sqlstmt := `
		create table file_entry (id integer primary key, md5sum text, name text not null,
			mtime integer not null, size integer not null, archive_id text, lock integer not null default 0);
		create table initial_vector (id integer primary key, iv blob);
		create table comments (id integer primary key, comment text);
		create table ex_request(id integer primary key, job_id text not null, start_dt int not null);
		create table config(k text primary key, v text not null);
	`
	_, err = db.Exec(sqlstmt)
	if err != nil {
		logger.Printf("create table failed.")
		return
	}

	_, err = db.Exec("insert into config (k, v) values ('region', ?), ('vault', ?), ('basedir', ?), ('password', ?)", region, vault, basedir, password)

	return
}
