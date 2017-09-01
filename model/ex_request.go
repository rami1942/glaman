package model

import (
	"database/sql"
	"github.com/pkg/errors"
	"time"
)

type ExRequest struct {
	Id      int64
	JobId   string
	StartDt time.Time
}

func FindExRequestById(db *sql.DB, id int64) (*ExRequest, error) {
	var jobId string
	var sd int64

	err := db.QueryRow("select job_id, start_dt from ex_request where id=?", id).Scan(&jobId, &sd)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, errors.WithStack(err)
	}
	t := time.Unix(0, sd)
	return &ExRequest{id, jobId, t}, nil
}

func InsertRequest(db *sql.DB, id int64, jobId string) error {
	t := time.Now()
	_, err := db.Exec("insert into ex_request (id, job_id, start_dt) values (?, ?, ?)", id, jobId, t.UnixNano())
	return errors.WithStack(err)
}

func DeleteRequest(db *sql.DB, id int64) error {
	_, err := db.Exec("delete from ex_request where id=?", id)
	return errors.WithStack(err)
}
