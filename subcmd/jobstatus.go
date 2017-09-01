package subcmd

import (
	"database/sql"
	"fmt"
	"github.com/rami1942/glaman/model"
	"github.com/rami1942/glaman/util"
	"time"
)

func JobStatus(config *util.Config) error {

	mgr, err := config.GlacierManager()
	if err != nil {
		return err
	}

	jobs, err := mgr.JobList()
	if err != nil {
		return err
	}

	for _, j := range jobs {
		entry, err := model.FindEntryByArchiveId(config.Database, *j.ArchiveId)
		if err == sql.ErrNoRows {
			fmt.Printf("UNKNOWN %s %s %s\n", *j.StatusCode, *j.Tier, *j.CreationDate)
		} else if err != nil {
			return err
		}

		utct, err := time.Parse("2006-01-02T15:04:05.999Z", *j.CreationDate)
		t := utct.Local()

		fmt.Printf("%s %s %s %s\n", entry.Name, *j.StatusCode, *j.Tier, t.Format("2006/01/02 15:04:05"))
	}

	return nil
}
