package subcmd

import (
	"github.com/rami1942/glaman/glacier-manager"
	"github.com/rami1942/glaman/util"
)

func Gdl(config *util.Config, jobid, fileName, vault, region string) error {
	gmgr, err := glacier_manager.New("-", vault, region)
	if err != nil {
		return err
	}

	return gmgr.DownloadFile(config.Logger, jobid, fileName)
}
