package subcmd

import (
	"github.com/rami1942/glaman/model"
	"github.com/rami1942/glaman/util"
)

func Lock(config *util.Config, ids []int64, lockValue int) error {
	for _, i := range ids {
		err := model.UpdateLock(config.Database, i, lockValue)
		if err != nil {
			return err
		}
	}
	return nil
}
