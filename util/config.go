package util

import (
	"crypto/sha256"
	"database/sql"
	"github.com/pkg/errors"
	"log"
	"github.com/rami1942/glaman/glacier-manager"
)

type Config struct {
	Database  *sql.DB
	Region    string
	VaultName string

	DocRoot string

	Key    []byte
	Logger *log.Logger

	glacierManager *glacier_manager.Manager
}

const (
	cfg_REGION   = "region"
	cfg_VAULT    = "vault"
	cfg_PASSWORD = "password"
	cfg_DOCROOT  = "basedir"
)

func NewConfig(logger *log.Logger, db *sql.DB) (*Config, error) {
	// 設定の取得
	rows, err := db.Query("select k, v from config")
	if err != nil {
		return nil, errors.Cause(err)
	}
	defer rows.Close()
	cfgMap := map[string]string{}
	for rows.Next() {
		var k, v string
		err = rows.Scan(&k, &v)
		if err != nil {
			return nil, errors.Cause(err)
		}
		cfgMap[k] = v
	}

	//設定チェック
	for _, k := range []string{cfg_REGION, cfg_VAULT, cfg_PASSWORD, cfg_DOCROOT} {
		_, ok := cfgMap[k]
		if !ok {
			return nil, errors.Errorf("必須パラメータ%sが取得できませんでした", k)
		}
	}

	k, err := getKey(logger, cfgMap[cfg_PASSWORD])
	if err != nil {
		return nil, errors.Cause(err)
	}
	return &Config{db, cfgMap[cfg_REGION], cfgMap[cfg_VAULT], cfgMap[cfg_DOCROOT], k, logger, nil}, nil
}

func getKey(logger *log.Logger, plain string) ([]byte, error) {

	if plain == "" {
		logger.Printf("パスワードが空です。")
		return nil, errors.New("パスワードが空です")
	}

	key := sha256.Sum256([]byte(plain))

	return key[:], nil
}

func (c *Config) GlacierManager() (*glacier_manager.Manager, error) {
	if c.glacierManager != nil {
		return c.glacierManager, nil
	}
	return glacier_manager.New("-", c.VaultName, c.Region)
}
