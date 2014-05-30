package scheduler

import (
	"github.com/c4pt0r/cfg"
	log "github.com/ngaut/logging"
)

var globalCfg *cfg.Cfg

func InitConfig(path string) {
	globalCfg = cfg.NewCfg(path)
	err := globalCfg.Load()
	if err != nil {
		log.Fatal(err)
	}
}
