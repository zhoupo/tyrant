package tyrant

import (
	"github.com/c4pt0r/cfg"
	log "github.com/ngaut/logging"
)

var globalCfg *cfg.Cfg

func init() {
	globalCfg = cfg.NewCfg("config.ini")
	err := globalCfg.Load()
	if err != nil {
		log.Fatal(err)
	}
}
