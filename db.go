package tyrant

import (
	"database/sql"
	"github.com/coopernurse/gorp"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/ngaut/logging"
)

var sharedDbMap *gorp.DbMap

func init() {
	InitSharedDbMap()
}

func InitSharedDbMap() {
	sharedDbMap = NewDbMap()
}

func NewDbMap() *gorp.DbMap {
	dsn, _ := globalCfg.ReadString("dsn", "root:root@/tyrant")
	dbType, _ := globalCfg.ReadString("db", "mysql")
	if dbType != "mysql" && dbType != "sqlite3" {
		log.Fatal("db must be mysql or sqlite3")
	}
	db, err := sql.Open(dbType, dsn)
	if err != nil {
		log.Fatal(err)
	}
	var dbmap *gorp.DbMap
	if dbType == "mysql" {
		dbmap = &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}
	} else {
		dbmap = &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	}

	tbl := dbmap.AddTableWithName(Job{}, "jobs").SetKeys(true, "Id")

	tbl.ColMap("name").SetMaxSize(512).SetUnique(true)
	tbl.ColMap("command").SetMaxSize(4096)
	tbl.ColMap("parents").SetMaxSize(4096)
	tbl.ColMap("executor").SetMaxSize(4096)
	tbl.ColMap("executor_flags").SetMaxSize(4096)
	tbl.ColMap("uris").SetMaxSize(2048)

	err = dbmap.CreateTablesIfNotExists()

	if err != nil {
		log.Fatal(err)
	}
	return dbmap
}
