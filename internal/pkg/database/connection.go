package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type Contact struct {
	ID string `json:"$id" gorm:"primary_key;column:$id"` // Primary key
	//Hash         string `json:"-" gorm:"column:$hash"`
	Ref          string `json:"$ref" gorm:"column:$ref"`
	Version      string `json:"$version" gorm:"column:$version"`
	CampaignID   string `json:"$campaign_id" gorm:"column:$campaign_id"`
	TaskID       string `json:"$task_id" gorm:"column:$task_id"`
	Task         string `json:"$task" gorm:"column:$task"`
	Status       string `json:"$status" gorm:"column:$status"`
	StatusDetail string `json:"$status_detail" gorm:"column:$status_detail"`
	Phone        string `json:"$phone" gorm:"column:$phone"`
	CallerID     string `json:"$caller_id" gorm:"column:$caller_id"`
	CreatedDate  string `json:"$created_date" gorm:"column:$created_date"`
	EntryDate    string `json:"$entry_date" gorm:"column:$entry_date"`
	FollowUpDate string `json:"$follow_up_date" gorm:"column:$follow_up_date"`
	Source       string `json:"$source" gorm:"column:$source"`
	Comment      string `json:"$comment" gorm:"column:$comment"`
	Error        string `json:"$error" gorm:"column:$error"`
	Trigger      string `json:"$trigger" gorm:"column:$trigger"`
	Owner        string `json:"$owner" gorm:"column:$owner"`
	RecordingURL string `json:"$recording_url" gorm:"column:$recording_url"`
	Recording    string `json:"$recording gorm:column:$recording"`
}

type Transaction struct {
	ID string `json:"-" gorm:"primary_key;column:$id"` // Primary key
	//Hash            string `json:"-" gorm:"column:$hash"`
	ContactID       string `json:"-" gorm:"column:$contact_id"` // Foreign key -> Contact
	Fired           string `json:"fired" gorm:"column:fired"`
	Type            string `json:"type" gorm:"column:type"`
	TaskID          string `json:"task_id" gorm:"column:task_id"`
	Task            string `json:"task" gorm:"column:task"`
	Status          string `json:"status" gorm:"column:status"`
	StatusDetail    string `json:"status_detail" gorm:"column:status_detail"`
	Actor           string `json:"actor" gorm:"column:actor"`
	Trigger         string `json:"trigger" gorm:"column:trigger"`
	Phone           string `json:"phone" gorm:"column:phone"`
	User            string `json:"user" gorm:"column:user"`
	UserLoginName   string `json:"user_loginName" gorm:"column:user_loginname"`
	UserBranch      string `json:"user_branch" gorm:"column:user_branch"`
	UserTenantAlias string `json:"user_tenantAlias" gorm:"column:user_tenantalias"`
	Dialergroup     string `json:"dialergroup" gorm:"column:dialergroup"`
	Dialerdomain    string `json:"dialerdomain" gorm:"column:dialerdomain"`
	Clientaddress   string `json:"clientaddress" gorm:"column:clientaddress"`
	StartedFrontend string `json:"startedFrontend" gorm:"column:startedfrontend"`
	Started         string `json:"started" gorm:"column:started"`
	Technology      string `json:"technology" gorm:"column:technology"`
	Disconnected    string `json:"disconnected" gorm:"column:disconnected"`
	Result          string `json:"result" gorm:"column:result"`
	IsHI            bool   `json:"isHI" gorm:"column:ishi"`
	Revoked         bool   `json:"revoked" gorm:"column:revoked"`
	WrapupTimeSec   int    `json:"wrapup_time_sec" gorm:"column:wrapup_time_sec"`
	PauseTimeSec    int    `json:"pause_time_sec" gorm:"column:pause_time_sec"`
	EditTimeSec     int    `json:"edit_time_sec" gorm:"column:edit_time_sec"`
	//SequenceNr      int    `json:"sequence_nr" gorm:"column:sequence_nr"`
	//NextSequenceNr  int    `json:"next_sequence_nr" gorm:"column:next_sequence_nr"`
}

type Connection struct {
	ID string `json:"-" gorm:"primary_key;column:$id"` // Primary key
	//Hash            string `json:"-" gorm:"column:$hash"`
	TransactionID   string `json:"-" gorm:"column:$transaction_id"` // Foreign key -> Transaction
	Type            string `json:"type" gorm:"column:type"`
	Dialergroup     string `json:"dialergroup" gorm:"column:dialergroup"`
	Dialerdomain    string `json:"dialerdomain" gorm:"column:dialerdomain"`
	Clientaddress   string `json:"clientaddress" gorm:"column:clientaddress"`
	Phone           string `json:"phone" gorm:"column:phone"`
	Actor           string `json:"actor" gorm:"column:actor"`
	Fired           string `json:"fired" gorm:"column:fired"`
	StartedFrontend string `json:"startedFrontend" gorm:"startedfrontend"`
	Started         string `json:"started" gorm:"column:started"`
	Technology      string `json:"technology" gorm:"column:technology"`
	Connected       string `json:"connected" gorm:"column:connected"`
	Disconnected    string `json:"disconnected" gorm:"column:disconnected"`
	TaskID          string `json:"task_id" gorm:"column:task_id"`
	User            string `json:"user" gorm:"column:user"`
	//SequenceNr      int    `json:"sequence_nr" gorm:"column:sequence_nr"`
}

type Recording struct {
	ID string `json:"-" gorm:"primary_key;column:$id"` // Primary key
	//Hash         string `json:"-" gorm:"column:$hash"`
	ConnectionID string `json:"-" gorm:"column:$connection_id"` // Foreign key -> Connection
	Callnumber   string `json:"callnumber" gorm:"column:callnumber"`
	Filename     string `json:"filename " gorm:"column:filename"`
	Started      string `json:"started" gorm:"started"`
	Stopped      string `json:"stopped" gorm:"column:stopped"`
	Location     string `json:"location" gorm:"column:location"`
}

type DBConnection struct {
	DB     *gorm.DB
	DBType string
}

var l *log.Logger

var typeMap = map[string]map[string]string{
	"mysql": {
		"string":      "varchar(1024)",
		"int64":       "int",
		"float64":     "float",
		"json.Number": "float",
		"bool":        "bool",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"postgres": {
		"string":      "varchar(1024)",
		"int64":       "int",
		"float64":     "float",
		"json.Number": "float",
		"bool":        "bool",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"sqlserver": {
		"string":      "nvarchar(1024)",
		"int64":       "int",
		"float64":     "float",
		"json.Number": "float",
		"bool":        "bool",
		"map[string]interface {}": "nvarchar(4000)", // Maximum
		"[]interface {}":          "nvarchar(4000)", // Maximum
	},
}

// URIs:
// MYSQL: root:modimai1.Sfm@/df_ml_camp
// POSTGRES: postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp
// MSSQL: sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp
func Open(dbType string, uri string, logger *log.Logger) *DBConnection {

	l = logger

	var db *gorm.DB
	var err error
	if dbType == "sqlserver" {
		db, err = gorm.Open("mssql", uri)
	} else {
		db, err = gorm.Open(dbType, uri)
	}

	if err != nil {
		panic(err)
	}

	if err = db.DB().Ping(); err != nil {
		panic(err)
	}

	gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
		return "df_" + defaultTableName
	}

	// Create tables
	db.AutoMigrate(&Contact{})
	db.AutoMigrate(&Transaction{})
	db.AutoMigrate(&Connection{})
	db.AutoMigrate(&Recording{})

	return &DBConnection{
		DB:     db,
		DBType: dbType,
	}
}

func (con *DBConnection) Upsert(tableName string, data map[string]interface{}) error {

	var columns = con.GetTableColumns(tableName)

	//l.Printf("Columns %v", columns)

	// Filter $Felder und erste 100 Felder
	var count = 0
	var newColumns = map[string]string{}
	var filteredData = map[string]interface{}{}
	for f := range data {

		var fieldName = strings.ToLower(f)                    // most DMBS are case insensitive
		fieldName = strings.Replace(fieldName, "ß", "ss", -1) // SQLSERVER has problems with 'ß'

		if strings.HasPrefix(fieldName, "$$") {
			continue
		}

		if !strings.HasPrefix(fieldName, "$") {
			count++
		}

		if columns[fieldName] == "" {
			newColumns[fieldName] = reflect.TypeOf(data[f]).String()
		}
		filteredData[fieldName] = data[f]

		if count == 100 {
			break
		}
	}

	if len(newColumns) > 0 {
		con.AddColumns(tableName, newColumns)
	}

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.UpsertMySQL(tableName, filteredData, &b)

	case "postgres":
		con.UpsertPostgres(tableName, filteredData, &b)

	case "sqlserver":
		con.UpsertSQLServer(tableName, filteredData, &b)
	}

	//l.Printf("%v\n\n", b.String())
	_, err := con.DB.DB().Exec(b.String())
	return err
}

func (con *DBConnection) AddColumns(tableName string, newColumns map[string]string) {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.AddColumnsMySQL(tableName, newColumns, &b)

	case "postgres":
		con.AddColumnsPostgres(tableName, newColumns, &b)

	case "sqlserver":
		con.AddColumnsSQLServer(tableName, newColumns, &b)
	}

	//l.Printf("%v\n\n", b.String())
	_, err := con.DB.DB().Exec(b.String())
	if err != nil {
		fmt.Fprintf(os.Stderr, "ALTER TABLE ERROR: %v | %v\n", tableName, err.Error())
		//panic(err.Error())
	}
}

func (con *DBConnection) toDBString(value interface{}) string {

	switch value.(type) {

	case json.Number:
		if strings.Contains(string(value.(json.Number)), ".") {
			vNum, _ := value.(json.Number).Float64()
			return strconv.FormatFloat(vNum, 'f', 10, 64)
		} else {
			vNum, _ := value.(json.Number).Int64()
			return strconv.FormatInt(vNum, 10)
		}

	case []interface{}:
		jsonString, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		return "'" + string(jsonString) + "'"

	case map[string]interface{}:
		jsonString, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		return "'" + string(jsonString) + "'"

	case int:
		return strconv.Itoa(value.(int))

	case bool:
		return strconv.FormatBool(value.(bool))

	case string:
		return "'" + value.(string) + "'"

	default:
		panic("unsupported type " + reflect.TypeOf(value).String())
	}

	return ""
}

func (con *DBConnection) toDBType(gotype string) string {

	var dbType = typeMap[con.DBType][gotype]
	if dbType == "" {
		panic("unsupported type " + gotype)
	}

	return dbType
}

func (con *DBConnection) GetTableColumns(tableName string) map[string]string {

	// TODO: Datenbank selektieren

	var stmt = ""

	switch con.DBType {

	case "mysql":
		//var stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'my_database' AND TABLE_NAME = 'my_table';"
		stmt = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "';"

	case "sqlserver":
		stmt = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + tableName + "')"

	case "postgres":
		//var stmt = "SELECT * FROM information_schema.columns WHERE table_schema = 'your_schema' AND table_name   = 'your_table'"
		stmt = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "';"
	}

	rows, err := con.DB.Raw(stmt).Rows()
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	var columns = map[string]string{}
	var col string
	for rows.Next() {
		rows.Scan(&col)
		columns[col] = col
	}

	return columns
}
