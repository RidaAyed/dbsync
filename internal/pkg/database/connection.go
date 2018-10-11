package database

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DBConnection struct {
	DB     *sql.DB
	DBType string
}

/*
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
	StartedFrontend string `json:"startedFrontend" gorm:"column:startedFrontend"`
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
	StartedFrontend string `json:"startedFrontend" gorm:"startedFrontend"`
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
*/

var tableSchemas = map[string][]map[string]string{
	"contact": {
		{"$id": "string"},
		{"$ref": "string"},
		{"$version": "string"},
		{"$campaign_id": "string"},
		{"$task_id": "string"},
		{"$task": "string"},
		{"$status": "string"},
		{"$status_detail": "string"},
		{"$phone": "string"},
		{"$caller_id": "string"},
		{"$created_date": "string"},
		{"$entry_date": "string"},
		{"$follow_up_date": "string"},
		{"$source": "string"},
		{"$comment": "string"},
		{"$error": "string"},
		{"$trigger": "string"},
		{"$owner": "string"},
		{"$recording_url": "string"},
		{"$recording": "string"},
	},
	"transaction": []map[string]string{
		{"$id": "string"},
		{"$contact_id": "string"},
		{"fired": "string"},
		{"type": "string"},
		{"task_id": "string"},
		{"task": "string"},
		{"status": "string"},
		{"status_detail": "string"},
		{"actor": "string"},
		{"trigger": "string"},
		{"phone": "string"},
		{"user": "string"},
		{"user_loginName": "string"},
		{"user_branch": "string"},
		{"user_tenantAlias": "string"},
		{"dialergroup": "string"},
		{"dialerdomain": "string"},
		{"clientaddress": "string"},
		{"startedFrontend": "string"},
		{"started": "string"},
		{"technology": "string"},
		{"disconnected": "string"},
		{"result": "string"},
		{"isHI": "bool"},
		{"revoked": "bool"},
		{"wrapup_time_sec": "int"},
		{"pause_time_sec": "int"},
		{"edit_time_sec": "int"},
	},
	"connection": []map[string]string{
		{"$id": "string"},
		{"$transaction_id": "string"},
		{"type": "string"},
		{"dialergroup": "string"},
		{"dialerdomain": "string"},
		{"clientaddress": "string"},
		{"phone": "string"},
		{"actor": "string"},
		{"fired": "string"},
		{"startedFrontend": "string"},
		{"started": "string"},
		{"technology": "string"},
		{"connected": "string"},
		{"disconnected": "string"},
		{"task_id": "string"},
		{"user": "string"},
	},
	"recording": []map[string]string{
		{"$id": "string"},
		{"$connection_id": "string"},
		{"callnumber": "string"},
		{"filename": "string"},
		{"started": "string"},
		{"stopped": "string"},
		{"location": "string"},
	},
}

var typeMap = map[string]map[string]string{
	"mysql": {
		"string":      "varchar(255)",
		"int":         "bigint",
		"float64":     "float",
		"json.Number": "float",
		"bool":        "bit",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"postgres": {
		"string":      "text",
		"int":         "bigint",
		"float64":     "decimal",
		"json.Number": "decimal",
		"bool":        "boolean",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"sqlserver": {
		"string":      "nvarchar(255)",
		"int":         "bigint",
		"float64":     "float",
		"json.Number": "float",
		"bool":        "bit",
		"map[string]interface {}": "nvarchar(4000)", // Maximum
		"[]interface {}":          "nvarchar(4000)", // Maximum
	},
}

var l func(level int, msg string, args ...interface{})

// URIs:
// MYSQL: root:modimai1.Sfm@/df_ml_camp
// POSTGRES: postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp
// MSSQL: sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp
func Open(dbType string, uri string, logger func(level int, msg string, args ...interface{})) (*DBConnection, error) {

	l = logger

	var db *sql.DB
	var err error

	//MySQL: 'root:modimai1.Sfm@localhost:3306/df_ml_camp'
	//Postgres: 'postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp"
	//"sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp"

	switch dbType {

	case "mysql":
		// Remove protocol prefix (not allowed with mysql)
		var idx1 = strings.Index(uri, "://")
		//var idx2 = strings.Index(uri, "@")
		//var idx3 = strings.LastIndex(uri, "/")
		//uri = uri[idx1+3:idx2+1] + "tcp(" + uri[idx2+1:idx3] + ")" + uri[idx3:]
		uri = uri[idx1+3:]
		l(0, "Connect to %v\n", uri)
		db, err = sql.Open("mysql", uri)

	case "sqlserver":
		var dbIdx = strings.LastIndex(uri, "/")
		uri = uri[:dbIdx] + "?database=" + uri[dbIdx+1:]
		l(0, "Connect to %v\n", uri)
		db, err = sql.Open("mssql", uri)

	case "postgres":
		// Remove protocol prefix (not allowed with postgres)
		//var idx1 = strings.Index(uri, "://")
		//uri = uri[idx1+3:]
		l(0, "Connect to %v\n", uri)
		db, err = sql.Open("postgres", uri)
	}

	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	var con = DBConnection{
		DB:     db,
		DBType: dbType,
	}

	return &con, nil
}

type Entity struct {
	Type string
	Data map[string]interface{}
}

func (con *DBConnection) CreateTable(tableName string, columns []map[string]string) error {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.CreateTableMySQL(tableName, columns, &b)

	case "postgres":
		con.CreateTablePostgres(tableName, columns, &b)

	case "sqlserver":
		con.CreateTableSQLServer(tableName, columns, &b)
	}

	//l(0, "%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		l(4, "%v\n", b.String())
		l(4, "%v \n", err.Error())
	}
	return err
}

func (con *DBConnection) Upsert(entity Entity) error {

	var b bytes.Buffer

	var tableName = "df_" + entity.Type + "s"
	var data = filter(entity)

	switch con.DBType {

	case "mysql":
		con.UpsertMySQL(tableName, data, &b)

	case "postgres":
		con.UpsertPostgres(tableName, data, &b)

	case "sqlserver":
		con.UpsertSQLServer(tableName, data, &b)
	}

	//l(0, "%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		l(4, "%v\n", b.String())
		l(4, "%v \n", err.Error())
	}
	return err
}

func filter(entity Entity) map[string]interface{} {

	var filteredData = make(map[string]interface{})

	for _, col := range tableSchemas[entity.Type] {

		// Sanitize field name
		//var fieldName = strings.ToLower(f)                    // most DMBS are case insensitive
		//fieldName = strings.Replace(fieldName, "ß", "ss", -1) // SQLSERVER has problems with 'ß'

		for cName, _ := range col {
			if entity.Data[cName] != nil {
				filteredData[cName] = entity.Data[cName]
			}
		}
	}

	return filteredData
}

type Campaign struct {
	Form struct {
		Elements []struct {
			Type      string `json:"type"`
			FieldType string `json:"fieldType"`
			Name      string `json:"name"`
			State     string `json:"state"`
		} `json:"elements"`
	} `json:"form"`
}

func (con *DBConnection) UpdateTables(campaign Campaign) {

	// Maximal 100 weitere Spalten
	var count = 0
	for _, element := range campaign.Form.Elements {

		if element.Type != "field" || element.State == "hidden" {
			continue
		}

		tableSchemas["contact"] = append(tableSchemas["contact"], map[string]string{element.Name: "string"})
		count++

		// Maximal 100 Elemente
		if count == 100 {
			break
		}
	}

	// ggf. Tabellen erzeugen
	con.createTable("df_contacts", tableSchemas["contact"])
	con.createTable("df_transactions", tableSchemas["transaction"])
	con.createTable("df_connections", tableSchemas["connection"])
	con.createTable("df_recordings", tableSchemas["recording"])

	con.updateColumns("df_contacts", tableSchemas["contact"])
}

func (con *DBConnection) createTable(tableName string, columns []map[string]string) error {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.CreateTableMySQL(tableName, columns, &b)

	case "postgres":
		con.CreateTablePostgres(tableName, columns, &b)

	case "sqlserver":
		con.CreateTableSQLServer(tableName, columns, &b)
	}

	//l(0, "%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		l(4, "%v\n", b.String())
		l(4, "%v \n", err.Error())
	}
	return err
}

func (con *DBConnection) toDBString(value interface{}) string {

	var result string

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
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		result = string(jsonBytes)

	case map[string]interface{}:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		result = string(jsonBytes)

	case int:
		return strconv.Itoa(value.(int))

	case bool:
		return strconv.FormatBool(value.(bool))

	case string:
		result = value.(string)

	default:
		panic("unsupported type " + reflect.TypeOf(value).String())
	}

	// Escape special characters
	return "'" + strings.Replace(result, "'", "''", -1) + "'"
}

func (con *DBConnection) toDBType(gotype string) string {

	var dbType = typeMap[con.DBType][gotype]
	if dbType == "" {
		panic("unsupported type " + gotype)
	}

	return dbType
}

func (con *DBConnection) updateColumns(tableName string, columns []map[string]string) {

	// ggf. Tabelle aktualisieren
	var newColumns = make(map[string]string)
	var existingColumns = con.getTableColumns(tableName)
	for _, col := range columns {
		for cName, cType := range col {
			if existingColumns[cName] == "" {
				newColumns[cName] = cType
			}
		}
	}

	// Datenbankschema aktualisieren
	if len(newColumns) > 0 {
		con.addTableColumns(tableName, newColumns)
	}
}

func (con *DBConnection) getTableColumns(tableName string) map[string]string {

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

	rows, err := con.DB.Query(stmt)
	if err != nil {
		l(4, "%v\n", stmt)
		l(4, "%v \n", err.Error())
		os.Exit(1)
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

func (con *DBConnection) addTableColumns(tableName string, newColumns map[string]string) {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.AddColumnsMySQL(tableName, newColumns, &b)

	case "postgres":
		con.AddColumnsPostgres(tableName, newColumns, &b)

	case "sqlserver":
		con.AddColumnsSQLServer(tableName, newColumns, &b)
	}

	//l(0, "%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		l(4, "%v\n", b.String())
		l(4, "%v \n", err.Error())
	}
}
