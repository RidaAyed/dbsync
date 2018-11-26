package database

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
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
		{"$comment": "text"},
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

var dbTypes = map[string]map[string]string{
	"mysql": {
		"string":      "varchar(255)",
		"text":        "text",
		"int":         "numeric",
		"float64":     "numeric",
		"json.Number": "numeric",
		"bool":        "boolean",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"postgres": {
		"string":      "varchar(255)",
		"text":        "text",
		"int":         "numeric",
		"float64":     "numeric",
		"json.Number": "numeric",
		"bool":        "boolean",
		"map[string]interface {}": "json",
		"[]interface {}":          "json",
	},
	"sqlserver": {
		"string":      "nvarchar(255)",
		"text":        "text",
		"int":         "numeric",
		"float64":     "numeric",
		"json.Number": "numeric",
		"bool":        "bit",
		"map[string]interface {}": "nvarchar(4000)", // Maximum is 4000
		"[]interface {}":          "nvarchar(4000)", // Maximum is 4000
	},
}

func (con *DBConnection) toDBType(gotype string) string {

	var dbType = dbTypes[con.DBType][gotype]
	if dbType == "" {
		panic("unsupported type " + gotype)
	}

	return dbType
}

var errorLog *log.Logger
var debugLog *log.Logger

// URIs:
// MYSQL: root:modimai1.Sfm@/df_ml_camp
// POSTGRES: postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp
// MSSQL: sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp
func Open(dbType string, uri string, dl *log.Logger, el *log.Logger) (*DBConnection, error) {

	debugLog = dl
	errorLog = el

	var db *sql.DB
	var err error

	//MySQL: 'root:modimai1.Sfm@localhost:3306/df_ml_camp'
	//Postgres: 'postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp"
	//"sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp"

	switch dbType {

	case "mysql":
		// Remove protocol prefix (not allowed with mysql)
		var idx1 = strings.Index(uri, "://")
		var idx2 = strings.Index(uri, "@")
		var idx3 = strings.LastIndex(uri, "/")
		uri = uri[idx1+3:idx2+1] + "tcp(" + uri[idx2+1:idx3] + ")" + uri[idx3:]
		debugLog.Printf("Connect to mysql: %v\n", uri)
		db, err = sql.Open("mysql", uri)

	case "sqlserver":
		var dbIdx = strings.LastIndex(uri, "/")
		uri = uri[:dbIdx] + "?database=" + uri[dbIdx+1:]
		debugLog.Printf("Connect to sqlserver: %v\n", uri)
		db, err = sql.Open("mssql", uri)

	case "postgres":
		debugLog.Printf("Connect to postgres: %v\n", uri)
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
	Data *map[string]interface{}
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

	//debugLog.Printf("%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		errorLog.Printf("%v\n", b.String())
		errorLog.Printf("%v \n", err.Error())
	}
	return err
}

func (con *DBConnection) Upsert(entity Entity) error {

	var tableName = "df_" + entity.Type + "s"
	var data = filter(entity)

	// Extract fieldNames and values
	var fieldNames []string
	var values []interface{}
	for name, value := range data {
		// Skip empty values (all string in contacts)
		if entity.Type == "contact" && len(value.(string)) == 0 {
			continue
		}
		fieldNames = append(fieldNames, name)
		values = append(values, con.toDBString(value))
		//values = append(values, value)
	}

	//debugLog.Printf("FIELDS: %v | VALUES: %v", fieldNames, values)

	// Prepare statement
	stmt, err := con.PrepareUpsertStatement(tableName, fieldNames)
	if err != nil {
		return err
	}

	// Daten duplizieren (1. Insert / 2. Update)
	values = append(values, values...)

	// SQLServer benÃ¶tigt zusÃ¤tzliches $id Feld fÃ¼r Query
	if con.DBType == "sqlserver" {
		values = append([]interface{}{(*entity.Data)["$id"]}, values...)
	}

	//debugLog.Printf("%v\n\n", fieldNames)
	//debugLog.Printf("%v\n\n", values)

	// Execute statement
	_, err = stmt.Exec(values...)

	// Close statement
	stmt.Close()

	return err
}
func (con *DBConnection) PrepareUpsertStatement(tableName string, data []string) (*sql.Stmt, error) {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.PrepareUpsertMySQL(tableName, data, &b)

	case "postgres":
		con.PrepareUpsertPostgres(tableName, data, &b)

	case "sqlserver":
		con.PrepareUpsertSQLServer(tableName, data, &b)
	}

	//debugLog.Printf("%v", b.String())
	return con.DB.Prepare(b.String())
}

func filter(entity Entity) map[string]interface{} {

	var filteredData = make(map[string]interface{})

	for _, col := range tableSchemas[entity.Type] {

		for cName, _ := range col {

			// Sanitize field name
			//var cName = strings.ToLower(cName)            // most DMBS are case insensitive
			//cName = strings.Replace(cName, "ÃŸ", "ss", -1) // SQLSERVER has problems with 'ÃŸ'

			if (*entity.Data)[cName] != nil {
				filteredData[cName] = (*entity.Data)[cName]
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
			Deleted   bool   `json:"deleted"`
		} `json:"elements"`
	} `json:"form"`
}

func (con *DBConnection) UpdateTables(campaign Campaign) error {

	// Maximal 100 weitere Spalten
	var count = 0
	for _, element := range campaign.Form.Elements {

		if element.Type != "field" || element.Deleted == true {
			continue
		}

		/* Kampagnentypen:
		"text":         "string",
		"date":         "string",
		"calendar":     "string",
		"phone":        "string",
		"radiogroup":   "string",
		"dropdown":     "string",
		"autocomplete": "string",
		"checkbox":     "bool",
		"number":       "int", // TODO: FlieÃŸkomma unterstÃ¼tzen
		"textarea":     "-",   // Ã¼berspringen (nicht relevant)
		*/
		// Abbildung auf datenbanktype
		var dbType string
		switch element.FieldType {

		case "checkbox":
			dbType = "bool"

		case "number":
			dbType = "int"

		default:
			dbType = "string"
		}

		tableSchemas["contact"] = append(tableSchemas["contact"], map[string]string{element.Name: dbType})
		count++

		// Maximal 100 Elemente
		if count == 100 {
			break
		}
	}

	// ggf. Tabellen erzeugen
	if err := con.createTable("df_contacts", tableSchemas["contact"]); err != nil {
		return err
	}
	if err := con.createTable("df_transactions", tableSchemas["transaction"]); err != nil {
		return err
	}
	if err := con.createTable("df_connections", tableSchemas["connection"]); err != nil {
		return err
	}
	if err := con.createTable("df_recordings", tableSchemas["recording"]); err != nil {
		return err
	}

	if err := con.updateColumns("df_contacts", tableSchemas["contact"]); err != nil {
		return err
	}
	return nil
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

	//debugLog.Printf("%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		errorLog.Printf("%v\n", b.String())
		errorLog.Printf("%v \n", err.Error())
	}
	return err
}

func (con *DBConnection) toDBString(value interface{}) string {

	var result string

	switch value.(type) {

	case json.Number:
		if strings.Contains(string(value.(json.Number)), ".") {
			vNum, _ := value.(json.Number).Float64()
			result = strconv.FormatFloat(vNum, 'f', 10, 64)
		} else {
			vNum, _ := value.(json.Number).Int64()
			result = strconv.FormatInt(vNum, 10)
		}

	case []interface{}:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		//result = "'" + string(jsonBytes) + "'"
		result = string(jsonBytes)

	case map[string]interface{}:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		//result = "'" + string(jsonBytes) + "'"
		result = string(jsonBytes)

	case int:
		result = strconv.Itoa(value.(int))

	case bool:
		if value.(bool) {
			result = strconv.FormatInt(1, 10)
		} else {
			result = strconv.FormatInt(0, 10)
		}

	case string:
		result = value.(string)
		// Auf 255 Zeichen beschrÃ¤nken
		/*
			if len(result) > 255 {
				result = result[:255]
			}
		*/

	default:
		panic("unsupported type " + reflect.TypeOf(value).String())
	}

	// Escape special characters
	//result = strings.Replace(result, "'", "\\'", -1)
	//result = strings.Replace(result, "\\\\'", "\\'", -1)

	//return "'" + result + "'"x
	return result
}

func (con *DBConnection) updateColumns(tableName string, columns []map[string]string) error {

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
		if err := con.addTableColumns(tableName, newColumns); err != nil {
			return err
		}
	}
	return nil
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
		errorLog.Printf("%v\n", stmt)
		errorLog.Printf("%v \n", err.Error())
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

func (con *DBConnection) addTableColumns(tableName string, newColumns map[string]string) error {

	var b bytes.Buffer

	switch con.DBType {

	case "mysql":
		con.AddColumnsMySQL(tableName, newColumns, &b)

	case "postgres":
		con.AddColumnsPostgres(tableName, newColumns, &b)

	case "sqlserver":
		con.AddColumnsSQLServer(tableName, newColumns, &b)
	}

	//debugLog.Printf("%v\n\n", b.String())
	_, err := con.DB.Exec(b.String())
	if err != nil {
		errorLog.Printf("%v\n", b.String())
		errorLog.Printf("%v \n", err.Error())
	}
	return err
}
