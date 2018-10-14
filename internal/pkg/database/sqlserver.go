package database

import (
	"bytes"
	"strings"
)

func (con *DBConnection) CreateTableSQLServer(tableName string, columns []map[string]string, b *bytes.Buffer) {

	var cols []string
	for i := 0; i < len(columns); i++ {
		for cName, cType := range columns[i] {
			var def = ""
			if cName == "$id" {
				def = "[$id] varchar(100) NOT NULL PRIMARY KEY "
			} else {
				def = "[" + cName + "] " + con.toDBType(cType)
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("IF NOT EXISTS (SELECT [name] FROM SYS.TABLES WHERE [name] = ")
	b.WriteString("'" + tableName + "'")
	b.WriteString(")")
	b.WriteString(" CREATE TABLE ")
	b.WriteString(tableName)
	b.WriteString("(" + strings.Join(cols, ",") + ")")
	b.WriteString(";")
}

func (con *DBConnection) AddColumnsSQLServer(tableName string, columns map[string]string, b *bytes.Buffer) {

	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)
	b.WriteString(" ADD ")

	var idx = 0
	for key, value := range columns {

		b.WriteString("[" + key + "] ")
		b.WriteString(con.toDBType(value))

		if idx < len(columns)-1 {
			b.WriteString(",")
		}
		idx++
	}
	b.WriteString(";")
}

func (con *DBConnection) PrepareUpsertSQLServer(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for _, col := range columns {
		cols = append(cols, "["+col+"]")
		insertData = append(insertData, "?")           //+strconv.Itoa(len(columns)+idx+2))
		updateData = append(updateData, "["+col+"]=?") //+strconv.Itoa(idx+2))
	}

	// Insert data
	b.WriteString("MERGE " + tableName)
	b.WriteString(" USING ")
	b.WriteString("(")
	b.WriteString("SELECT ")
	b.WriteString("? AS ID")
	b.WriteString(") AS T")
	b.WriteString(" ON ")
	b.WriteString(tableName + ".[$id]")
	b.WriteString("=")
	b.WriteString("T.ID")
	b.WriteString(" WHEN MATCHED THEN UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(" WHEN NOT MATCHED THEN ")
	b.WriteString("INSERT")
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(insertData, ","))
	b.WriteString(")")
	b.WriteString(";")
}

/*
func (con *DBConnection) UpsertSQLServer(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var colData []string
	var updateData []string
	for col := range values {
		cols = append(cols, "["+col+"]")

		// SQLServer unterstützt keine Bools
		if reflect.TypeOf(values[col]).Kind() == reflect.Bool {
			if values[col] == true {
				values[col] = 1
			} else {
				values[col] = 0
			}
		}

		colData = append(colData, con.toDBString(values[col]))
		updateData = append(updateData, "["+col+"]="+con.toDBString(values[col]))
	}

	// Insert data
	b.WriteString("MERGE " + tableName)
	b.WriteString(" USING ")
	b.WriteString("(")
	b.WriteString("SELECT ")
	b.WriteString(con.toDBString(values["$id"]) + " AS ID")
	b.WriteString(") AS T")
	b.WriteString(" ON ")
	b.WriteString(tableName + ".[$id]")
	b.WriteString("=")
	b.WriteString("T.ID")
	b.WriteString(" WHEN MATCHED THEN UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(" WHEN NOT MATCHED THEN ")
	b.WriteString("INSERT")
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(colData, ","))
	b.WriteString(")")
	b.WriteString(";")
}

func (con *DBConnection) InsertSQLServer(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var colData []string
	for col := range values {
		cols = append(cols, "["+col+"]")

		// SQLServer unterstützt keine Bools
		if reflect.TypeOf(values[col]).Kind() == reflect.Bool {
			if values[col] == true {
				values[col] = 1
			} else {
				values[col] = 0
			}
		}

		colData = append(colData, con.toDBString(values[col]))
	}

	// Insert data
	b.WriteString("IF NOT EXISTS (SELECT * FROM " + tableName)
	b.WriteString(" WHERE ")
	b.WriteString("[$id]")
	b.WriteString("=")
	b.WriteString("'" + con.toDBString(values["$id"]) + "'")
	b.WriteString(")")
	b.WriteString(" BEGIN ")
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(colData, ","))
	b.WriteString(")")
	b.WriteString(" END")
	b.WriteString(";")
}
*/
