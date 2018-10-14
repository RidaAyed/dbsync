package database

import (
	"bytes"
	"strconv"
	"strings"
)

func (con *DBConnection) CreateTablePostgres(tableName string, columns []map[string]string, b *bytes.Buffer) {

	var cols []string
	for i := 0; i < len(columns); i++ {
		for cName, cType := range columns[i] {
			var def = ""
			if cName == "$id" {
				def = "\"$id\" varchar(100) NOT NULL PRIMARY KEY "
			} else {
				def = "\"" + cName + "\" " + con.toDBType(cType)
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tableName)
	b.WriteString("(" + strings.Join(cols, ",") + ")")
	b.WriteString(";")
}

func (con *DBConnection) AddColumnsPostgres(tableName string, columns map[string]string, b *bytes.Buffer) {

	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	var idx = 0
	for key, value := range columns {

		b.WriteString(" ADD IF NOT EXISTS ")
		b.WriteString("\"" + key + "\" ")
		b.WriteString(con.toDBType(value))

		if idx < len(columns)-1 {
			b.WriteString(",")
		}
		idx++
	}
	b.WriteString(";")
}

func (con *DBConnection) PrepareUpsertPostgres(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for idx, col := range columns {
		cols = append(cols, "\""+col+"\"")
		insertData = append(insertData, "$"+strconv.Itoa(idx+1))
		updateData = append(updateData, "\""+col+"\"=$"+strconv.Itoa(len(columns)+idx+1))
	}

	// Insert data
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(insertData, ","))
	b.WriteString(")")
	b.WriteString(" ON CONFLICT ")
	b.WriteString("(\"$id\")")
	b.WriteString(" DO UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(";")
}

/*
func (con *DBConnection) UpsertPostgres(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for col := range values {
		cols = append(cols, "\""+col+"\"")
		insertData = append(insertData, con.toDBString(values[col]))
		updateData = append(updateData, "\""+col+"\"="+con.toDBString(values[col]))
	}

	// Insert data
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(insertData, ","))
	b.WriteString(")")
	b.WriteString(" ON CONFLICT ")
	b.WriteString("(\"$id\")")
	b.WriteString(" DO UPDATE SET ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(";")
}


func (con *DBConnection) InsertPostgres(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var colData []string
	for col := range values {
		cols = append(cols, "\""+col+"\"")
		colData = append(colData, con.toDBString(values[col]))
	}

	// Insert data
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(colData, ","))
	b.WriteString(")")
	b.WriteString(" ON CONFLICT DO NOTHING")
	b.WriteString(";")
}
*/
