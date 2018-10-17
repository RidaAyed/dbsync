package database

import (
	"bytes"
	"strings"
)

func (con *DBConnection) CreateTableMySQL(tableName string, columns []map[string]string, b *bytes.Buffer) {

	var cols []string
	for i := 0; i < len(columns); i++ {
		for cName, cType := range columns[i] {
			var def = "`" + cName + "` " + con.toDBType(cType)
			if cName == "$id" {
				def += " NOT NULL PRIMARY KEY "
			}
			cols = append(cols, def)
		}
	}

	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tableName)
	b.WriteString("(" + strings.Join(cols, ",") + ")")
	b.WriteString(";")
}

func (con *DBConnection) AddColumnsMySQL(tableName string, columns map[string]string, b *bytes.Buffer) {

	b.WriteString("ALTER TABLE ")
	b.WriteString(tableName)

	var idx = 0
	for key, value := range columns {

		b.WriteString(" ADD ")
		b.WriteString("`" + key + "` ")
		b.WriteString(con.toDBType(value))

		if idx < len(columns)-1 {
			b.WriteString(",")
		}
		idx++
	}
	b.WriteString(";")
}

func (con *DBConnection) PrepareUpsertMySQL(tableName string, columns []string, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for _, col := range columns {
		cols = append(cols, "`"+col+"`")
		insertData = append(insertData, "?")
		updateData = append(updateData, "`"+col+"`=?")
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
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	b.WriteString(strings.Join(updateData, ","))
	b.WriteString(";")
}
