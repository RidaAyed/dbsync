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
