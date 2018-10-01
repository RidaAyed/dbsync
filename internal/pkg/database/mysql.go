package database

import (
	"bytes"
	"strings"
)

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

func (con *DBConnection) InsertMySQL(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var colData []string
	for col := range values {
		cols = append(cols, "`"+col+"`")
		colData = append(colData, con.toDBString(values[col]))
	}

	// Insert data
	b.WriteString("INSERT IGNORE INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ","))
	b.WriteString(") ")
	b.WriteString("VALUES")
	b.WriteString(" (")
	b.WriteString(strings.Join(colData, ","))
	b.WriteString(")")
	b.WriteString(";")
}

func (con *DBConnection) UpsertMySQL(tableName string, values map[string]interface{}, b *bytes.Buffer) {

	// Convert keys and values to string array
	var cols []string
	var insertData []string
	var updateData []string
	for col := range values {
		cols = append(cols, "`"+col+"`")
		insertData = append(insertData, con.toDBString(values[col]))
		updateData = append(updateData, "`"+col+"`="+con.toDBString(values[col]))
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
