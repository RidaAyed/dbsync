package database

import (
	"fmt"
	"os"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type Contact struct {
	ID           string `json:"$id" gorm:"primary_key"`
	Ref          string `json:"$ref"`
	Version      string `json:"$version"`
	CampaignID   string `json:"$campaign_id"`
	TaskID       string `json:"$task_id"`
	Task         string `json:"$task"`
	Status       string `json:"$status"`
	StatusDetail string `json:"$status_detail"`
	Phone        string `json:"$phone"`
	CallerID     string `json:"$caller_id"`
	CreatedDate  string `json:"$created_date"`
	EntryDate    string `json:"$entry_date"`
	FollowUpDate string `json:"$follow_up_date"`
	Source       string `json:"$source"`
	Comment      string `json:"$comment"`
	Error        string `json:"$error"`
	Trigger      string `json:"$trigger"`
	Owner        string `json:"$owner"`
	RecordingURL string `json:"$recording_url"`
	Recording    string `json:"$recording"`
}

type Transaction struct {
	ContactID       string       `json:"contact_id" gorm:"primary_key"`
	Fired           string       `json:"fired" gorm:"primary_key"`
	Type            string       `json:"type"`
	TaskID          string       `json:"task_id"`
	Task            string       `json:"task"`
	Status          string       `json:"status"`
	StatusDetail    string       `json:"status_detail"`
	Actor           string       `json:"actor"`
	Trigger         string       `json:"trigger"`
	SequenceNr      int          `json:"sequence_nr"`
	Phone           string       `json:"phone"`
	User            string       `json:"user"`
	IsHI            bool         `json:"isHI"`
	UserLoginName   string       `json:"user_loginName"`
	UserBranch      string       `json:"user_branch"`
	UserTenantAlias string       `json:"user_tenantAlias"`
	Revoked         bool         `json:"revoked"`
	Dialergroup     string       `json:"dialergroup"`
	Dialerdomain    string       `json:"dialerdomain"`
	Clientaddress   string       `json:"clientaddress"`
	StartedFrontend string       `json:"startedFrontend"`
	Started         string       `json:"started"`
	Technology      string       `json:"technology"`
	Disconnected    string       `json:"disconnected"`
	Result          string       `json:"result"`
	WrapupTimeSec   int          `json:"wrapup_time_sec"`
	Connections     []Connection `json:"connections"`
	//Data            map[string]interface{} `json:"data" sql:"json"`
}

type Connection struct {
	Type            string      `json:"type"`
	Dialergroup     string      `json:"dialergroup"`
	Dialerdomain    string      `json:"dialerdomain"`
	Clientaddress   string      `json:"clientaddress"`
	Phone           string      `json:"phone"`
	Actor           string      `json:"actor"`
	Fired           string      `json:"fired"`
	StartedFrontend string      `json:"startedFrontend"`
	Started         string      `json:"started"`
	Technology      string      `json:"technology"`
	Connected       string      `json:"connected"`
	Disconnected    string      `json:"disconnected"`
	TaskID          string      `json:"task_id"`
	SequenceNr      int         `json:"sequence_nr"`
	User            string      `json:"user"`
	Recordings      []Recording `json:"recordings"`
}

type Recording struct {
	Callnumber string `json:"callnumber"`
	Filename   string `json:"filename"`
	Started    string `json:"started"`
	Stopped    string `json:"stopped"`
	Location   string `json:"location"`
}

type DBConnection struct {
	db *gorm.DB
}

// URIs:
// MYSQL: root:modimai1.Sfm@/df_ml_camp
// POSTGRES: postgres://postgres:modimai1.Sfm@localhost:5432/df_ml_camp
// MSSQL: sqlserver://sa:modimai1.Sfm@localhost:1433?database=df_ml_camp
func Open(dbType string, uri string) *DBConnection {

	db, err := gorm.Open(dbType, uri)
	if err != nil {
		panic(err)
	}

	gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
		return "df_" + defaultTableName
	}

	db.AutoMigrate(&Transaction{}, &Contact{})

	return &DBConnection{
		db: db,
	}
}

func (con *DBConnection) UpsertContact(campaignID string, contact Contact) {

	fmt.Fprintf(os.Stdout, "Upsert contact %v\n", contact.ID)
	con.db.Save(&contact)
}

func (con *DBConnection) UpsertTransaction(campaignID string, transaction Transaction) {

	fmt.Fprintf(os.Stdout, "Upsert transaction %v\n", transaction.ContactID+" | "+transaction.Fired)
	con.db.Save(&transaction)
}
