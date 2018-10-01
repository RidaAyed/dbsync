package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bitbucket.org/modima/dbsync/internal/pkg/database"
	"github.com/wunderlist/ttlcache"
)

const baseURL = "https://dev-xdot-pepperdial-xdot-com-dot-cloudstack5.appspot.com"

//const baseURL = "https://api-xdot-dialfire-xdot-com"

var l *log.Logger

var (
	config        Config
	campaignID    string
	campaignToken string
	mode          string // webhook | database
	whURI         string
	startDate     string
	cntWorker     int
	db            *database.DBConnection
)

/******************************************
* * * * * * * * CONFIGURATION * * * * * * *
*******************************************/

type Config struct {
	NextRead string `json:"next_read"`
}

func loadConfig() {

	configFile, err := ioutil.ReadFile(os.Getenv("HOME") + "/.dbsync/" + campaignID + ".json")
	if err != nil {
		config = Config{
			NextRead: time.Now().Format(time.RFC3339)[:7], // current month
		}
		l.Printf("Load config: %v\n", config.NextRead)
		saveConfig()
	} else {
		l.Printf("Config loaded. Start read: %v\n", config.NextRead)
	}

	json.Unmarshal(configFile, &config)
}

func saveConfig() {

	var path = os.Getenv("HOME") + "/.dbsync/"

	if _, err := os.Stat(path); err != nil {

		if os.IsNotExist(err) {

			err = os.MkdirAll(path, 0755)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: '%v': %v\n", path, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error creating directory: '%v': %v\n", path, err)
			os.Exit(1)
		}
	}

	jsonData, err := json.Marshal(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	ioutil.WriteFile(path+campaignID+".json", jsonData, 0644)
}

/*******************************************
* * * CLEANUP WORK ON APPLICATION KILL * * *
********************************************/
func cleanup() {

	// Write configuration
	saveConfig()
}

/*******************************************
* * * * * * * * * * MAIN * * * * * * * * * *
********************************************/
func main() {

	// Catch signals
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGKILL,
		syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
		os.Exit(1)
	}()

	// Flags
	//os.OpenFile("debug.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	l = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "%s", os.Args[0])
		fmt.Fprintln(os.Stderr, "Usage of %s:", os.Args[0])
		flag.PrintDefaults()
	}

	cid := flag.String("c", "", "Campaign ID")
	token := flag.String("t", "", "Campaign token")
	workerCount := flag.Int("w", 32, "Number of simultaneous workers")
	execMode := flag.String("m", "database", "Mode (webhook, database)")
	sDate := flag.String("s", "", "Start date in the format '2006-01-02'")

	// Webhook Parameters
	whuri := flag.String("whuri", "", "The webhook connection uri to be used")

	// Database Parameters
	dbmode := flag.String("dbmode", "db_sync", "Database mode (db_sync, db_init, db_update)")
	dbms := flag.String("dbms", "mysql", "The database driver to be used (mysql, postgres or mssql)")
	dburi := flag.String("dburi", "", "The database connection uri to be used")

	flag.Parse()

	campaignID = *cid
	if len(campaignID) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign ID (-c) is required")
		os.Exit(1)
	}

	campaignToken = *token
	if len(campaignToken) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign token (-t) is required")
		os.Exit(1)
	}

	cntWorker = *workerCount

	mode = *execMode
	startDate = *sDate

	// load config
	loadConfig()

	switch mode {
	case "webhook":

		if len(*whuri) == 0 {
			fmt.Fprintln(os.Stderr, "Webhook connection uri is required")
			os.Exit(1)
		}

		whURI = *whuri

		modeWebhook()
	case "database":

		// Check supported db types
		var dbValid = false
		for _, l := range []string{"mysql", "postgres", "mssql"} {
			if *dbms == l {
				dbValid = true
				break
			}
		}
		if !dbValid {
			fmt.Fprintln(os.Stderr, "Invalid database driver "+*dbms)
			os.Exit(1)
		}

		if len(*dburi) == 0 {
			fmt.Fprintln(os.Stderr, "Database connection uri is required")
			os.Exit(1)
		}

		modeDatabase(*dbmode, *dbms, *dburi)
	}
}

func modeWebhook() {

	l.Printf("Mode: Webhook")

	for i := 0; i < cntWorker; i++ {

		go webhookSender(i)
		go contactFetcher(i)
		go eventFetcher(i)
	}

	ticker()
}

func modeDatabase(dbMode string, dbType string, dbURI string) {

	l.Printf("Mode: Database")

	// TODO: restliche DB Modi implementieren

	// init database
	db = database.Open(dbType, dbURI, l)

	for i := 0; i < cntWorker; i++ {

		go dbUpdater(i)
		go contactFetcher(i)
		go eventFetcher(i)
	}

	ticker()
}

/*******************************************
* * * * * * * * DIALFIRE API * * * * * * * *
********************************************/

func getContacts(campaignToken string, campaignID string, contactIDs []string) ([]byte, error) {

	url := baseURL + "/!" + campaignToken + "/api/campaigns/" + campaignID + "/contacts/flat_view"

	l.Printf("Get Contacts: %v\n", url)
	//l.Printf("Data: %v\n", contactIDs)

	data, err := json.Marshal(contactIDs)
	if err != nil {
		return nil, err
	}

	var req *http.Request
	if req, err = http.NewRequest("POST", url, bytes.NewReader(data)); err != nil {
		return nil, err
	}

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			break
		}

		//l.Printf("get contacts response %v", resp.Status)

		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
		time.Sleep(timeout)
	}

	//l.Printf("Contact resp %v" + resp.Status)

	defer resp.Body.Close()

	var result []byte
	if result, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	return result, nil
}

// Parameters: from string, to string, cursor string
func getTransactions(campaignToken string, campaignID string, params map[string]string) ([]byte, error) {

	url := baseURL + "/!" + campaignToken + "/api/campaigns/" + campaignID + "/contacts/transactions/?"
	for k, v := range params {
		url += k + "=" + v + "&"
	}

	l.Printf("Get Transactions: %v\n", url)

	var req *http.Request
	var err error
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return nil, err
	}

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			break
		}

		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 403 {
			fmt.Fprintln(os.Stderr, url+" - "+resp.Status)
			os.Exit(1)
		}

		timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
		time.Sleep(timeout)
	}

	defer resp.Body.Close()

	var result []byte
	if result, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	return result, nil
}

/*******************************************
* * * * * * * * * * WORKER * * * * * * * * *
*******************************************/

type FetchResult struct {
	Count   int
	Results []TAEvent
	Cursor  string
}

type TAEvent struct {
	Fired     string `json:"fired"`
	Seqnr     string `json:"seqnr"`
	Type      string `json:"type"`
	HI        string `json:"hi"`
	Task      string `json:"task"`
	Pointer   string `json:"pointer"`
	MD5       string `json:"md5"`
	ContactID string `json:"contact_id"`
}

type TAPointerList struct {
	ContactID string
	Contact   map[string]interface{}
	Pointer   []string
}

var eventCache = ttlcache.NewCache(time.Minute) // Autoextend bei GET
var chanEventFetcher = make(chan string)

func eventFetcher(n int) {

	l.Printf("Start event fechter %v", n)

	for {

		start := <-chanEventFetcher

		var params = map[string]string{
			"from": start,
		}

		var eventsByContactID = map[string]TAPointerList{}
		for {

			data, err := getTransactions(campaignToken, campaignID, params)
			if err != nil {
				l.Printf(err.Error())
				break
			}

			//l.Printf("%v", string(data))

			var resp FetchResult
			if err = json.Unmarshal(data, &resp); err != nil {
				l.Printf(err.Error())
				break
			}

			// MD5 Prüfen
			for _, event := range resp.Results {

				var key = event.ContactID + event.Fired + event.Seqnr
				oldHash, exists := eventCache.Get(key)

				if exists && oldHash == event.MD5 {
					continue
				}

				var p = event.Pointer
				if !exists {
					p += ",new" // new event
				} else {
					p += ",updated" // updated event
				}

				if pList, exists := eventsByContactID[event.ContactID]; exists {
					pList.Pointer = append(pList.Pointer, p)
				} else {
					//l.Printf("Add Pointer for contact %v", event.ContactID)
					eventsByContactID[event.ContactID] = TAPointerList{
						ContactID: event.ContactID,
						Pointer:   []string{p},
					}
				}
				eventCache.Set(key, event.MD5)
			}

			// Bulk in 30er Schritten
			if len(eventsByContactID) == 30 || resp.Cursor == "" {

				if len(eventsByContactID) > 0 {
					chanContactFetcher <- eventsByContactID
					eventsByContactID = make(map[string]TAPointerList)
				}
			}

			if resp.Cursor == "" {
				break
			}

			params["cursor"] = resp.Cursor
		}
	}
}

var chanContactFetcher = make(chan map[string]TAPointerList)

func contactFetcher(n int) {

	l.Printf("Start contact fechter %v", n)

	for {

		eventsByContactID := <-chanContactFetcher

		//l.Printf("Fetch contacts %v", eventsByContactID)

		var contactIDs = make([]string, 0, len(eventsByContactID))
		for id := range eventsByContactID {
			contactIDs = append(contactIDs, id)
		}

		data, err := getContacts(campaignToken, campaignID, contactIDs)
		if err != nil {
			l.Printf(err.Error())
			break
		}

		var results []interface{}
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		if d.Decode(&results); err != nil {
			l.Printf(err.Error())
			break
		}

		for _, c := range results {

			var contact = c.(map[string]interface{})
			var taPointer = eventsByContactID[contact["$id"].(string)]
			taPointer.Contact = contact

			chanInserter <- taPointer
		}
	}
}

var chanInserter = make(chan TAPointerList)

func webhookSender(n int) {

	l.Printf("Start webhook sender %v", n)

	for {

		taPointer := <-chanInserter

		//l.Printf("got contact %v", taPointer)

		// Kontakt
		var contact = taPointer.Contact
		var taskLog = contact["$task_log"].([]interface{})
		delete(contact, "$task_log")

		// Transaktion
		for _, p := range taPointer.Pointer {

			var splits = strings.Split(p, ",")
			var tlIdx, _ = strconv.Atoi(splits[0])
			var taIdx, _ = strconv.Atoi(splits[1])
			var status = splits[2] // new or updated

			var entry = taskLog[tlIdx].(map[string]interface{})
			var transactions = entry["transactions"].([]interface{})
			var transaction = transactions[taIdx]

			l.Printf("tlIdx %v", tlIdx)
			l.Printf("taIdx %v", taIdx)
			l.Printf("TA %v", transaction)

			var data = map[string]interface{}{
				`contact`:     contact,
				`transaction`: transaction,
				`type`:        status,
			}

			payload, err := json.Marshal(data)
			if err != nil {
				l.Printf(err.Error())
				continue
			}

			sendToWebhook(payload)
		}
	}
}

func dbUpdater(n int) {

	l.Printf("Start database updater %v", n)

	for {

		pointerList := <-chanInserter

		// Kontakt
		var contact = pointerList.Contact
		var taskLog = contact["$task_log"].([]interface{})
		delete(contact, "$task_log")
		db.Upsert("df_contacts", contact)

		// Transaktion
		for _, p := range pointerList.Pointer {

			var splits = strings.Split(p, ",")
			var tlIdx, _ = strconv.Atoi(splits[0])
			var taIdx, _ = strconv.Atoi(splits[1])

			// Transaktion
			var entry = taskLog[tlIdx].(map[string]interface{})
			var transactions = entry["transactions"].([]interface{})
			var transaction = transactions[taIdx].(map[string]interface{})
			transaction["$id"] = hash(contact["$id"].(string) + transaction["fired"].(string) + transaction["sequence_nr"].(json.Number).String())
			transaction["contact_id"] = contact["$id"].(string)

			// Connections
			var connections = transaction["connections"]
			delete(transaction, "connections")
			db.Upsert("df_transactions", transaction)

			if connections == nil {
				continue
			}

			for _, con := range connections.([]interface{}) {

				var connection = con.(map[string]interface{})
				connection["$id"] = hash(transaction["$id"].(string) + connection["fired"].(string))
				connection["transaction_id"] = transaction["$id"]

				// Recordings
				var recordings = connection["recordings"]
				delete(connection, "recordings")
				db.Upsert("df_connections", connection)

				if recordings == nil {
					continue
				}

				for _, rec := range recordings.([]interface{}) {

					var recording = rec.(map[string]interface{})
					recording["$id"] = hash(connection["$id"].(string) + recording["location"].(string))
					recording["connection_id"] = connection["$id"]

					db.Upsert("df_recordings", recording)
				}
			}
		}
	}
}

func sendToWebhook(data []byte) error {

	//l.Printf("send contact %v", data)

	var err error
	var req *http.Request
	if req, err = http.NewRequest("POST", whURI, bytes.NewReader(data)); err != nil {
		return err
	}

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode < 300 {
			break
		}

		if err != nil {
			return err
		}

		timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
		time.Sleep(timeout)
	}

	defer resp.Body.Close()

	return nil
}

func hash(text string) string {
	h := md5.New()
	io.WriteString(h, text)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func ticker() {

	// TODO: use different start date than today

	l.Printf("Start ticker")

	tMin := time.NewTicker(time.Second) // Testing
	//var tMin := time.NewTicker(time.Second * 20)
	tHour := time.NewTicker(time.Minute * 20)
	t12Hour := time.NewTicker(time.Hour * 4)

	for {

		var start = time.Now().UTC()

		select {

		case <-tMin.C:
			chanEventFetcher <- start.Add(-1 * time.Minute).Format("2006-01-02T15:04:05")

		case <-tHour.C:
			chanEventFetcher <- start.Add(-1 * time.Hour).Format("2006-01-02T15:04:05")

		case <-t12Hour.C:
			chanEventFetcher <- start.Add(-12 * time.Hour).Format("2006-01-02T15:04:05")

		}
	}
}
