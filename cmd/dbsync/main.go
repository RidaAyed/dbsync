package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
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
	"sync"
	"syscall"
	"time"

	"bitbucket.org/modima/dbsync/internal/pkg/database"
	"github.com/wunderlist/ttlcache"
)

const FETCH_SIZE_EVENTS = 1000 // Number of transaction events to fetch in one step
const FETCH_SIZE_CONTACTS = 30 // Number of contacts to fetch in one step
const MAX_DB_CONNECTIONS = 64

const (
	baseURL               = "https://dev-xdot-pepperdial-xdot-com-dot-cloudstack5.appspot.com"
	tokenRawContactReader = "ts5uaUtG9QbahmeF6Qrk4tmv6Ru_uV7MHEQJJac_-Pulo3nlvGLcrvCvBAD-hZ_6azy9vUtIK6gJxrw1p1krfW3btMwIimlrh2OrO4UTKI6" // Access token for contact listing (/data/campaigns/*/contacts/) - DEV

	//baseURL               = "https://api.dialfire.com"
	//tokenRawContactReader = "rleKVIRD9XnF3g0zxZSiFcEp0y0FnijlS6ddPDKlCJhmdvfGajvwwBvzwjLtbUFoTbburstKdJvRZ5BFbfOpwioidN6ZFzB5YblqkBCD4QA" // Access token for contact listing (/data/campaigns/*/contacts/) - DEV
)

/******************************************
* RUNTIME VARS
*******************************************/
var (
	db            *database.DBConnection
	config        *AppConfig
	campaignID    string
	campaignToken string
	mode          string
	cntWorker     int
)

/******************************************
* LOGGING
*******************************************/
var (
	debugLog *log.Logger
	errorLog *log.Logger
)

func createLog(filePath string) (*log.Logger, error) {

	var dirPath = filePath[:strings.LastIndex(filePath, "/")]
	if err := createDirectory(dirPath); err != nil {
		return nil, err
	}

	logFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	var logger = log.New(logFile, "[DEBUG]", log.Ldate|log.Ltime|log.Lshortfile)
	return logger, nil
}

/******************************************
* CONFIGURATION
*******************************************/

type AppConfig struct {
	Path      string `json:"-"`
	Timestamp string `json:"timestamp"`
}

func loadConfig(filePath string) (*AppConfig, error) {

	var dirPath = filePath[:strings.LastIndex(filePath, "/")]
	if err := createDirectory(dirPath); err != nil {
		return nil, err
	}

	var config AppConfig
	configFile, err := ioutil.ReadFile(filePath)
	if err != nil {
		config = AppConfig{
			Timestamp: time.Now().UTC().Format(time.RFC3339)[:19], // default: current UTC time in format "2006-01-02T15:04:05"
		}
		debugLog.Printf("Configuration file %v not found!", filePath)
	}

	json.Unmarshal(configFile, &config)
	config.Path = filePath

	return &config, nil
}

func (c *AppConfig) save() {

	jsonData, err := json.Marshal(c)
	if err != nil {
		errorLog.Printf("%v\n", err.Error())
	}

	debugLog.Printf("Save config to " + c.Path)
	//debugLog.Printf("%v", *c)

	ioutil.WriteFile(c.Path, jsonData, 0644)
}

/*******************************************
* teardown TASKS (ON KILL)
********************************************/
func teardown() {

	// Close database connection
	if db != nil {
		db.DB.Close()
	}

	// Save configuration
	config.save()
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
		teardown()
		os.Exit(1)
	}()

	// Flags
	flag.Usage = func() {
		var description = `This tool can be used to export all transactions on contacts in dialfire to either a DBMS or a webservice. The export is campaign based (flag 'c').
A valid access token for the specified campaign is required (flag 'ct'). The token can be created in Dialfire. Further a custom start date can be specified to delimit the export (flag 's').
		
Example 1: Insert all transactions that occured after the 01. February 2018 in campaign "MY_CAMPAIGN" to a local running instance of SQL Server:
	./dbsync -a db_sync  -c MY_CAMPAIGN_ID -ct MY_CAMPAIGN_TOKEN -s 2018-02-01 -url 'sqlserver://my_user:my_password@localhost:1433/my_database'
		
Example 2: Send all future transactions in campaign "MY_CAMPAIGN" to a webservice (The webservice should accept JSON data and respnd with status code 200 ... 299 on success):
	./dbsync -a webhook -c MY_CAMPAIGN_ID -ct MY_CAMPAIGN_TOKEN -url 'https://example.com/api/transactions/'`

		fmt.Printf("\n%v\n\n", description)
		fmt.Printf("Flags:\n")
		flag.PrintDefaults()
	}

	cid := flag.String("c", "", "Campaign ID (required)")
	token := flag.String("ct", "", "Campaign API token (required)")
	workerCount := flag.Int("w", 128, "Number of simultaneous workers")
	execMode := flag.String("a", "", `Execution mode:
webhook ... Send all transactions to a webservice
db_init ... Initialize a database with all transactions of the campaign, then stop
db_update ... Update a database with all transactions after specified start date (CLI arg 's'), then stop (default is one week)
db_sync ...  Update a database with all future transactions, optionally go back to a specified start date (CLI arg 's')`)
	dateStart := flag.String("s", "", "Start date in the format '2006-01-02T15:04:05'")
	filterMode := flag.String("fm", "", `Transaction filter mode:
updates_only ... only transactions of type 'update'
hi_updates_only ... only transactions of type 'update' that were triggered by a human interaction`)
	tPrefix := flag.String("fp", "", "Filter transactions by one or several task(-prefixes) (comma separated), e.g. 'fc_,qc_'")
	URL := flag.String("url", "", `Mode webhook: URL pointing to a webservice that handles the transaction data
Mode db_*: DBMS Connection URL of the form '{mysql|sqlserver|postgres}://user:password@host:port/database'`)

	flag.Parse()

	// Check required flags
	campaignID = *cid
	if len(campaignID) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign ID (-c) is required")
		os.Exit(1)
	}

	campaignToken = *token
	if len(campaignToken) == 0 {
		fmt.Fprintln(os.Stderr, "Campaign token (-ct) is required")
		os.Exit(1)
	}

	cntWorker = *workerCount
	mode = *execMode
	url := *URL

	// Setup parameters
	if len(*tPrefix) > 0 {
		eventOptions["tasks"] = *tPrefix
	}

	if len(*filterMode) > 0 {

		switch *filterMode {
		case "updates_only":
			eventOptions["type"] = "update"
		case "hi_updates_only":
			eventOptions["type"] = "update"
			eventOptions["hi"] = "true"
		}
	}

	// Create logger
	var err error
	//debugLog, err = createLog("/var/log/dbsync/" + campaignID + "_" + mode + "_" + time.Now().Format("20060102150405") + ".log")
	debugLog, err = createLog("/var/log/dbsync/" + campaignID + "_" + mode + ".log")
	if err != nil {
		debugLog, err = createLog(os.Getenv("HOME") + "/.dbsync/logs/" + campaignID + "_" + mode + ".log")
		if err != nil {
			panic(err)
		}
	}
	errorLog = log.New(os.Stdout, "[ERROR]", log.Ldate|log.Ltime|log.Lshortfile)

	// Load config
	config, err = loadConfig("/var/opt/dbsync/" + campaignID + ".json")
	if err != nil {
		config, err = loadConfig(os.Getenv("HOME") + "/.dbsync/" + campaignID + ".json")
		if err != nil {
			panic(err)
		}
	}

	// Set start date from config file (iff not explicitly defined)
	var startDate string
	if *dateStart != "" {
		startDate = *dateStart
	} else {
		startDate = config.Timestamp
	}

	if mode == "webhook" {

		if len(url) == 0 {
			fmt.Fprintln(os.Stderr, "URL (CLI arg 'url') is required")
			os.Exit(1)
		}

		modeWebhook(url, startDate)
	} else {

		var dbms = url[:strings.Index(url, ":")]
		var dbName = url[strings.LastIndex(url, "/")+1:]

		// Check supported db types
		var dbValid = false
		for _, l := range []string{"mysql", "postgres", "sqlserver"} {
			if dbms == l {
				dbValid = true
				break
			}
		}
		if !dbValid {
			errorLog.Printf("Invalid database driver '%v'", dbms)
			os.Exit(1)
		}

		if len(url) == 0 {
			fmt.Fprintln(os.Stderr, "Database URL (CLI arg 'dburi') is required")
			os.Exit(1)
		}

		if len(dbName) == 0 {
			fmt.Fprintln(os.Stderr, "Database name is required")
			os.Exit(1)
		}

		// Datenbankverbindung öffnent
		db, err = database.Open(dbms, url, debugLog, errorLog)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		// Schema aktualisieren
		prepareDatabase()

		switch mode {

		case "db_init":
			modeDatabaseInit()

		case "db_update":

			if *dateStart == "" {
				startDate = time.Now().UTC().Add(-168 * time.Hour).Format("2006-01-02") // default: -1 week, iff no start date was passed as command line argument
			}
			modeDatabaseUpdate(startDate)

		case "db_sync":
			modeDatabaseSync(startDate)
		}

	}
}

func prepareDatabase() {

	// Kampagne laden
	data, err := getCampaign()
	if err != nil {
		errorLog.Printf("%v\n", err.Error())
		os.Exit(1)
	}

	var campaign database.Campaign
	if err = json.Unmarshal(data, &campaign); err != nil {
		errorLog.Printf("%v\n", err.Error())
		os.Exit(1)
	}

	// Schema für Kontakttabelle erzeugen und ggf. DB Tabelle aktualisieren
	if err = db.UpdateTables(campaign); err != nil {
		errorLog.Printf("%v\n", err.Error())
		os.Exit(1)
	}
}

/*******************************************
* MODE: WEBHOOK
********************************************/
func modeWebhook(url string, startDate string) {

	debugLog.Printf("Mode: Webhook")

	var wg1, wg2, wg3, wg4 sync.WaitGroup

	// Events rückwärts laden, falls Startdatum gesetzt
	if startDate != "" {
		wg1.Add(1)
		go reverseTicker(startDate, &wg1)
	}

	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	wg4.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {

		go eventFetcher(i, &wg2)
		go contactFetcher(i, &wg3)
		go webhookSender(i, url, &wg4)
	}

	// Runs forever
	ticker()

	// Cleanup
	teardown()
}

func webhookSender(n int, url string, wg *sync.WaitGroup) {

	//debugLog.Printf("Start webhook sender %v", n)

	defer wg.Done()

	for {

		taPointer, ok := <-chanDataSplitter
		if !ok {
			break
		}

		debugLog.Printf("Send transactions contact: %v | pointer: %v", taPointer.ContactID, taPointer.Pointer)

		// Kontakt
		var contact = taPointer.Contact
		var taskLog = contact["$task_log"].([]interface{})
		delete(contact, "$task_log")

		// Transaktion
		for _, p := range taPointer.Pointer {

			var splits = strings.Split(p, ",")
			var tlIdx, _ = strconv.Atoi(splits[0])
			var taIdx, _ = strconv.Atoi(splits[1])
			var state = splits[2] // new or updated

			var entry = taskLog[tlIdx].(map[string]interface{})
			var transactions = entry["transactions"].([]interface{})
			var transaction = transactions[taIdx]

			//debugLog.Printf("tlIdx %v", tlIdx)
			//debugLog.Printf("taIdx %v", taIdx)
			//debugLog.Printf("TA %v", transaction)

			var data = map[string]interface{}{
				`contact`:     contact,
				`transaction`: transaction,
				`state`:       state,
			}

			debugLog.Printf("Send transaction contact: %v | pointer: %v", taPointer.ContactID, p)

			payload, err := json.Marshal(data)
			if err != nil {
				errorLog.Printf("%v\n", err.Error())
				continue
			}

			// TESTING
			/*
				var re = regexp.MustCompile(`\W`)
				s := re.ReplaceAllString(transaction.(map[string]interface{})["fired"].(string), ``)
				var url = whURI + taPointer.ContactID + "_" + s
			*/
			// TESTING END

			err = callWebservice(url, payload)
			if err == nil {
				// Save start date if transaction was sent successfully
				config.Timestamp = transaction.(map[string]interface{})["fired"].(string)
			} else {
				errorLog.Printf("%v\n", err.Error())
			}
		}
	}

	//debugLog.Printf("Stop webhook sender %v", n)
}

func callWebservice(url string, data []byte) error {

	var err error
	var req *http.Request
	if req, err = http.NewRequest("POST", url, bytes.NewReader(data)); err != nil {
		return err
	}

	var resp *http.Response

	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode < 300 {
			return nil
		}

		if err != nil {
			return err
		}

		debugLog.Printf("[POST] %v | attempt: %v | status: %v", url, i+1, resp.Status)

		timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
		time.Sleep(timeout)
	}

	defer resp.Body.Close()

	return errors.New("Webhook status " + resp.Status)
}

/*******************************************
* MODE: DATABASE INITIALIZE
********************************************/

func modeDatabaseInit() {

	debugLog.Printf("Mode: Database Initialize")

	var wg1, wg2, wg3, wg4 sync.WaitGroup

	wg1.Add(1)
	go contactLister(&wg1)

	// Start worker
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		go contactFetcher(i, &wg2)
		go dataSplitter(i, &wg3)
	}

	wg4.Add(MAX_DB_CONNECTIONS)
	for i := 0; i < MAX_DB_CONNECTIONS; i++ {
		go databaseUpdater(i, &wg4)
	}

	go statisticAggregator()

	// 1. Wait until all contact ids have been listed
	wg1.Wait()
	debugLog.Printf("Contact listing DONE")
	close(chanContactFetcher)

	wg2.Wait()
	debugLog.Printf("Contact fetch DONE")
	close(chanDataSplitter)

	wg3.Wait()
	debugLog.Printf("Data split DONE")
	close(chanDatabaseUpdater)

	wg4.Wait()
	debugLog.Printf("Database update DONE")
	close(chanStatistics)
	<-chanDone // Wait until statistics have been logged

	// Cleanup
	teardown()
}

/*******************************************
* MODE: DATABASE UPDATE
********************************************/

func modeDatabaseUpdate(startDate string) {

	debugLog.Printf("Mode: Database Update starting at " + startDate)

	var wg1, wg2, wg3, wg4, wg5 sync.WaitGroup

	wg1.Add(1)
	go reverseTicker(startDate, &wg1)

	// Start worker
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	wg4.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {

		go eventFetcher(i, &wg2)
		go contactFetcher(i, &wg3)
		go dataSplitter(i, &wg4)
	}

	wg5.Add(MAX_DB_CONNECTIONS)
	for i := 0; i < MAX_DB_CONNECTIONS; i++ {
		go databaseUpdater(i, &wg5)
	}

	go statisticAggregator()

	// 1. Wait until time range has been past
	wg1.Wait()
	//debugLog.Printf("Iterate time range DONE")
	close(chanEventFetcher)

	wg2.Wait()
	debugLog.Printf("Event fetch DONE")
	close(chanContactFetcher)

	wg3.Wait()
	debugLog.Printf("Contact fetch DONE")
	close(chanDataSplitter)

	wg4.Wait()
	debugLog.Printf("Data split DONE")
	close(chanDatabaseUpdater)

	wg5.Wait()
	debugLog.Printf("Database update DONE")
	close(chanStatistics)
	<-chanDone // Wait until statistics have been logged

	// Cleanup
	teardown()
}

/*******************************************
* MODE: DATABASE SYNCHRONIZATION
********************************************/

func modeDatabaseSync(startDate string) {

	debugLog.Printf("Mode: Database Synchronize")

	var wg1, wg2, wg3, wg4 sync.WaitGroup

	// Events rückwärts laden, falls Startdatum gesetzt
	if startDate != "" {
		wg1.Add(1)
		go reverseTicker(startDate, &wg1)
	}

	// Start worker
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {

		go dataSplitter(i, &wg1)
		go contactFetcher(i, &wg2)
		go eventFetcher(i, &wg3)
	}

	wg4.Add(MAX_DB_CONNECTIONS)
	for i := 0; i < MAX_DB_CONNECTIONS; i++ {
		go databaseUpdater(i, &wg4)
	}

	ticker()

	// Cleanup
	teardown()
}

/*******************************************
* DIALFIRE API
********************************************/
func getCampaign() ([]byte, error) {

	url := baseURL + "/api/campaigns/" + campaignID

	//debugLog.Printf("Load contacts: %v\n", contactIDs)
	//debugLog.Printf("Data: %v\n", contactIDs)

	var err error
	var req *http.Request
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			//debugLog.Printf("GET contacts: %v - Status: 200", url)
			break
		}

		debugLog.Printf("GET campaign: %v - Status %v", url, resp.Status)

		//debugLog.Printf("get contacts response %v", resp.Status)

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

func listContacts(cursor string) ([]byte, error) {

	url := baseURL + "/data/campaigns/" + campaignID + "/contacts/?_type_=f&_limit_=" + strconv.Itoa(FETCH_SIZE_CONTACTS) + "&_name___GT=" + cursor

	//debugLog.Printf("List Contacts: %v\n", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tokenRawContactReader)

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

func getContacts(contactIDs []string) ([]byte, error) {

	url := baseURL + "/api/campaigns/" + campaignID + "/contacts/flat_view"

	//debugLog.Printf("Load contacts: %v\n", contactIDs)
	//debugLog.Printf("Data: %v\n", contactIDs)

	data, err := json.Marshal(contactIDs)
	if err != nil {
		return nil, err
	}

	var req *http.Request
	if req, err = http.NewRequest("POST", url, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			//debugLog.Printf("GET contacts: %v - Status: 200", url)
			break
		}

		debugLog.Printf("GET contacts: %v - Status %v", url, resp.Status)

		//debugLog.Printf("get contacts response %v", resp.Status)

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

var eventOptions = map[string]string{
	"type":  "",
	"hi":    "",
	"tasks": "",
}

// Parameters: from string, to string, cursor string
func getTransactions(params map[string]string) ([]byte, error) {

	url := baseURL + "/api/campaigns/" + campaignID + "/contacts/transactions/?"

	//debugLog.Printf("Params %v", params)

	// CLI Options
	for k, v := range eventOptions {
		if v != "" {
			url += k + "=" + v + "&"
		}
	}

	// Additional Parameters
	for k, v := range params {
		url += k + "=" + v + "&"
	}

	// Limit
	url += "limit=" + strconv.Itoa(FETCH_SIZE_EVENTS)

	//debugLog.Printf("[GET] %v", url)

	var req *http.Request
	var err error
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			//debugLog.Printf("GET transactions: %v - Status: 200", url)
			break
		}

		debugLog.Printf("GET transactions: %v - Status %v", url, resp.Status)

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
* WORKER
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

type TimeRange struct {
	From time.Time
	To   time.Time
	Ack  bool // Indicates if acknoledgement is required (for flow control)
}

type EventFetchResult struct {
	Duration time.Duration // Timespan
	Count    int           // Number of events
}

var chanEventFetcher = make(chan TimeRange)
var chanEventFetchDone = make(chan EventFetchResult, 100) // Channel für DONE Message von event fetcher (liefert Anzahl d. Evens zurück)
var eventCache = ttlcache.NewCache(time.Minute)           // Autoextend bei GET

func eventFetcher(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start event fechter %v", n)

	defer wg.Done()

	for {

		timeRange, ok := <-chanEventFetcher
		if !ok {
			break
		}

		var from = timeRange.From.Format("2006-01-02T15:04:05")
		var to = timeRange.To.Format("2006-01-02T15:04:05")

		var params = map[string]string{}

		if !timeRange.From.IsZero() {
			params["from"] = from
		}
		if !timeRange.To.IsZero() {
			params["to"] = to
		}

		var eventCount = 0
		var eventCountTotal = 0
		var eventsByContactID = map[string]TAPointerList{}
		for {

			// Transaktionen laden
			data, err := getTransactions(params)
			if err != nil {
				errorLog.Printf("%v\n", err.Error())
				break
			}

			// Result
			var resp FetchResult
			if err = json.Unmarshal(data, &resp); err != nil {
				errorLog.Printf("%v\n", err.Error())
				break
			}

			// MD5 Prüfung
			for _, event := range resp.Results {

				var key = event.ContactID + event.Fired + event.Seqnr
				oldHash, exists := eventCache.Get(key)
				if exists && oldHash == event.MD5 {
					continue
				}

				// Event counter erhöhen
				eventCount++
				eventCountTotal++

				var p = event.Pointer
				if !exists {
					p += ",new" // new event
				} else {
					p += ",updated" // updated event
				}

				if eventsByContactID[event.ContactID].ContactID == "" {
					eventsByContactID[event.ContactID] = TAPointerList{
						ContactID: event.ContactID,
						Pointer:   []string{p},
					}
				} else {
					var pList = eventsByContactID[event.ContactID]
					pList.Pointer = append(pList.Pointer, p)
					eventsByContactID[event.ContactID] = pList
				}
				eventCache.Set(key, event.MD5)
			}

			// Batch in 1000er Schritten
			if len(eventsByContactID) >= FETCH_SIZE_CONTACTS || resp.Cursor == "" {
				//debugLog.Printf("Eventlist %v", eventsByContactID)

				if len(eventsByContactID) > 0 {
					//debugLog.Printf("Event fetcher %v: %v transactions | %v contacts", n, eventCount, len(eventsByContactID))
					chanContactFetcher <- eventsByContactID
					eventsByContactID = make(map[string]TAPointerList)
					eventCount = 0
				}
			}

			if resp.Cursor == "" {
				debugLog.Printf("Event fetcher %v: %v transactions | from: %v | to: %v", n, eventCountTotal, params["from"], params["to"])
				// Acknoledge fetch DONE
				if timeRange.Ack {
					var duration = time.Duration(0)
					if !timeRange.To.IsZero() {
						duration = timeRange.To.Sub(timeRange.From)
					}
					chanEventFetchDone <- EventFetchResult{
						Duration: duration,
						Count:    eventCountTotal,
					}
				}
				break
			}

			params["cursor"] = resp.Cursor
		}
	}

	//debugLog.Printf("Stop event fechter %v", n)
}

type Statistic struct {
	Type  string
	Count int
}

var chanStatistics = make(chan Statistic)
var chanDone = make(chan bool)

func statisticAggregator() {

	var statistics = make(map[string]int)

	for {

		statistic, ok := <-chanStatistics
		if !ok {
			break
		}

		statistics[statistic.Type] += statistic.Count
	}

	// Print statistics
	debugLog.Printf("------------------------------------------------------------------------------------------")
	debugLog.Printf("Protocol:")
	for sType, sCount := range statistics {
		debugLog.Printf("%v: %v", sType, sCount)
	}
	chanDone <- true
}

type ListingResponse struct {
	Results []map[string]interface{} `json:"_results_"`
	Count   int                      `json:"_count_"`
}

func contactLister(wg *sync.WaitGroup) {

	//debugLog.Printf("Start contact lister")

	defer wg.Done()

	var cursor string
	for {

		data, err := listContacts(cursor)
		if err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		var resp ListingResponse
		if err = json.Unmarshal(data, &resp); err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		var eventsByContactID = map[string]TAPointerList{}
		for _, r := range resp.Results {

			var contactID = r["_name_"].(string)
			cursor = contactID
			eventsByContactID[contactID] = TAPointerList{}
		}
		chanContactFetcher <- eventsByContactID

		if resp.Count < FETCH_SIZE_CONTACTS {
			break
		}
	}

	//debugLog.Printf("Stop contact lister")
}

// TODO: Caching von Kontakten (MD5 und Viewabfrage???)
var chanContactFetcher = make(chan map[string]TAPointerList)

//var contactCache = ttlcache.NewCache(time.Minute) // Autoextend bei GET

func contactFetcher(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start contact fechter %v", n)

	defer wg.Done()

	for {

		eventsByContactID, ok := <-chanContactFetcher
		if !ok {
			break
		}

		//debugLog.Printf("Fetch contacts %v\n", eventsByContactID)
		debugLog.Printf("Contact fetcher %v: Load %v contacts", n, len(eventsByContactID))

		var contactIDs = make([]string, 0, len(eventsByContactID))
		for id := range eventsByContactID {
			contactIDs = append(contactIDs, id)
		}

		data, err := getContacts(contactIDs)
		if err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		var results []interface{}
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		if d.Decode(&results); err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		for _, c := range results {

			var contact = c.(map[string]interface{})
			var taPointer = eventsByContactID[contact["$id"].(string)]
			taPointer.Contact = contact

			chanDataSplitter <- taPointer
		}
	}

	//debugLog.Printf("Stop contact fechter %v", n)
}

var chanDataSplitter = make(chan TAPointerList)

func dataSplitter(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start database updater %v", n)

	defer wg.Done()

	for {

		pointerList, ok := <-chanDataSplitter
		if !ok {
			break
		}

		//debugLog.Printf("Splitter %v: Extract %v transactions", n, len(pointerList.Pointer))

		// Kontakt
		var contact = pointerList.Contact
		var taskLog = contact["$task_log"].([]interface{})

		chanDatabaseUpdater <- database.Entity{
			Type: "contact",
			Data: contact, // Alle überflüssigen Felder entfernen
		}

		if pointerList.Pointer != nil {

			// Pointer mitgeliefert
			for _, p := range pointerList.Pointer {

				var splits = strings.Split(p, ",")
				var tlIdx, _ = strconv.Atoi(splits[0])
				var taIdx, _ = strconv.Atoi(splits[1])

				// Transaktion
				var entry = taskLog[tlIdx].(map[string]interface{})
				var transactions = entry["transactions"].([]interface{})
				var transaction = transactions[taIdx].(map[string]interface{})

				var tid = contact["$id"].(string) + transaction["fired"].(string)
				if transaction["sequence_nr"] != nil {
					tid += transaction["sequence_nr"].(json.Number).String()
				}
				transaction["$id"] = hash(tid)
				transaction["$contact_id"] = contact["$id"].(string)

				insertTransaction(transaction)
			}
		} else {

			// Kein Pointer --> Alle Transaktionen importieren
			for _, e := range taskLog {

				// Transaktion
				var entry = e.(map[string]interface{})
				var transactions = entry["transactions"].([]interface{})
				for _, tran := range transactions {

					var transaction = tran.(map[string]interface{})

					var tid = contact["$id"].(string) + transaction["fired"].(string)
					if transaction["sequence_nr"] != nil {
						tid += transaction["sequence_nr"].(json.Number).String()
					}
					transaction["$id"] = hash(tid)
					transaction["$contact_id"] = contact["$id"].(string)

					insertTransaction(transaction)
				}
			}
		}

	}

	//debugLog.Printf("Stop database updater %v", n)
}

func insertTransaction(transaction map[string]interface{}) {

	// Connections
	var connections = transaction["connections"]
	delete(transaction, "connections")
	chanDatabaseUpdater <- database.Entity{
		Type: "transaction",
		Data: transaction,
	}

	if connections == nil {
		return
	}

	for _, con := range connections.([]interface{}) {

		var connection = con.(map[string]interface{})
		connection["$id"] = hash(transaction["$id"].(string) + connection["fired"].(string))
		connection["$transaction_id"] = transaction["$id"]

		// Recordings
		var recordings = connection["recordings"]
		delete(connection, "recordings")
		chanDatabaseUpdater <- database.Entity{
			Type: "connection",
			Data: connection,
		}

		if recordings == nil {
			continue
		}

		for _, rec := range recordings.([]interface{}) {

			var recording = rec.(map[string]interface{})
			recording["$id"] = hash(connection["$id"].(string) + recording["location"].(string))
			recording["$connection_id"] = connection["$id"]

			chanDatabaseUpdater <- database.Entity{
				Type: "recording",
				Data: recording,
			}
		}
	}
}

var chanDatabaseUpdater = make(chan database.Entity)

func databaseUpdater(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start database inserter")

	defer wg.Done()

	var counter = map[string]int{}

	for {

		entity, ok := <-chanDatabaseUpdater
		if !ok {
			break
		}

		//debugLog.Printf("DB Updater: Upsert %v", entity.Data)
		err := db.Upsert(entity)
		if err == nil {
			// Save start date if transaction was stored successfully
			if entity.Type == "transaction" {
				debugLog.Printf("Update ts: %v", entity.Data["fired"].(string))
				config.Timestamp = entity.Data["fired"].(string)
			}
			counter[entity.Type+" success"]++
		} else {
			upsertError(entity, err)
			//debugLog.Printf("%v", entity.Data)
			panic("a")
			counter[entity.Type+" failed"]++
		}
	}

	for eType, eCount := range counter {
		chanStatistics <- Statistic{
			Type:  eType,
			Count: eCount,
		}
	}

	//debugLog.Printf("Stop database inserter")
}

func upsertError(entity database.Entity, err error) {

	switch entity.Type {
	case "contact":
		errorLog.Printf("UPSERT ERROR: Contact | CONTACT ID: %v | %v\n\n", entity.Data["$id"], err.Error())
	case "transaction":
		errorLog.Printf("UPSERT ERROR: Transaction | CONTACT ID: %v | %v\n\n", entity.Data["$contact_id"], err.Error())
	case "connection":
		errorLog.Printf("UPSERT ERROR: Connection | TRANSACTION ID: %v | %v\n\n", entity.Data["$transaction_id"], err.Error())
	case "recordings":
		errorLog.Printf("UPSERT ERROR: Recording | CONNECTION ID: %v | %v\n\n", entity.Data["$connection_id"], err.Error())
	}
}

/******************************************
* TICKER FÜR ZEITINTERVALLE
*******************************************/
func ticker() {

	//debugLog.Printf("Start ticker")

	//tMin := time.NewTicker(time.Second) // Testing
	tMin := time.NewTicker(time.Second * 20)
	tHour := time.NewTicker(time.Minute * 20)
	t12Hour := time.NewTicker(time.Hour * 4)

	for {

		select {

		case <-tMin.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-1 * time.Minute),
			}

		case <-tHour.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-1 * time.Hour),
			}

		case <-t12Hour.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-12 * time.Hour),
			}

		}
	}
}

/** läuft zurück bis zum angegebenen Startdatum zurück und beendet sich anschließend**/
func reverseTicker(startDate string, wg *sync.WaitGroup) {

	//debugLog.Printf("Start reverse ticker")
	debugLog.Printf("Load all transactions after %v", startDate)

	// Parse start date
	var layout = "2006-01-02T15:04:05.999Z"[0:len(startDate)]
	timeStart, err := time.Parse(layout, startDate)
	if err != nil {
		errorLog.Printf(err.Error())
		os.Exit(1)
	}

	defer wg.Done()

	var decrement = time.Minute // Start Schrittgröße für Zurückgehen im Zeitintervall
	var nextTo = time.Now().UTC()
	var nextFrom = nextTo.Add(-1 * decrement)

	//debugLog.Printf("FROM: %v", nextFrom)
	//debugLog.Printf("TO: %v", nextTo)

loop:
	for {

		select {

		// TODO: Dynamische Timeframeanpassung überarbeiten / limitieren (ACK auf 'true')
		case fetchResult := <-chanEventFetchDone: // Flusskontrolle
			if fetchResult.Duration > 0 && fetchResult.Count > 0 {
				decrement = time.Duration((fetchResult.Duration.Seconds() / float64(fetchResult.Count)) * FETCH_SIZE_EVENTS * 1000000000) // Umrechnung in Nanosekunden
				debugLog.Printf("Decresase timeframe interval to %v", decrement)
			} else {
				// exp backoff
				decrement *= 2
				debugLog.Printf("Increase timeframe interval to %v", decrement)
			}

		default: // Nächster Zeitintervall
			if nextFrom.Before(timeStart) { // ...bis Startzeitpunkt erreicht ist
				chanEventFetcher <- TimeRange{
					From: timeStart,
					To:   nextTo,
					Ack:  false,
				}
				break loop
			}

			chanEventFetcher <- TimeRange{
				From: nextFrom,
				To:   nextTo,
				Ack:  false,
			}

			nextTo = nextFrom
			nextFrom = nextFrom.Add(-1 * decrement)
		}
	}

	//debugLog.Printf("Stop reverse ticker")
}

/******************************************
* UTILITY FUNCTIONS
*******************************************/

func hash(text string) string {
	h := md5.New()
	io.WriteString(h, text)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func createDirectory(path string) error {

	if _, err := os.Stat(path); err != nil {

		if os.IsNotExist(err) {

			err = os.MkdirAll(path, 0755)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}
