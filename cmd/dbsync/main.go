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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"ttlcache"

	"bitbucket.org/modima/dbsync/internal/pkg/database"
)

const (
	DEBUG_MODE             = false // Verbose logging
	FETCH_SIZE_EVENTS      = 1000  // Number of transaction events to fetch in one step
	FETCH_SIZE_CONTACT_IDS = 1000  // Number of contact ids to fetch in one step
	FETCH_SIZE_CONTACTS    = 30    // Number of contacts to fetch in one step
	WORKER_COUNT           = 64    // Number of workers
	MAX_DB_CONNECTIONS     = 16    // Number of simultaneous database connections
	BASE_URL               = "https://api.dialfire.com"
	//BASE_URL               = "https://dev-xdot-pepperdial-xdot-com-dot-cloudstack5.appspot.com"
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
	cntDBConn     int
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

	var logger = log.New(logFile, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile)

	logger.Printf("Logfile: %v", filePath)

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
		//debugLog.Printf("Configuration file %v not found!", filePath)
	}

	json.Unmarshal(configFile, &config)
	config.Path = filePath

	debugLog.Printf("Configuration: %v", config.Path)

	return &config, nil
}

func (c *AppConfig) save() {

	jsonData, err := json.Marshal(c)
	if err != nil {
		errorLog.Printf("%v\n", err.Error())
	}

	debugLog.Printf("Save config to " + c.Path)

	ioutil.WriteFile(c.Path, jsonData, 0644)
}

/*******************************************
* teardown TASKS (ON KILL)
********************************************/
func teardown() {

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
		
Example 1: Insert all transactions that occured after the 01. February 2018 in campaign "MY_CAMPAIGN" to a local running instance of SQL Server. Filter only user interactions on contacts in tasks starting with prefix 'fc_' or 'qc_':
	./dbsync -a db_sync -fm hi_updates_only -fp 'fc_,qc_' -c MY_CAMPAIGN_ID -ct MY_CAMPAIGN_TOKEN -s 2018-02-01 -url 'sqlserver://my_user:my_password@localhost:1433/my_database'
		
Example 2: Send all future transactions in campaign "MY_CAMPAIGN" to a webservice (The webservice should accept JSON data and respnd with status code 200 ... 299 on success):
	./dbsync -a webhook -c MY_CAMPAIGN_ID -ct MY_CAMPAIGN_TOKEN -url 'https://example.com/api/transactions/'`

		fmt.Printf("\n%v\n\n", description)
		fmt.Printf("Flags:\n")
		flag.PrintDefaults()
	}

	cid := flag.String("c", "", "Campaign ID (required)")
	token := flag.String("ct", "", "Campaign API token (required)")
	workerCount := flag.Int("w", WORKER_COUNT, "Number of simultaneous workers")
	dbConnCount := flag.Int("d", MAX_DB_CONNECTIONS, "Maximum number of simultaneous database connections")
	execMode := flag.String("a", "", `Execution mode:
webhook ... Send all transactions to a webservice
db_init ... Initialize a database with all transactions of the campaign, then stop
db_update ... Update a database with all transactions after specified start date (CLI arg 's'), then stop (default start date is one week ago)
db_sync ...  Update a database with all future transactions, optionally go back to a specified start date (CLI arg 's')`)
	dateStart := flag.String("s", "", "Start date in the format '2006-01-02T15:04:05'")
	filterMode := flag.String("fm", "", `Transaction filter mode:
updates_only ... only transactions of type 'update'
hi_updates_only ... only transactions of type 'update' that were triggered by a human interaction`)
	tPrefix := flag.String("fp", "", "Filter transactions by one or several task(-prefixes) (comma separated), e.g. 'fc_,qc_'")
	URL := flag.String("url", "", `URL pointing to a webservice that handles the transaction data (if a=webhook)
DBMS Connection URL of the form '{mysql|sqlserver|postgres}://user:password@host:port/database' (if a=db_*)`)
	doProfiling := flag.Bool("p", false, `Enable profiling`)

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
	cntDBConn = *dbConnCount
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
	if DEBUG_MODE {
		debugLog = log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		debugLog, err = createLog("/var/log/dbsync/" + campaignID + "_" + mode + "_" + time.Now().Format("20060102150405") + ".log")
		if err != nil {
			//debugLog, err = createLog(os.Getenv("HOME") + "/.dbsync/logs/" + campaignID + "_" + mode + ".log")
			debugLog, err = createLog(os.Getenv("HOME") + "/.dbsync/logs/" + campaignID + "_" + mode + "_" + time.Now().Format("20060102150405") + ".log")
			if err != nil {
				panic(err)
			}
		}
	}
	errorLog = log.New(os.Stdout, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)

	// Load config
	config, err = loadConfig("/var/opt/dbsync/" + campaignID + ".json")
	if err != nil {
		config, err = loadConfig(os.Getenv("HOME") + "/.dbsync/" + campaignID + ".json")
		if err != nil {
			panic(err)
		}
	}

	// Periodically save config (every minute)
	go func() {
		t := time.NewTicker(time.Minute)
		for {
			<-t.C
			config.save()
		}
	}()

	// Start profiler
	if *doProfiling {
		go http.ListenAndServe(":8080", http.DefaultServeMux)
	}

	// Set start date from config file (iff not explicitly defined)
	var startDate string
	if *dateStart != "" {
		startDate = *dateStart
	} else {
		startDate = config.Timestamp
	}

	debugLog.Printf("Mode: %v", mode)
	debugLog.Printf("Campaign ID: %v", campaignID)
	debugLog.Printf("Start date: %v", startDate)

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

		// Configure connection pool
		db.DB.SetMaxOpenConns(cntDBConn)
		//db.DB.SetMaxIdleConns(cntDBConn) // Kann zu "packets.go:123: write tcp 127.0.0.1:60948->127.0.0.1:3306: write: broken pipe" error führen

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

	var wg1, wg2, wg3 sync.WaitGroup

	// Start worker
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		go eventFetcher(i, &wg1)
		go contactFetcher(i, &wg2)
		go webhookSender(i, url, &wg3)
	}

	// Events aus Vergangenheit laden
	if startDate != "" {
		chanEventFetcher <- TimeRange{
			From: startDate,
		}
	}

	// Runs forever
	ticker()
}

func webhookSender(n int, url string, wg *sync.WaitGroup) {

	//debugLog.Printf("Start webhook sender %v", n)

	defer wg.Done()

	for {

		taPointer, ok := <-chanDataSplitter
		if !ok {
			break
		}

		//debugLog.Printf("Send transactions contact: %v | pointer: %v", taPointer.ContactID, taPointer.Pointer)

		// Kontakt
		var contact = *taPointer.Contact
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

			var data = map[string]interface{}{
				`contact`:     contact,
				`transaction`: transaction,
				`state`:       state,
			}

			//debugLog.Printf("Send transaction contact: %v | pointer: %v", taPointer.ContactID, p)

			payload, err := json.Marshal(data)
			if err != nil {
				errorLog.Printf("%v\n", err.Error())
				continue
			}

			// TESTING
			/*
				var re = regexp.MustCompile(`\W`)
				s := re.ReplaceAllString(transaction.(map[string]interface{})["fired"].(string), ``)
				var url = url + "/" + taPointer.ContactID + "_" + s
				// TESTING END
			*/

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

	wg4.Add(cntDBConn)
	for i := 0; i < cntDBConn; i++ {
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

	// Close database connection
	if db != nil {
		db.DB.Close()
	}

	// Cleanup
	teardown()
}

/*******************************************
* MODE: DATABASE UPDATE
********************************************/

func modeDatabaseUpdate(startDate string) {

	debugLog.Printf("Mode: Database Update starting at " + startDate)

	var wg1, wg2, wg3, wg4 sync.WaitGroup

	// Start worker
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		go eventFetcher(i, &wg1)
		go contactFetcher(i, &wg2)
		go dataSplitter(i, &wg3)
	}

	// Start database updater
	wg4.Add(cntDBConn)
	for i := 0; i < cntDBConn; i++ {
		go databaseUpdater(i, &wg4)
	}

	go statisticAggregator()

	chanEventFetcher <- TimeRange{
		From:       startDate,
		To:         time.Now().UTC().Format("2006-01-02T15:04:05.999"),
		SignalDone: true,
	}

	// 1. Wait until time range has been past
	<-chanEventFetchDone
	close(chanEventFetcher)

	wg1.Wait()
	debugLog.Printf("Event fetch DONE")
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

	// Close database connection
	if db != nil {
		db.DB.Close()
	}

	// Cleanup
	teardown()
}

/*******************************************
* MODE: DATABASE SYNCHRONIZATION
********************************************/

func modeDatabaseSync(startDate string) {

	debugLog.Printf("Mode: Database Synchronize")

	var wg1, wg2, wg3, wg4 sync.WaitGroup

	// Start worker
	wg1.Add(cntWorker)
	wg2.Add(cntWorker)
	wg3.Add(cntWorker)
	for i := 0; i < cntWorker; i++ {
		go eventFetcher(i, &wg1)
		go dataSplitter(i, &wg2)
		go contactFetcher(i, &wg3)
	}

	// Start database updater
	wg4.Add(cntDBConn)
	for i := 0; i < cntDBConn; i++ {
		go databaseUpdater(i, &wg4)
	}

	// Events aus Vergangenheit laden
	if startDate != "" {
		chanEventFetcher <- TimeRange{
			From: startDate,
		}
	}

	// Runs forever
	ticker()
}

/*******************************************
* DIALFIRE API
********************************************/
func getCampaign() ([]byte, error) {

	url := BASE_URL + "/api/campaigns/" + campaignID

	//debugLog.Printf("Load contacts: %v\n", contactIDs)

	var err error
	var req *http.Request
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
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
		debugLog.Printf("[GET] %v | attempt: %v | status %v | next try in %v ", url, i, resp.Status, timeout)
		time.Sleep(timeout)
	}

	defer resp.Body.Close()

	var result []byte
	if result, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	return result, nil
}

func getContactIds(cursor string, limit int) ([]byte, error) {

	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/ids/?limit=" + strconv.Itoa(limit) + "&cursor=" + cursor

	if DEBUG_MODE {
		debugLog.Printf("[GET] %v", url)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

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
		debugLog.Printf("[GET] %v | attempt: %v | status %v | next try in %v", url, i, resp.Status, timeout)
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

	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/"

	if DEBUG_MODE {
		debugLog.Printf("[GET] %v", url)
	}

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
		debugLog.Printf("[GET] %v | attempt: %v | status %v | next try in %v\n[Payload] %v", url, i, resp.Status, timeout, contactIDs)
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
func getTransactionEvents(params map[string]string) ([]byte, error) {

	url := BASE_URL + "/api/campaigns/" + campaignID + "/contacts/transactions/?"

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

	if DEBUG_MODE {
		debugLog.Printf("[GET] %v", url)
	}

	var req *http.Request
	var err error
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+campaignToken)

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
		debugLog.Printf("[GET] %v | attempt: %v | status %v | next try in %v ", url, i, resp.Status, timeout)
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

/*
type FetchResult struct {
	Count   int
	Results []struct {
		Fired     string `json:"fired"`
		Seqnr     string `json:"seqnr"`
		Type      string `json:"type"`
		HI        string `json:"hi"`
		Task      string `json:"task"`
		Pointer   string `json:"pointer"`
		MD5       string `json:"md5"`
		ContactID string `json:"contact_id"`
	}
	Cursor string
}
*/
type FetchResult struct {
	Count   int      `json:"count"`
	Results []string `json:"results"`
	Cursor  string   `json:"cursor"`
}

type TAPointerList struct {
	ContactID string
	Contact   *map[string]interface{}
	Pointer   []string
}

type TimeRange struct {
	From       string
	To         string
	SignalDone bool // Signal that all events have been fetched
}

var chanEventFetcher = make(chan TimeRange)
var chanEventFetchDone = make(chan int)             // Returns number of fetched events (if TimeRange.SignalDone==true)
var eventCache = ttlcache.NewCache(2 * time.Minute) // (2 Minuten) Autoextend bei GET

func eventFetcher(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start event fechter %v", n)

	defer wg.Done()

	for {

		timeRange, ok := <-chanEventFetcher
		if !ok {
			break
		}

		//debugLog.Printf("Event fetcher %v: %v", n, timeRange)

		var params = map[string]string{
			"from": timeRange.From,
		}

		if timeRange.To != "" {
			params["to"] = timeRange.To
		}

		var timeout = time.Second * 10 // Aktuelles timeout zwischen zwei Abfragen --> Langsam skalieren
		var newEventsCurPage = 0
		var newEventsTotal = 0
		var eventsByContactID = map[string]TAPointerList{}
		for {
			// Transaktionen laden
			data, err := getTransactionEvents(params)
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

			var fired string
			for _, event := range resp.Results {

				// "2018-10-17T08:07:46.468Z0217|cf44c921a79577858dea5a5b89e9f219|6EU52ECUGEJPHEJV|6,166"
				var splits = strings.Split(event, "|")
				fired = splits[0]
				var md5 = splits[1]
				var contactID = splits[2]
				var pointer = splits[3]

				// MD5 Prüfung
				var key = fired + contactID
				oldHash, exists := eventCache.Get(key)
				if exists && oldHash == md5 {
					continue
				}

				// Event counter erhöhen
				newEventsCurPage++

				if !exists {
					pointer += ",new" // new event
				} else {
					pointer += ",updated" // updated event
				}

				if eventsByContactID[contactID].ContactID == "" {
					eventsByContactID[contactID] = TAPointerList{
						ContactID: contactID,
						Pointer:   []string{pointer},
					}
				} else {
					var pList = eventsByContactID[contactID]
					pList.Pointer = append(pList.Pointer, pointer)
					eventsByContactID[contactID] = pList
				}
				eventCache.Set(key, md5)

				// Chunkweises holen der Kontakte
				if len(eventsByContactID) >= FETCH_SIZE_CONTACTS {
					//debugLog.Printf("Event fetcher %v: %v transactions | %v contacts", n, eventCount, len(eventsByContactID))
					chanContactFetcher <- eventsByContactID
					eventsByContactID = make(map[string]TAPointerList)
				}
			}

			if resp.Cursor != "" {
				params["cursor"] = resp.Cursor

				// Request throttling (falls 75% neue Events)
				if newEventsCurPage > FETCH_SIZE_EVENTS*.75 {
					debugLog.Printf("Event fetcher %v: %v events | from: %v | to: %v | current: %v | (sleep %v)", n, newEventsTotal, params["from"], params["to"], fired, timeout)
					time.Sleep(timeout)
					if timeout > time.Second {
						timeout -= timeout / 10 // 10 % verringern
					} else {
						timeout = time.Second
					}
				} else {
					debugLog.Printf("Event fetcher %v: %v events | from: %v | to: %v | current: %v", n, newEventsTotal, params["from"], params["to"], fired)
				}

			} else {

				debugLog.Printf("Event fetcher %v: %v events | from: %v | to: %v", n, newEventsTotal, params["from"], params["to"])

				// Letzter chunk
				if len(eventsByContactID) > 0 {
					chanContactFetcher <- eventsByContactID
				}

				if timeRange.SignalDone {
					chanEventFetchDone <- newEventsTotal
				}
				break
			}

			newEventsTotal += newEventsCurPage
			newEventsCurPage = 0
		}
	}

	//debugLog.Printf("Stop event fechter %v", n)
}

/*******************************************
* Importstatistik
*******************************************/
type Statistic struct {
	Type  string
	Count uint
}

var chanStatistics = make(chan Statistic)
var chanDone = make(chan bool)

func statisticAggregator() {

	var start = time.Now()
	var statistics = make(map[string]uint)

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
	debugLog.Printf("duration: %v", time.Since(start))
	chanDone <- true
}

func contactLister(wg *sync.WaitGroup) {

	//debugLog.Printf("Start contact lister")

	defer wg.Done()

	var timeout = time.Second * 10 // Aktuelles timeout zwischen zwei Abfragen --> Langsam skalieren
	var limit = FETCH_SIZE_CONTACT_IDS
	var cursor string
	var contactsTotal = 0
	for {

		data, err := getContactIds(cursor, limit)
		if err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		var resp FetchResult
		if err = json.Unmarshal(data, &resp); err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		var eventsByContactID = map[string]TAPointerList{}
		for _, contactID := range resp.Results {
			eventsByContactID[contactID] = TAPointerList{}
			contactsTotal++

			// Chunkweises holen der Kontakte
			if len(eventsByContactID) >= FETCH_SIZE_CONTACTS {
				chanContactFetcher <- eventsByContactID
				eventsByContactID = make(map[string]TAPointerList)
			}
		}

		if resp.Cursor != "" {
			cursor = resp.Cursor

			// Request throttling
			debugLog.Printf("Contact lister: %v contacts (sleep %v)", contactsTotal, timeout)
			time.Sleep(timeout)
			if timeout > time.Second {
				timeout -= timeout / 10 // 10 % verringern
			} else {
				timeout = time.Second
			}
		} else {

			debugLog.Printf("Contact lister: %v contacts", contactsTotal)

			// Letzter chunk
			if len(eventsByContactID) > 0 {
				chanContactFetcher <- eventsByContactID
			}

			break
		}
	}

	//debugLog.Printf("Stop contact lister")
}

var chanContactFetcher = make(chan map[string]TAPointerList)

func contactFetcher(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start contact fechter %v", n)

	defer wg.Done()

	for {

		eventsByContactID, ok := <-chanContactFetcher
		if !ok {
			break
		}

		//debugLog.Printf("Contact fetcher %v: Load %v contacts", n, len(eventsByContactID))

		var contactIDs = make([]string, 0, len(eventsByContactID))
		for id := range eventsByContactID {
			contactIDs = append(contactIDs, id)
		}

		data, err := getContacts(contactIDs)
		if err != nil {
			errorLog.Printf("%v\n", err.Error())
			break
		}

		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		// read "["
		_, err = dec.Token()
		if err != nil {
			log.Fatal(err)
		}

		// while the array contains contacts
		for dec.More() {

			// decode one contact
			var contact map[string]interface{}
			err := dec.Decode(&contact)
			if err != nil {
				log.Fatal(err)
			}

			// send to splitter
			var taPointer = eventsByContactID[contact["$id"].(string)]
			//taPointer.ContactData = data
			taPointer.Contact = &contact
			chanDataSplitter <- taPointer
		}

		// read "]"
		_, err = dec.Token()
		if err != nil {
			log.Fatal(err)
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
		var contact = *pointerList.Contact
		var taskLog = contact["$task_log"].([]interface{})

		chanDatabaseUpdater <- database.Entity{
			Type: "contact",
			Data: &contact, // Alle überflüssigen Felder entfernen
		}

		if pointerList.Pointer != nil {

			// Pointer mitgeliefert
			for _, p := range pointerList.Pointer {

				var splits = strings.Split(p, ",")
				var tlIdx, _ = strconv.Atoi(splits[0])
				var taIdx, _ = strconv.Atoi(splits[1])

				if tlIdx > len(taskLog)-1 {
					errorLog.Printf("Tasklog pointer out of range | Contact %v | Index %v\n", pointerList.ContactID, tlIdx)
					continue
				}

				// Transaktion
				var entry = taskLog[tlIdx].(map[string]interface{})
				var transactions = entry["transactions"].([]interface{})

				if taIdx > len(transactions)-1 {
					errorLog.Printf("Transaction pointer out of range | Contact %v | Pointer %v\n", pointerList.ContactID, taIdx)
					continue
				}

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
		Data: &transaction,
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
			Data: &connection,
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
				Data: &recording,
			}
		}
	}
}

var chanDatabaseUpdater = make(chan database.Entity)

func databaseUpdater(n int, wg *sync.WaitGroup) {

	//debugLog.Printf("Start database inserter")

	defer wg.Done()

	var counter = map[string]uint{}

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
				//debugLog.Printf("Update ts: %v", entity.Data["fired"].(string))
				config.Timestamp = (*entity.Data)["fired"].(string)
			}
			counter[entity.Type+" success"]++
		} else {
			upsertError(entity, err)
			//debugLog.Printf("%v", entity.Data)
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

	if DEBUG_MODE {
		switch entity.Type {
		case "contact":
			errorLog.Printf("UPSERT ERROR: Contact | CONTACT ID: %v | %v\nDATA: %v\n\n", (*entity.Data)["$id"], err.Error(), entity.Data)
		case "transaction":
			errorLog.Printf("UPSERT ERROR: Transaction | CONTACT ID: %v | %v\nDATA: %v\n\n", (*entity.Data)["$contact_id"], err.Error(), entity.Data)
		case "connection":
			errorLog.Printf("UPSERT ERROR: Connection | TRANSACTION ID: %v | %v\nDATA: %v\n\n", (*entity.Data)["$transaction_id"], err.Error(), entity.Data)
		case "recordings":
			errorLog.Printf("UPSERT ERROR: Recording | CONNECTION ID: %v | %v\nDATA: %v\n\n", (*entity.Data)["$connection_id"], err.Error(), entity.Data)
		}
	} else {
		switch entity.Type {
		case "contact":
			errorLog.Printf("UPSERT ERROR: Contact | CONTACT ID: %v | %v\n\n", (*entity.Data)["$id"], err.Error())
		case "transaction":
			errorLog.Printf("UPSERT ERROR: Transaction | CONTACT ID: %v | %v\n\n", (*entity.Data)["$contact_id"], err.Error())
		case "connection":
			errorLog.Printf("UPSERT ERROR: Connection | TRANSACTION ID: %v | %v\n\n", (*entity.Data)["$transaction_id"], err.Error())
		case "recordings":
			errorLog.Printf("UPSERT ERROR: Recording | CONNECTION ID: %v | %v\n\n", (*entity.Data)["$connection_id"], err.Error())
		}
	}
}

/******************************************
* TICKER FÜR ZEITINTERVALLE
*******************************************/
func ticker() {

	//debugLog.Printf("Start ticker")

	tMin := time.NewTicker(time.Minute)
	//tHour := time.NewTicker(time.Hour)
	//t12Hour := time.NewTicker(12 * time.Hour)

	for {

		var now = time.Now().UTC()

		select {

		case <-tMin.C:
			chanEventFetcher <- TimeRange{
				From: now.Add(-2 * time.Minute).Format("2006-01-02T15:04:05.999"),
				To:   now.Format("2006-01-02T15:04:05.999"),
			}

			/*
				case <-tHour.C:
					chanEventFetcher <- TimeRange{
						From: now.Add(-2 * time.Hour).Format("2006-01-02T15:04:05.999"),
						To:   now.Format("2006-01-02T15:04:05.999"),
					}

				case <-t12Hour.C:
					chanEventFetcher <- TimeRange{
						From: time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02T15:04:05.999"),
						To:   now.Format("2006-01-02T15:04:05.999"),
					}
			*/
		}
	}
}

/** läuft zurück bis zum angegebenen Startdatum zurück und beendet sich anschließend**/
/*
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

	var curDuration = time.Minute // aktuelle Schrittgröße für Zurückgehen im Zeitintervall
	var nextTo = time.Now().UTC()
	var nextFrom = nextTo.Add(-1 * curDuration)

	//debugLog.Printf("FROM: %v", nextFrom)
	//debugLog.Printf("TO: %v", nextTo)

loop:
	for {

		select {

		// Dynamische Anpassung des Zeitintervalls (zwischen 1s und 1min)
		// Ziel: Anzahl d. gefetchten Events möglichst nah an FETCH_SIZE_EVENTS pro Zeitinterval
		case eventsFetched := <-chanEventFetchDone:
			if eventsFetched == 0 {
				curDuration += (curDuration / 100).Truncate(time.Millisecond) // Keine Events in Zeitraum --> Zeitspanne um 1% vergrößern
				if curDuration > time.Minute {
					curDuration = time.Minute
				} else {
					//debugLog.Printf("ticker: Increase time interval: %v", curDuration)
				}
			} else {
				var diff = fetchResult.Count - FETCH_SIZE_EVENTS // Überhängende Events
				if diff > 0 {
					curDuration -= (curDuration / 100 * time.Duration(diff/FETCH_SIZE_EVENTS)).Truncate(time.Millisecond)
					if curDuration < time.Second {
						curDuration = time.Second
					} else {
						//debugLog.Printf("ticker: Decrease time interval: %v", curDuration)
					}
				}
			}

		default: // Nächster Zeitintervall
			if nextFrom.Before(timeStart) { // ...bis Startzeitpunkt erreicht ist
				chanEventFetcher <- TimeRange{
					From: timeStart,
					To:   nextTo,
					Ack:  true,
				}
				break loop
			}

			chanEventFetcher <- TimeRange{
				From: nextFrom,
				To:   nextTo,
				Ack:  true,
			}

			nextTo = nextFrom
			nextFrom = nextFrom.Add(-1 * curDuration)
		}
	}

	//debugLog.Printf("Stop reverse ticker")
}
*/

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
