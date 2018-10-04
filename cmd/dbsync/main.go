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

const (
	batchSizeContactFetch = 30 // specifies the number of contacts that should be fetched in one request
	//baseURL               = "https://dev-xdot-pepperdial-xdot-com-dot-cloudstack5.appspot.com"
	baseURL               = "https://api-xdot-dialfire-xdot-com"
	tokenRawContactReader = "ts5uaUtG9QbahmeF6Qrk4tmv6Ru_uV7MHEQJJac_-Pulo3nlvGLcrvCvBAD-hZ_6azy9vUtIK6gJxrw1p1krfW3btMwIimlrh2OrO4UTKI6" // Access token for contact listing (/data/campaigns/*/contacts/) - DEV

)

var (
	l             *log.Logger
	db            *database.DBConnection
	config        Config
	campaignID    string
	campaignToken string
	mode          string // webhook | database
	whURI         string
	cntWorker     int
)

var eventOptions = map[string]string{
	"type":  "",
	"hi":    "",
	"tasks": "",
}

/******************************************
* CONFIGURATION
*******************************************/

type Config struct {
	Timestamp string `json:"timestamp"`
}

func loadConfig() {

	configFile, err := ioutil.ReadFile(os.Getenv("HOME") + "/.dbsync/" + campaignID + ".json")
	if err != nil {
		config = Config{
			Timestamp: time.Now().UTC().Format(time.RFC3339)[:19], // default: current UTC time in format "2006-01-02T15:04:05"
		}
		l.Printf("Configuration file for campaign %v not found! (will be created)", campaignID)
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
* CLEANUP TASKS (ON KILL)
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

	cid := flag.String("c", "", "Campaign ID (required)")
	token := flag.String("t", "", "Campaign API token (required)")
	workerCount := flag.Int("w", 32, "Number of simultaneous workers")
	execMode := flag.String("m", "database", "Mode (webhook ... Send all transactions to a specified URL, database ... Store all transactions in a database)")
	dateStart := flag.String("s", "", "Start date in the format '2006-01-02T15:04:05'")
	filterMode := flag.String("f", "all", "Filter transactions (all ... no filter (default) | updates_only ... only transactions of type 'update' | hi_updates_only ... only transactions of type 'update' that were triggered by a human")
	tPrefix := flag.String("p", "", "Filter transactions by task(-prefix) (comma separated), e.g. fc_,qc_")

	// Webhook Parameters
	whuri := flag.String("whuri", "", "URL of the webhook, that receives the transaction data (required, if mode=webhook) ... The webhook should return response code 200...299 on success")

	// Database Parameters
	dburi := flag.String("dburi", "", "URL of the database to be used (required, if mode=database)")
	dbms := flag.String("dbms", "mysql", "Database driver to be used (mysql, postgres, sqlserver)")
	dbmode := flag.String("dbmode", "db_sync", "Database mode (db_sync ... Synchronize all transactions starting after specified start date (CLI arg 's') | db_init ... Initialize database with all transactions within campaign, then stop | db_update ... Update database with all transactions after specified start date (CLI arg 's'), then stop)")

	flag.Parse()

	// Check required flags
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

	// Setup parameters
	if len(*tPrefix) > 0 {
		eventOptions["tasks"] = *tPrefix
	}

	if len(*filterMode) > 0 {

		switch *filterMode {

		case "all":
			// no filter
		case "updates_only":
			eventOptions["type"] = "update"
		case "hi_updates_only":
			eventOptions["type"] = "update"
			eventOptions["hi"] = "true"
		}
	}

	// Load config
	loadConfig()

	// Set start date from config file (iff not explicitly defined)
	var startDate string
	if *dateStart != "" {
		startDate = *dateStart
	} else {
		startDate = config.Timestamp
	}

	switch mode {
	case "webhook":

		if len(*whuri) == 0 {
			fmt.Fprintln(os.Stderr, "Webhook connection uri is required")
			os.Exit(1)
		}

		whURI = *whuri

		modeWebhook(startDate)
	case "database":

		// Check supported db types
		var dbValid = false
		for _, l := range []string{"mysql", "postgres", "sqlserver"} {
			if *dbms == l {
				dbValid = true
				break
			}
		}
		if !dbValid {
			fmt.Fprintf(os.Stderr, "Invalid database driver '%v'", *dbms)
			os.Exit(1)
		}

		if len(*dburi) == 0 {
			fmt.Fprintln(os.Stderr, "Database URL (CLI arg 'dburi') is required")
			os.Exit(1)
		}

		switch *dbmode {

		case "db_init":
			modeDatabaseInit(*dbms, *dburi)

		case "db_update":

			if *dateStart == "" {
				startDate = time.Now().UTC().Add(-168 * time.Hour).Format("2006-01-02") // default: -1 week, iff no start date was passed as command line argument
			}
			modeDatabaseUpdate(*dbms, *dburi, startDate)

		case "db_sync":
			modeDatabaseSync(*dbms, *dburi, startDate)
		}

	}
}

/*******************************************
* MODE: WEBHOOK
********************************************/
func modeWebhook(startDate string) {

	l.Printf("Mode: Webhook")

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
		go webhookSender(i, &wg4)
	}

	ticker()
}

func webhookSender(n int, wg *sync.WaitGroup) {

	//l.Printf("Start webhook sender %v", n)

	defer wg.Done()

	for {

		taPointer, ok := <-chanDataSplitter
		if !ok {
			break
		}

		l.Printf("Send transactions contact: %v | pointer: %v", taPointer.ContactID, taPointer.Pointer)

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

			//l.Printf("tlIdx %v", tlIdx)
			//l.Printf("taIdx %v", taIdx)
			//l.Printf("TA %v", transaction)

			var data = map[string]interface{}{
				`contact`:     contact,
				`transaction`: transaction,
				`state`:       state,
			}

			l.Printf("Send transaction contact: %v | pointer: %v", taPointer.ContactID, p)

			payload, err := json.Marshal(data)
			if err != nil {
				l.Printf(err.Error())
				continue
			}

			// TESTING
			/*
				var re = regexp.MustCompile(`\W`)
				s := re.ReplaceAllString(transaction.(map[string]interface{})["fired"].(string), ``)
				var url = whURI + taPointer.ContactID + "_" + s
			*/
			// TESTING END

			err = sendToWebhook(whURI, payload)
			if err == nil {
				// Save start date if transaction was sent successfully
				config.Timestamp = transaction.(map[string]interface{})["fired"].(string)
			} else {
				fmt.Fprintf(os.Stderr, "%v\n", err.Error())
			}
		}
	}

	//l.Printf("Stop webhook sender %v", n)
}

func sendToWebhook(url string, data []byte) error {

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

		l.Printf("[POST] %v | attempt: %v | status: %v", url, i+1, resp.Status)

		timeout := time.Second * time.Duration(math.Pow(2, float64(i)))
		time.Sleep(timeout)
	}

	defer resp.Body.Close()

	return errors.New("Webhook status " + resp.Status)
}

/*******************************************
* MODE: DATABASE INITIALIZE
********************************************/

func modeDatabaseInit(dbType string, dbURI string) {

	l.Printf("Mode: Database Initialize")

	// init database
	db = database.Open(dbType, dbURI, l)

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

	wg4.Add(1)
	go databaseUpdater(&wg4)

	// 1. Wait until all contact ids have been listed
	wg1.Wait()
	l.Println("Contact listing DONE")
	close(chanContactFetcher)

	wg2.Wait()
	l.Println("Contact fetch DONE")
	close(chanDataSplitter)

	wg3.Wait()
	l.Println("Data split DONE")
	close(chanDatabaseUpdater)

	wg4.Wait()
	l.Println("Database update DONE")

	// Close database connection
	db.DB.Close()
}

/*******************************************
* MODE: DATABASE UPDATE
********************************************/

func modeDatabaseUpdate(dbType string, dbURI string, startDate string) {

	l.Printf("Mode: Database Update starting at " + startDate)

	// init database
	db = database.Open(dbType, dbURI, l)

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

	wg5.Add(1)
	go databaseUpdater(&wg5)

	// 1. Wait until time range has been past
	wg1.Wait()
	l.Println("Iterate time range DONE")
	close(chanEventFetcher)

	wg2.Wait()
	l.Println("Event fetch DONE")
	close(chanContactFetcher)

	wg3.Wait()
	l.Println("Contact fetch DONE")
	close(chanDataSplitter)

	wg4.Wait()
	l.Println("Data split DONE")
	close(chanDatabaseUpdater)

	wg5.Wait()
	l.Println("Database update DONE")

	// Close database connection
	db.DB.Close()
}

/*******************************************
* MODE: DATABASE SYNCHRONIZATION
********************************************/

func modeDatabaseSync(dbType string, dbURI string, startDate string) {

	// TODO: Ordentlich beenden (alle worker und start datum speichern)

	l.Printf("Mode: Database Synchronize")

	// init database
	db = database.Open(dbType, dbURI, l)

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

	wg4.Add(1)
	go databaseUpdater(&wg4)

	ticker()

	// Close database connection
	db.DB.Close()
}

/*******************************************
* DIALFIRE API
********************************************/
func listContacts(cursor string) ([]byte, error) {

	url := baseURL + "/!" + tokenRawContactReader + "/data/campaigns/" + campaignID + "/contacts/?_type_=f&_limit_=" + strconv.Itoa(batchSizeContactFetch) + "&_name___GT=" + cursor

	l.Printf("List Contacts: %v\n", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
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

func getContacts(contactIDs []string) ([]byte, error) {

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
func getTransactions(params map[string]string) ([]byte, error) {

	url := baseURL + "/!" + campaignToken + "/api/campaigns/" + campaignID + "/contacts/transactions/?"

	//l.Printf("Params %v", params)

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

	//l.Printf("[GET] %v", url)

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

var chanEventFetcher = make(chan TimeRange)
var eventCache = ttlcache.NewCache(time.Minute) // Autoextend bei GET

func eventFetcher(n int, wg *sync.WaitGroup) {

	//l.Printf("Start event fechter %v", n)

	defer wg.Done()

	for {

		timeRange, ok := <-chanEventFetcher
		if !ok {
			break
		}

		l.Printf("Load transactions from: %v | to: %v", timeRange.From, timeRange.To)

		var params = map[string]string{}

		if timeRange.From != "" {
			params["from"] = timeRange.From
		}
		if timeRange.To != "" {
			params["to"] = timeRange.To
		}

		var eventCount = 0
		var eventsByContactID = map[string]TAPointerList{}
		for {

			data, err := getTransactions(params)
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

			// Event counter erhöhen
			eventCount += resp.Count

			// MD5 Prüfen
			for _, event := range resp.Results {

				var key = event.ContactID + event.Fired + event.Seqnr
				oldHash, exists := eventCache.Get(key)

				if exists && oldHash == event.MD5 {
					//l.Printf("Skip event %v (already processed)", key)
					continue
				}

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

			// Bulk in 30er Schritten
			if len(eventsByContactID) == 30 || resp.Cursor == "" {

				//l.Printf("Eventlist %v", eventsByContactID)

				if len(eventsByContactID) > 0 {
					chanContactFetcher <- eventsByContactID
					eventsByContactID = make(map[string]TAPointerList)
				}
			}

			if resp.Cursor == "" {
				// Acknoledge fetch DONE
				if timeRange.Ack {
					chanFetchDone <- eventCount
				}
				break
			}

			params["cursor"] = resp.Cursor
		}
	}

	//l.Printf("Stop event fechter %v", n)
}

type ListingResponse struct {
	Results []map[string]interface{} `json:"_results_"`
	Count   int                      `json:"_count_"`
}

func contactLister(wg *sync.WaitGroup) {

	//l.Printf("Start contact lister")

	defer wg.Done()

	var cursor string
	for {

		data, err := listContacts(cursor)
		if err != nil {
			l.Printf(err.Error())
			break
		}

		var resp ListingResponse
		if err = json.Unmarshal(data, &resp); err != nil {
			l.Printf(err.Error())
			break
		}

		var eventsByContactID = map[string]TAPointerList{}
		for _, r := range resp.Results {

			var contactID = r["_name_"].(string)
			cursor = contactID
			eventsByContactID[contactID] = TAPointerList{}
		}
		chanContactFetcher <- eventsByContactID

		if resp.Count < batchSizeContactFetch {
			break
		}
	}

	//l.Printf("Stop contact lister")
}

var chanContactFetcher = make(chan map[string]TAPointerList)

func contactFetcher(n int, wg *sync.WaitGroup) {

	//l.Printf("Start contact fechter %v", n)

	defer wg.Done()

	for {

		eventsByContactID, ok := <-chanContactFetcher
		if !ok {
			break
		}

		var contactIDs = make([]string, 0, len(eventsByContactID))
		for id := range eventsByContactID {
			contactIDs = append(contactIDs, id)
		}

		data, err := getContacts(contactIDs)
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

			chanDataSplitter <- taPointer
		}
	}

	//l.Printf("Stop contact fechter %v", n)
}

var chanDataSplitter = make(chan TAPointerList)

func dataSplitter(n int, wg *sync.WaitGroup) {

	//l.Printf("Start database updater %v", n)

	defer wg.Done()

	for {

		pointerList, ok := <-chanDataSplitter
		if !ok {
			break
		}

		// Kontakt
		var contact = pointerList.Contact
		var taskLog = contact["$task_log"].([]interface{})
		delete(contact, "$task_log")
		chanDatabaseUpdater <- Entity{
			Type: "contact",
			Data: contact,
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
				transaction["$id"] = hash(contact["$id"].(string) + transaction["fired"].(string) + transaction["sequence_nr"].(json.Number).String())
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
					transaction["$id"] = hash(contact["$id"].(string) + transaction["fired"].(string) + transaction["sequence_nr"].(json.Number).String())
					transaction["$contact_id"] = contact["$id"].(string)

					insertTransaction(transaction)
				}
			}
		}

	}

	//l.Printf("Stop database updater %v", n)
}

func insertTransaction(transaction map[string]interface{}) {

	// Connections
	var connections = transaction["connections"]
	delete(transaction, "connections")
	chanDatabaseUpdater <- Entity{
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
		chanDatabaseUpdater <- Entity{
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

			chanDatabaseUpdater <- Entity{
				Type: "recording",
				Data: recording,
			}
		}
	}
}

type Entity struct {
	Type string
	Data map[string]interface{}
}

var chanDatabaseUpdater = make(chan Entity)

func databaseUpdater(wg *sync.WaitGroup) {

	//l.Printf("Start database inserter")

	defer wg.Done()

	for {

		entity, ok := <-chanDatabaseUpdater
		if !ok {
			break
		}

		var tableName = "df_" + entity.Type + "s"
		err := db.Upsert(tableName, entity.Data)
		if err == nil {
			// Save start date if transaction was stored successfully
			if entity.Type == "transaction" {
				config.Timestamp = entity.Data["fired"].(string)
			}
		} else {
			upsertError(entity, err)
		}
	}

	//l.Printf("Stop database inserter")
}

func upsertError(entity Entity, err error) {

	switch entity.Type {
	case "contact":
		fmt.Fprintf(os.Stderr, "UPSERT ERROR | Contact | CONTACT ID: %v | %v\n", entity.Data["$id"], err.Error())
	case "transaction":
		fmt.Fprintf(os.Stderr, "UPSERT ERROR | Transaction | CONTACT ID: %v | %v\n", entity.Data["$contact_id"], err.Error())
	case "connection":
		fmt.Fprintf(os.Stderr, "UPSERT ERROR | Connection | TRANSACTION ID: %v | %v\n", entity.Data["$transaction_id"], err.Error())
	case "recordings":
		fmt.Fprintf(os.Stderr, "UPSERT ERROR | Recording | CONNECTION ID: %v | %v\n", entity.Data["$connection_id"], err.Error())
	}
}

/******************************************
* TICKER FÜR ZEITINTERVALLE
*******************************************/

type TimeRange struct {
	From string
	To   string
	Ack  bool // Indicates if acknoledgement is required
}

func ticker() {

	//l.Printf("Start ticker")

	tMin := time.NewTicker(time.Second) // Testing
	//var tMin := time.NewTicker(time.Second * 20)
	tHour := time.NewTicker(time.Minute * 20)
	t12Hour := time.NewTicker(time.Hour * 4)

	for {

		select {

		case <-tMin.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-1 * time.Minute).Format("2006-01-02T15:04:05"),
			}

		case <-tHour.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02T15:04:05"),
			}

		case <-t12Hour.C:
			chanEventFetcher <- TimeRange{
				From: time.Now().UTC().Add(-12 * time.Hour).Format("2006-01-02T15:04:05"),
			}

		}
	}
}

/** läuft zurück bis zum angegebenen Startdatum zurück und beendet sich anschließend**/
var chanFetchDone = make(chan int, 100) // Channel für DONE Message von event fetcher (liefert Anzahl d. Evens zurück)

func reverseTicker(startDate string, wg *sync.WaitGroup) {

	//l.Printf("Start reverse ticker")
	l.Printf("Load transactions beginning at %v", startDate)

	defer wg.Done()

	var nextTo = time.Now().UTC()
	var nextFrom = nextTo.Add(-1 * time.Hour)

	for {

		if nextFrom.Format("2006-01-02T15:04:05") < startDate { // ...bis Startzeitpunkt erreicht ist
			chanEventFetcher <- TimeRange{
				From: startDate,
				To:   nextTo.Format("2006-01-02T15:04:05"),
				Ack:  true,
			}
			<-chanFetchDone // Warten bis alle events gefetcht wurden
			break
		}

		chanEventFetcher <- TimeRange{
			From: nextFrom.Format("2006-01-02T15:04:05"),
			To:   nextTo.Format("2006-01-02T15:04:05"),
			Ack:  true,
		}
		eventCount := <-chanFetchDone // Warten bis alle events gefetcht wurden

		// Timeout in Abhängigkeit der Eventanzahl
		var timeout = time.Duration(eventCount) * time.Millisecond
		if timeout > 0 {
			l.Printf("Reverse ticker timeout %v", timeout)
			time.Sleep(timeout)
		}

		nextTo = nextFrom
		nextFrom = nextFrom.Add(-1 * time.Hour) // alle 10 Sekunden eine Stunde zurück gehen...
	}

	//l.Printf("Stop reverse ticker")
}

/******************************************
* UTILITY FUNCTIONS
*******************************************/

func hash(text string) string {
	h := md5.New()
	io.WriteString(h, text)
	return fmt.Sprintf("%x", h.Sum(nil))
}
