package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bitbucket.org/modima/dbsync/internal/pkg/database"
	cache "github.com/patrickmn/go-cache"
	"github.com/tidwall/gjson"
)

const baseURL = "https://dev-xdot-pepperdial-xdot-com-dot-cloudstack5.appspot.com"

//const baseURL = "https://api-xdot-dialfire-xdot-com"

var chanInsertTransaction = make(chan database.Transaction, 100)
var chanInsertContact = make(chan database.Contact, 100)

var (
	campaignID    string
	campaignToken string
	config        Config
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
		fmt.Fprintf(os.Stdout, "Load config: %v\n", config.NextRead)
		saveConfig()
	} else {
		fmt.Fprintf(os.Stdout, "Config loaded. Start read: %v\n", config.NextRead)
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
		log.Fatal(err)
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
* * * * * * * * DIALFIRE API * * * * * * * *
********************************************/
var contactCache = cache.New(time.Hour, 2*time.Hour)

func getContact(campaignToken string, campaignID string, contactID string) []byte {

	//fmt.Fprintf(os.Stdout, "Load contact start %v\n", contactID)

	contact, found := contactCache.Get(campaignID + contactID)
	if found {
		//fmt.Fprintf(os.Stdout, "contact (from cache): %v\n", contact)
		result, err := json.Marshal(contact)
		if err != nil {
			fmt.Fprintf(os.Stderr, "getContact error: %v\n", err.Error())
		} else {
			return result
		}
	}

	url := baseURL + "/!" + campaignToken + "/api/campaigns/" + campaignID + "/contacts/" + contactID + "/flat_view"

	var err error
	var req *http.Request
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			break
		}

		if err != nil {
			log.Printf(err.Error())
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
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	fmt.Sprintf("Load contact %v\n", string(result))

	err = json.Unmarshal(result, &contact)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	contactCache.Set(campaignID+contactID, contact, cache.DefaultExpiration)

	return result
}

// Parameters: from string, to string, cursor string
func getTransactions(campaignToken string, campaignID string, params map[string]string) []byte {

	url := baseURL + "/!" + campaignToken + "/api/campaigns/" + campaignID + "/contacts/transactions/?"
	for k, v := range params {
		url += k + "=" + v + "&"
	}

	fmt.Fprintf(os.Stdout, "Get Transactions: %v\n", url)

	var err error
	var req *http.Request
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	var resp *http.Response
	for i := 0; i < 10; i++ {

		if resp, err = http.DefaultClient.Do(req); err == nil && resp.StatusCode == 200 {
			break
		}

		if err != nil {
			log.Printf(err.Error())
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
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	return result
}

/*******************************************
* * * * * * * * * * WORKER * * * * * * * * *
*******************************************/
var transactionCache = cache.New(time.Hour, 2*time.Hour)

func fetcher(interval time.Duration, wg *sync.WaitGroup) {

	defer wg.Done()
	tick := time.Tick(interval)
	fmt.Fprintf(os.Stdout, "START TA fetcher\n")

	for {
		<-tick

		var params = make(map[string]string)
		if len(config.NextRead) > 0 {
			params["from"] = config.NextRead
		}

		data := getTransactions(campaignToken, campaignID, params)

		// Transaktionen iterieren
		results := gjson.GetBytes(data, "results")
		results.ForEach(func(key, value gjson.Result) bool {

			var ta database.Transaction
			err := json.Unmarshal([]byte(value.Raw), &ta)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
			}

			// Nur importieren, falls neue Transaktion
			var taKey = ta.ContactID + ta.Fired
			_, found := transactionCache.Get(taKey)
			if !found {
				chanInsertTransaction <- ta
				transactionCache.Set(taKey, taKey, cache.DefaultExpiration)
			}

			return true // keep iterating
		})

		// Calculate next request date
		to := gjson.GetBytes(data, "to").String()
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
		}

		var nextRead = t.Add(time.Duration(-1) * time.Minute)
		config.NextRead = nextRead.Format(time.RFC3339)

		fmt.Fprintf(os.Stdout, "Next timestamp: %v\n", config.NextRead)
	}

	fmt.Fprintln(os.Stdout, "STOP TA fetcher\n")
}

func inserter(n int, wg *sync.WaitGroup) {

	defer wg.Done()
	fmt.Fprintf(os.Stdout, "START inserter %v\n", n)

	for {
		transaction := <-chanInsertTransaction

		// insert contact
		data := getContact(campaignToken, campaignID, transaction.ContactID)
		var contact database.Contact
		json.Unmarshal(data, &contact)
		db.UpsertContact(campaignID, contact)

		// insert transaction
		db.UpsertTransaction(campaignID, transaction)
	}
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "%s", os.Args[0])
		fmt.Fprintln(os.Stderr, "Usage of %s:", os.Args[0])
		flag.PrintDefaults()
	}

	cid := flag.String("c", "", "Campaign ID")
	token := flag.String("t", "", "Campaign token")
	dbType := flag.String("type", "mysql", "The database driver to be used (mysql, postgres or mssql)")
	dbURI := flag.String("uri", "", "The database connection uri to be used")
	workerCount := flag.Int("w", 32, "Number of simultaneous workers")

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

	if len(*dbType) == 0 {
		fmt.Fprintln(os.Stderr, "Database driver (-db) is required")
		os.Exit(1)
	}

	// Check supported db types
	var dbValid = false
	var drivers = []string{"mysql", "postgres", "mssql"}
	for _, l := range drivers {
		if *dbType == l {
			dbValid = true
			break
		}
	}
	if !dbValid {
		fmt.Fprintln(os.Stderr, "Invalid database driver "+*dbType)
		os.Exit(1)
	}

	if len(*dbURI) == 0 {
		fmt.Fprintln(os.Stderr, "Database connection uri (-uri) is required")
		os.Exit(1)
	}

	// load config
	loadConfig()

	// init database
	db = database.Open(*dbType, *dbURI)

	// Wait group
	var wg sync.WaitGroup
	wg.Add(1)
	go fetcher(time.Second, &wg)

	for i := 0; i < *workerCount; i++ {
		wg.Add(1)
		go inserter(i, &wg)
	}

	wg.Wait()
}

/*
func initConfig(campaignToken string, campaignID string) {

	err := loadConfig()
	if err != nil {
		config = &Config{Campaigns: make(map[string]CampaignConfig)}
		config.Campaigns[campaignID] = CampaignConfig{
			Token: campaignToken,
		}
		saveConfig(config)
	}

	if config.Campaigns == nil {
		config.Campaigns = make(map[string]CampaignConfig)
	}

	campaign := config.Campaigns[campaignID]
	if campaign == (CampaignConfig{}) {
		campaign = CampaignConfig{
			Token:    campaignToken,
			NextRead: time.Now().Format(time.RFC3339)[:7], // current month
		}
		config.Campaigns[campaignID] = campaign
	} else {
		campaign.Token = campaignToken
		campaign.NextRead = time.Now().Format(time.RFC3339)[:7] // current month
		config.Campaigns[campaignID] = campaign
	}

	saveConfig(config)
}
*/
