package main

import (
	"fmt"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"os"
	"io"
	"io/ioutil"
	"strconv"
	"time"
	"log"
	"github.com/kniren/gota/dataframe"
	"github.com/patrickmn/go-cache"
	"github.com/boltdb/bolt"
)

/**
	* Note: When gathering data
	* 1. Check and enforce expected types
	* 2. Create a standard method for interacting with data. (Standard way to communicate with DB, text files etc.)
	* 3. Version your data
*/

func CSVReadAll(){
	f, err := os.Open("./data/iris.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	//var rawCSVData [][]string
	rawCSVData, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(rawCSVData)
}

func CSVReadPerLine(){
	f, err := os.Open("./data/iris.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = 5
	type Iris struct {
		SL float64
		SW float64
		PL float64
		PW float64
		Sps string
		PErr error
	}
	var IrisData []Iris
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		//ensure the required number of fields are contained if not do not append
		if err != nil {
			fmt.Println(err)
			continue
		}
		irisRecord := &Iris{}
		//ensure that the data types are valid
		for i, val := range record {
			if i == 4 {
				//ensure that the specie name is not an empty string
				if val == "" {
					fmt.Printf("Unexpected type in column %d\n", i)
					irisRecord.PErr = fmt.Errorf("Empty string value")
					break
				}
				// Add the string value to the CSVRecord.
				irisRecord.Sps = val
				continue
			}
			
			var length float64
			if length, err = strconv.ParseFloat(val, 64); err != nil {
				fmt.Printf("Unexpected type in column %d\n", i)
				irisRecord.PErr = fmt.Errorf("Could not parse float")
				break
			}
			// Add the float value to the respective field in the CSVRecord.
			switch i {
			case 0:
				irisRecord.SL = length
			case 1:
				irisRecord.SW = length
			case 2:
				irisRecord.PL = length
			case 3:
				irisRecord.PW = length
			}
		}

		if irisRecord.PErr == nil {
			IrisData = append(IrisData, *irisRecord)
		}
	}
	fmt.Println(IrisData)
}

func CSVManipulation(){
	f, err := os.Open("./data/iris.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	irisDF := dataframe.ReadCSV(f)
	fmt.Println(irisDF)
}

func JSONProcessing(){
	const citiBikeURL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
	type Station struct {
		ID string `json:"station_id"`
		NumBikesAvailable int `json:"num_bikes_available"`
		NumBikesDisabled int `json:"num_bike_disabled"`
		NumDocksAvailable int `json:"num_docks_available"`
		NumDocksDisabled int `json:"num_docks_disabled"`
		IsInstalled int `json:"is_installed"`
		IsRenting int `json:"is_renting"`
		IsReturning int `json:"is_returning"`
		LastReported int `json:"last_reported"`
		HasAvailableKeys bool `json:"eightd_has_available_keys"`
	}
	type Response struct {
		LastUpdated int `json:"last_updated"`
		TTL int `json:"ttl"`
		Data struct { 
			Stations []Station `json:"stations"`
		} `json:"data"`
	}

	response, err := http.Get(citiBikeURL)
	if err != nil {
		fmt.Println(err)
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
	}
	var sd Response
	if err := json.Unmarshal(body, &sd); err != nil {
		fmt.Println(err)
	}
	// Print the first station.
	fmt.Printf("%+v\n\n", sd.Data.Stations[0])
	//save the data to a file
	outputData, err := json.Marshal(sd)
	if err != nil {
		fmt.Println(err)
	}
	// Save the marshalled data to a file.
	if err := ioutil.WriteFile("data/citibike.json", outputData, 0644); err != nil {
		fmt.Println(err)
	}
}

func InMemoryCache() {
	c := cache.New(5*time.Minute, 30*time.Second)
	// Put a key and value into the cache.
	c.Set("tut-key", "go tutorial", cache.DefaultExpiration)
	v, found := c.Get("tut-key")
	if found {
		fmt.Printf("key: tut-key, value: %s\n", v)
	}
}

func DiskCache() {
	db, err := bolt.Open("data/tutorial.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// Create a "bucket" in the boltdb file for our data.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Output the keys and values in the embedded
	// BoltDB file to standard out.
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key: %s, value: %s\n", k, v)
		}
		return nil
		}); err != nil {
		log.Fatal(err)
	}
}

func main() {
	JSONProcessing()
	CSVManipulation()
	CSVReadPerLine()
	InMemoryCache()
	DiskCache()
}