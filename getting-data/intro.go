package main

import (
	"fmt"
	"encoding/csv"
	"os"
	"io"
	"strconv"
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

func main() {
	CSVReadPerLine()
}