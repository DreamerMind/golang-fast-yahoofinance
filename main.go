/**
*	Retrive financial data from web service
*
*	My aim was to check the performance of this
*	language for crawling & concurrency
*
*	This program is experimental then take care
*	Based on yahoo financial web service it's have 
*	quota query restriction intensive use will end up with error message:
*	"The current table 'yahoo.finance.historicaldata' has been blocked."
*	"It exceeded the allotted quotas of either time or instructions"
*/

package main

// elegant and lightweight "import,const,var" code structure
// for multiple assignations contrary to java C# python... where
// keywords must be written each time

import (
	"fmt"
	"os"
	"io"
	"io/ioutil"
	"strconv"
	"log"
	"regexp"
	"time"
	"net/http"
	"net/url"
	"encoding/json"
)

//--------------------------------------

const (
	PATH_CONFIG = "./config.json"
	PATH_DATA   = "./to_retrive.txt"
	PATH_EXPORT = "./export_file.csv"
)
//--------------------------------------
// Global Variables
//
var (
	// instance of file configuration in config.json
	config		ConfigCrawler
	
	// all brands where string id will
	// be the supposed brand name 
	mbrands		map[string]Brand
)

//--------------------------------------
// Specifics Structs
//

// type allow to name a specific type,or struct
// look like typedef in c
type ConfigCrawler struct {
	Timeout			 		int32
		UrlBrandIdentify		string
		UrlBrandCloseHistory		string
		YqlQueryBrandCloseHistory	string
		BeginDate			string
		EndDate				string
		RegexpJsonNodeBrandId		string
		RegexpJsonNodeBrandDateAndClose	string
}

type Brand struct {
	//Acronym is the market brand identifier
	Acronym				string
	//map with day as index
	//contain market valuation at the end of day
	BrandCloseAction 	map[string]float64
}

//--------------------------------------
// Load config.json file into struct
// config

func loadConfig(addr string) {

	//golang architects create library func
	//witch often return tuples as this line
	//witch is a realy elegant way to
	//deal with error
	//
	//the := allow you to create and asign
	//variable symultaneously
	//
	//Package name are explicitly written
	//each time you acces it shortcut can
	//be created with "." before module name
	//in import section but it not recommended
	//for readability.
	//So take care to give vars names that don't
	//match it to maintain easily go program's.
	fileContent, err := os.Open(addr)
	//syntax of if statement are more expressive
	//less verbose than other languages Java likes.
	if err != nil {
		//log.Fatal will print err and break program
		log.Fatal(err)
	}
	decoder := json.NewDecoder(fileContent)
	//infinite loop
	for {
		//Here we let decoder looking for config
		//stucture in our json content
		err := decoder.Decode(&config)
		if err != nil {
			//note that json decoder will throw err in the End of File
			if err == io.EOF {
				//end the for loop
				break
			}
			log.Fatal(err)
		}
	}
	fmt.Println("====== configuration Crawling Loaded =========")
}

//--------------------------------------
// Load a file that list the brands 
// and launch an action f foreach line

func doSupposedBrand(dataUrl string,f func(string)) {
	file, err := os.Open(dataUrl)
	if err != nil {
		log.Fatal(err)
	}
	//defer is a function witch allow to
	//call stuff at the end of a function
	defer file.Close()
	for {
		name := ""
		n, err := fmt.Fscanf(file, "%s", &name)
		if err != nil{
			if err == io.EOF{
				break
			}
			log.Fatal(err)
		}
		if n != 1 {
			break
		}
		//call param function f
		//with extracted file line
		//string
		f(name)
	}
}
//--------------------------------------
// get the brand acronym given the
// supposed brand name by calling
// web services 
// c chan <- string is a parallel channel param
// from it's parent function witch is a go routine
// it's allow to directly communicate with 
// the go routine parent 

func crawlBrandAcronym(supposedBrandName string, c chan <- string)string{
		start := time.Now()
		//construct url
		url := fmt.Sprintf(config.UrlBrandIdentify,supposedBrandName)
		
		//crawl
		resp, err := http.Get(url)
	
		if err != nil{
			log.Fatal(err)
		}
		var httpBody []byte;
		if err == nil {
			httpBody, err = ioutil.ReadAll(resp.Body)
			resp.Body.Close()
		} 
	
		//regexp content
		reg, err := regexp.Compile (config.RegexpJsonNodeBrandId);
		if err != nil {
			log.Fatal(err)
		}
		output := []string (reg.FindStringSubmatch(string(httpBody)));
		
		result := ""
		
		if len(output) < 1 {
			c <- fmt.Sprintf("No brand seem to be called %s",supposedBrandName)
		} else {
			result = output[1]
		}
		//info
		c <- fmt.Sprintf("Get Acronym %s [%.2fs]\n",result,time.Since(start).Seconds())
		
		return result
}

//--------------------------------------
// get the brand close price given the
// brand market identifier and the 
// config dates
// by calling web services 

func crawlBrandClosePrice(brandAccronym string,c chan <- string) map[string]float64{
		var mBrandClosePriceDbD = make( map[string]float64 )
		start := time.Now()
		//construct url
		qParam := fmt.Sprintf(config.YqlQueryBrandCloseHistory,brandAccronym , config.BeginDate, config.EndDate)
		escapedParam, err := url.Parse(qParam)
		if err != nil {
			log.Fatal(err)
		}
		myurl := fmt.Sprint(config.UrlBrandCloseHistory+fmt.Sprint(escapedParam))
		
		//crawl
		resp, err := http.Get(myurl)
		
		//json parse BrandCloseAction
		var httpBody []byte;
		if err == nil {
			httpBody, err = ioutil.ReadAll(resp.Body)
			resp.Body.Close()
		}

		regDate, err := regexp.Compile (config.RegexpJsonNodeBrandDateAndClose);
		if err != nil {
			log.Fatal(err)
		}
		output := [][]string (regDate.FindAllStringSubmatch(string(httpBody),400));
		if len(output) < 1  {
			c <- fmt.Sprintf("no data retrived")
		} else {
			for oD := range output {
				//fmt.Println("date:", output[oD][1]," StockPriceClose:",output[oD][2])
				closePrice, _ := strconv.ParseFloat(output[oD][2], 64)
				mBrandClosePriceDbD[output[oD][1]] = closePrice
			}
		}
		
		//info
		c<- fmt.Sprintf("Get data history %s [%.2fs]",brandAccronym,time.Since(start).Seconds())
		return mBrandClosePriceDbD
}


//--------------------------------------
//	crawlingGoRoutine is a go routine
//	witch manage crawling actions
//	that can be done in paralell
//	and is responsible to 'feed'
//	the mbrands global variable
//	with the results
func crawlingGoRoutine(supposedBrandName string, c chan <- string){
	acronym := crawlBrandAcronym(supposedBrandName, c)
	if acronym != "" {
		mBrandClosePriceDbD := crawlBrandClosePrice(acronym, c)
		
		mbrands[supposedBrandName] = 
				Brand{
					acronym,
					mBrandClosePriceDbD,
				}
	}
}

//--------------------------------------
//	Manage the concurence of crawlingGoRoutine
//	and check time out
//	if time out exceeded the concurents routines
//	are stopped
//	semaphore and mutability are manage in backend
//	by compilator
func concurentCrawlStockMarket(urlData string){
	
	c := make(chan string)
	n := 0
	
	doSupposedBrand(urlData,func(supposedBrand string){
		n++
		//go allow to declare sub routine
		go crawlingGoRoutine(supposedBrand, c)
	});
	
	// Recive routine communication and check timeout
	timeout := time.After(time.Second * time.Duration(config.Timeout))
	for i:=0;i < n; i++ {
		select{
		case result := <-c:
			fmt.Println(result)
		case <- timeout:
			fmt.Print("Timed Out \n")
			return
		}
	}
	
}

//--------------------------------------
//	Get all day between begin date and end date
//	except Sunday and Monday
//	in format "yyyy-mm-dd"
func doDayRangeSorted( beginDate, endDate string,f func(string)){
	
	myBeginDate, err := time.Parse("2006-01-02",beginDate) 
	if err != nil { log.Fatal(err)}
	myEndDate, err 	:= time.Parse("2006-01-02", endDate)
	if err != nil { log.Fatal(err)}

	//check begin before end
	if myBeginDate.Before(myEndDate) {
		curDate := myBeginDate
		for {
			//no trade in weekend
			if curDate.Weekday() != 6 && curDate.Weekday() != 0 {
				f(curDate.Format("2006-01-02"))
			}
			curDate = curDate.AddDate(0,0,1)
			if curDate.After(myEndDate) {
				break
			}
		}
	} else {
		log.Fatal("Begin Date After End Date")
	}
}

//--------------------------------------
//	export data contained in mbrands
//	in a csv with nice format

func exportBrandDataCsv(addr string) {
	
	f, err := os.OpenFile(addr, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {log.Fatal(err) }

	defer f.Close()
	// "_" allow to ignore a tuple part
	_, err = f.Write([]byte(";"))
	if err != nil {log.Fatal(err) }

	// Write Brand List in first line
	var sliceBrands []string
	for index, _ := range mbrands {
		sliceBrands = append(sliceBrands,index)
		_, err = f.Write([]byte(index+"("+mbrands[index].Acronym + ");"))
		if err != nil {log.Fatal(err) }
	}
	_, err = f.Write([]byte("\n"))
	if err != nil {log.Fatal(err) }

	// For all other lines
	doDayRangeSorted(config.BeginDate, config.EndDate ,func (curDate string){
		_, err = f.Write([]byte(curDate + ";"))
		if err != nil {log.Fatal(err) }

		for brandi := range sliceBrands{
			_, err = f.Write([]byte(fmt.Sprint(mbrands[sliceBrands[brandi]].BrandCloseAction[curDate])+";"))
			if err != nil {log.Fatal(err) }
		}
		_, err = f.Write([]byte("\n"))
		if err != nil {log.Fatal(err) }
	})
}
//--------------------------------------
// The main function entry point
// of the programs

func main() {
		start := time.Now()
		loadConfig(PATH_CONFIG)
		fmt.Println("Data Downloading ...")
		concurentCrawlStockMarket(PATH_DATA)
		fmt.Println("Data Export ...")
		exportBrandDataCsv(PATH_EXPORT)
		
		fmt.Println("===============================================")
		fmt.Printf("Total perform [%.2fs]\n",time.Since(start).Seconds())
}
