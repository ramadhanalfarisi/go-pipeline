package main

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Genre struct {
	Id int `json:"id"`
	Name string `json:"name"`
}

type Companies struct {
	Id int `json:"id"`
	Name string `json:"name"`
}

type Movies struct {
	Adult bool `json:"adult"`
	Budget int `json:"budget"`
	Genres []Genre `json:"genres"`
	Id int `json:"id"`
	OriginalLanguage string `json:"original_language"`
	OriginalTitle string `json:"original_title"`
	Popularity float64 `json:"popularity"`
	ProductionCompanies []Companies `json:"production_companies"`
	ReleaseDate time.Time `json:"release_date"`
	Revenue float64 `json:"revenue"`
	Title string `json:"title"`
}


func getDataCSV() <-chan Movies {
	var moviesChan = make(chan Movies)
	file, err := os.Open("movies_metadata.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	go func(){
		for i, record := range records {
			if i > 0 &&  len(record) > 20 {
				movies := Movies{}
				movies.Adult, err = strconv.ParseBool(record[0])
				if err != nil {
					log.Fatal(err)
				}
				movies.Budget, err = strconv.Atoi(record[2])
				if err != nil {
					log.Fatal(err)
				}
				var genres []Genre
				genreString := strings.ReplaceAll(record[3], "'", "\"")
				err = json.Unmarshal([]byte(genreString), &genres)
				if err != nil {
					log.Fatal(err)
				}
				movies.Genres = genres
				movies.Id, err = strconv.Atoi(record[5])
				if err != nil {
					log.Fatal(err)
				} 
				movies.OriginalLanguage = record[7]
				movies.OriginalTitle = record[8]
				movies.Popularity, err = strconv.ParseFloat(record[10], 64)
				if err != nil {
					log.Fatal(err)
				}
				var companies []Companies
				companyString := strings.ReplaceAll(record[12], "\"", "'")
				companyString = strings.ReplaceAll(companyString, "\\", "")
				companyString = strings.ReplaceAll(companyString, "{'", "{\"")
				companyString = strings.ReplaceAll(companyString, "': '", "\": \"")
				companyString = strings.ReplaceAll(companyString, "', '", "\", \"")
				companyString = strings.ReplaceAll(companyString, "':", "\":")
				companyString = strings.ReplaceAll(companyString, "'", "")
				err = json.Unmarshal([]byte(companyString), &companies)
				if err != nil {
					log.Println(record[12])
					log.Fatal(err)
				}
				movies.ProductionCompanies = companies
				if record[14] == "" {
					movies.ReleaseDate = time.Time{}
				}else {
					movies.ReleaseDate, err = time.Parse("2006-01-02", record[14])
					if err != nil {
						log.Fatal(err)
					}
				}
				movies.Revenue, err = strconv.ParseFloat(record[15], 64)
				if err != nil {
					log.Fatal(err)
				}
				movies.Title = record[20]
				moviesChan <- movies
			}
		}
		close(moviesChan)
	}()
	return moviesChan
}

func searchTitle(moviesChan <-chan Movies, title string) <-chan Movies {
	var moviesResult = make(chan Movies)
	go func() {
		for movie := range moviesChan {
			if strings.Contains(movie.Title, title) {
				moviesResult <- movie
			}
		}
		close(moviesResult)
	}()
	return moviesResult
}

func searchGenre(moviesChan <-chan Movies, genre string) <-chan Movies {
	var moviesResult = make(chan Movies)
	go func() {
		for movie := range moviesChan {
			for _, g := range movie.Genres {
				if g.Name == genre {
					moviesResult <- movie
				}
			}
		}
		close(moviesResult)
	}()
	return moviesResult
}

func mergeMovies(moviesChan ...<- chan Movies) <-chan Movies {
	var moviesResult = make(chan Movies)
	var wg sync.WaitGroup
	for _, movies := range moviesChan {
		wg.Add(1)
		go func(movies <-chan Movies) {
			for movie := range movies {
				moviesResult <- movie
			}
			wg.Done()
		}(movies)
	}
	go func() {
		wg.Wait()
		close(moviesResult)
	}()
	return moviesResult
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	moviesChan := getDataCSV()

	moviesSearchTitle := searchTitle(moviesChan, "The")
	moviesSearchTitle2 := searchTitle(moviesChan, "The")
	moviesSearchTitle3 := searchTitle(moviesChan, "The")

	moviesResult := mergeMovies(moviesSearchTitle, moviesSearchTitle2, moviesSearchTitle3)

	moviesSearchGenre := searchGenre(moviesResult, "Action")
	moviesSearchGenre2 := searchGenre(moviesResult, "Action")
	moviesSearchGenre3 := searchGenre(moviesResult, "Action")

	moviesResult = mergeMovies(moviesSearchGenre, moviesSearchGenre2, moviesSearchGenre3)

	var result []Movies
	for movie := range moviesResult {
		result = append(result, movie)
	}
	log.Println(len(result))
}