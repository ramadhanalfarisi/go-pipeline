package main

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type FileDetail struct {
	Filename  string
	Filepath  string
	Iscreated bool
	Isfilled  bool
}

func SetFileDetail(num int) <-chan FileDetail {
	output := make(chan FileDetail)

	go func() {
		for i := 0; i < num; i++ {
			angka := i + 1
			file := FileDetail{Filename: "filename" + strconv.Itoa(angka)}
			output <- file
		}
		close(output)
	}()
	return output
}

func CreatedFile(chanFile <-chan FileDetail) <-chan FileDetail {
	output := make(chan FileDetail)
	go func(chanFile <-chan FileDetail) {
		for file := range chanFile {
			log_path := "files/" + file.Filename + ".txt"
			if _, err := os.Stat(log_path); os.IsNotExist(err) {
				f, errcreate := os.Create(log_path)
				if errcreate != nil {
					log.Println(errcreate)
				} else {
					file.Filepath = log_path
					file.Iscreated = true
				}
				f.Close()
			}
			output <- file
		}
		close(output)
	}(chanFile)
	return output
}

func MergeFile(chanFiles ...<-chan FileDetail) <-chan FileDetail {
	wg := new(sync.WaitGroup)
	output := make(chan FileDetail)
	wg.Add(len(chanFiles))

	for _, chanFile := range chanFiles {
		go func(chanFile <-chan FileDetail) {
			for file := range chanFile {
				output <- file
			}
			wg.Done()
		}(chanFile)
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

func WriteFile(chanFile <-chan FileDetail) <-chan FileDetail {
	output := make(chan FileDetail)
	go func(chanFile <-chan FileDetail) {
		for file := range chanFile {
			f, erropen := os.OpenFile(file.Filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
			if erropen != nil {
				log.Println(erropen)
			} else {
				f.WriteString("filename " + file.Filename)
				file.Isfilled = true
			}
			output <- file
			f.Close()
		}
		close(output)
	}(chanFile)
	return output
}

func main() {
	start := time.Now()
	log.Println("Start")

	files := SetFileDetail(3000)

	createdFiles1 := CreatedFile(files)
	createdFiles2 := CreatedFile(files)
	createdFiles3 := CreatedFile(files)
	createdFiles4 := CreatedFile(files)

	createdFiles := MergeFile(createdFiles1, createdFiles2, createdFiles3, createdFiles4)

	writeFiles1 := WriteFile(createdFiles)
	writeFiles2 := WriteFile(createdFiles)
	writeFiles3 := WriteFile(createdFiles)
	writeFiles4 := WriteFile(createdFiles)

	writeFiles := MergeFile(writeFiles1, writeFiles2, writeFiles3, writeFiles4)

	countCreated := 0
	countFilled := 0

	for file := range writeFiles {
		if file.Iscreated {
			countCreated++
		}
		if file.Isfilled {
			countFilled++
		}
	}
	log.Println("Files", countCreated, "created")
	log.Println("Files", countFilled, " filled")

	duration := time.Since(start)
	log.Println("done in", duration.Seconds(), "seconds")
}
