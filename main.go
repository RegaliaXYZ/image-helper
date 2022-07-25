package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type fileSet map[string]struct{}

func (f fileSet) add(file string) {
	f[file] = struct{}{}
}

func (f fileSet) contains(file string) bool {
	_, ok := f[file]
	return ok
}


func md5sum(filePath string) (string, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return "", err
    }
    defer file.Close()

    hash := md5.New()
    if _, err := io.Copy(hash, file); err != nil {
        return "", err
    }
    return hex.EncodeToString(hash.Sum(nil)), nil
}

func main() {
	DELETE := false
	DIRECTORY := "./test-data/"
	files, err := ioutil.ReadDir(DIRECTORY)
	if err != nil {
		log.Fatal(err)
	}
	hash_set := fileSet{}
	var wg sync.WaitGroup
	wg.Add(len(files))

	for _, file := range files {
		go func(file os.FileInfo) {
			defer wg.Done()
			hash, err := md5sum(DIRECTORY + file.Name())
			if err != nil {
				log.Fatal(err)
			}
			if hash_set.contains(hash) {
				fmt.Println(file.Name() + " is a duplicate")
				if DELETE {
					e := os.Remove(DIRECTORY + file.Name())
					if e != nil {
						log.Fatal(e)
					}
				}
			} else {
				hash_set.add(hash)
			}
		}(file)
	}
	wg.Wait()
}