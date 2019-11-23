package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type FileID struct {
	path    string
	md5     string
	size    int64
	modTime time.Time
}

// two type of goroutines, digestFunc and compareFunc.
// digestFunc do md5sum then send file to compareFunc.
// compareFunc store md5 and file list.
// after all digest and compare tasks done, keep the oldest file, remove the others.
func main() {
	var rootPath string
	var dryRun bool
	var workerCount int
	var verbose bool

	flag.StringVar(&rootPath, "p", ".", "the directory to delete duplicate files. default is current directory")
	flag.BoolVar(&dryRun, "dry", true, "dry run just prints files are going to be removed, default is true")
	flag.IntVar(&workerCount, "count", 128, "the goroutine count to do md5 sum, default 128")
	flag.BoolVar(&verbose, "verbose", false, "print more detail")
	flag.Parse()

	if dryRun {
		log.Printf("dry run mode, not really delete files")
	}
	digestCh := make(chan *FileID)
	compareCh := make(chan *FileID)
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	digestFunc := func(group *sync.WaitGroup, ch <-chan *FileID, out chan<- *FileID) {
		defer group.Done()

		for {
			f, ok := <-ch
			if !ok {
				return
			}

			if verbose {
				log.Printf("md5sum %s", f.path)
			}

			file, err := os.Open(f.path)
			if err != nil {
				log.Printf("open file error: %v", err)
				continue
			}

			digest := md5.New()
			bytes, err := io.Copy(digest, file)
			if err != nil {
				log.Printf("read file: %s error: %v", f.path, err)
				continue
			}
			if bytes != f.size {
				log.Printf("%s read %d bytes, but file size is: %d", f.path, bytes, f.size)
			}
			f.md5 = fmt.Sprintf("%x", digest.Sum(nil))
			file.Close()

			out <- f
		}
	}

	dupFiles := make(map[string][]*FileID)

	compareFuc := func(group *sync.WaitGroup, ch <-chan *FileID) {
		defer group.Done()
		for {
			f, ok := <-ch
			if !ok {
				return
			}

			dupFiles[f.md5] = append(dupFiles[f.md5], f)
		}
	}

	for i := 0; i < workerCount; i++ {
		wg1.Add(1)
		go digestFunc(&wg1, digestCh, compareCh)
	}

	wg2.Add(1)
	go compareFuc(&wg2, compareCh)

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			digestCh <- &FileID{path: path, size: info.Size(), modTime: info.ModTime()}
		}
		return nil
	})

	if err != nil {
		log.Printf("walk error: %v", err)
		return
	}
	close(digestCh)
	wg1.Wait()
	if verbose {
		log.Printf("digest done")
	}

	close(compareCh)
	wg2.Wait()
	if verbose {
		log.Printf("compare done")
	}

	for md5sum, files := range dupFiles {
		if len(files) == 1 {
			continue
		}
		// old file go first
		sort.SliceStable(files, func(i, j int) bool {
			return files[i].modTime.After(files[j].modTime)
		})

		for i := 0; i < len(files)-1; i++ {
			file := files[i]
			if !dryRun {
				if err := os.Remove(file.path); err != nil {
					log.Printf("delete error md5: %s, file: %s err: %v", md5sum, file.path, err)
				}
			}
			log.Printf("delete md5: %s, file: %s dup: %d", md5sum, file.path, len(files))
		}
	}
}
