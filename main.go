package main

import (
	"context"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/karrick/godirwalk"
	"google.golang.org/api/option"
)

func main() {
	path := flag.String("dir", "", "Directory path for file search")
	regex := flag.String("regex", "", "File path pattern to match")
	key := flag.String("key", "", "Path to Google Service Account key file")
	project := flag.String("project", "", "BigQuery project ID")
	dataset := flag.String("dataset", "", "BigQuery dataset ID")
	table := flag.String("table", "", "BigQuery table ID")
	flag.Parse()

	errs := make(chan error, 1)
	stats := make(chan *fileStat)

	walk(errs, stats, *path, *regex)

	loadStats(errs, stats, &bigQueryConfig{
		*key, *project, *dataset, *table,
	})

	if err := <-errs; err != nil {
		log.Fatal(err)
	}
	log.Println("Done")
}

func walk(
	errs chan<- error,
	stats chan<- *fileStat,
	path string,
	regex string,
) {
	var err error
	if path, err = filepath.Abs(path); err != nil {
		errs <- err
		return
	}
	re := regexp.MustCompile(regex)
	go func() {
		defer close(stats)
		var wg sync.WaitGroup
		err := godirwalk.Walk(path, &godirwalk.Options{
			// FollowSymbolicLinks: true,
			Unsorted:      true,
			ErrorCallback: walkErrorCallback(stats),
			Callback:      walkCallback(re, &wg, stats),
		})
		if err != nil {
			errs <- err
		}
		wg.Wait()
	}()
}

func walkErrorCallback(
	stats chan<- *fileStat,
) func(string, error) godirwalk.ErrorAction {
	return func(path string, err error) godirwalk.ErrorAction {
		stats <- &fileStat{path: path, err: err}
		return godirwalk.SkipNode
	}
}

func walkCallback(
	re *regexp.Regexp,
	wg *sync.WaitGroup,
	stats chan<- *fileStat,
) godirwalk.WalkFunc {
	return func(path string, de *godirwalk.Dirent) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			walkHandler(path, re, de, stats)
		}()
		return nil
	}
}

func walkHandler(
	path string,
	re *regexp.Regexp,
	de *godirwalk.Dirent,
	stats chan<- *fileStat,
) {
	if de.IsDevice() || de.IsDir() || !re.MatchString(path) {
		return
	}
	var target string
	var err error
	if de.IsSymlink() {
		if target, err = filepath.EvalSymlinks(path); err != nil {
			if e, ok := err.(*os.PathError); ok {
				target = e.Path
			}
		}
	}
	resolved := path
	if target != "" {
		resolved = target
	}
	var stat os.FileInfo
	if err == nil {
		stat, err = os.Stat(resolved)
	}
	if err != nil {
		var e error
		if stat, e = os.Lstat(path); e != nil {
			mode := de.ModeType()
			stats <- &fileStat{
				path, &mode, nil, nil, target, err,
			}
			return
		}
	}
	if stat.IsDir() {
		return
	}
	mode := stat.Mode()
	modTime := stat.ModTime()
	size := stat.Size()
	stats <- &fileStat{
		path, &mode, &modTime, &size, target, err,
	}
	return
}

type bigQueryConfig struct {
	key     string
	project string
	dataset string
	table   string
}

func loadStats(
	errs chan<- error,
	stats <-chan *fileStat,
	c *bigQueryConfig,
) {
	ctx, loader, writer, err := getWriter(c)
	if err != nil {
		errs <- err
		return
	}
	go func() {
		if err := writeStats(writer, stats); err != nil {
			errs <- err
		}
	}()
	go func() {
		errs <- loadJob(ctx, loader)
	}()
}

func writeStats(
	writer *io.PipeWriter,
	stats <-chan *fileStat,
) (
	err error,
) {
	w := csv.NewWriter(writer)
	w.Comma = '\t'

	defer writer.Close()
	defer w.Flush()

	for stat := range stats {
		mode := ""
		if stat.mode != nil {
			mode = stat.mode.String()
		}
		modTime := ""
		if stat.modTime != nil {
			modTime = civil.DateTimeOf(*stat.modTime).String()
		}
		size := ""
		if stat.size != nil {
			size = strconv.FormatInt(*stat.size, 10)
		}
		e := ""
		if stat.err != nil {
			e = stat.err.Error()
		}
		err = w.Write([]string{
			stat.path,
			mode,
			modTime,
			size,
			stat.target,
			e,
		})
		if err != nil {
			break
		}
	}
	return
}

type fileStat struct {
	path    string
	mode    *os.FileMode
	modTime *time.Time
	size    *int64
	target  string
	err     error
}

func getSchema() []*bigquery.FieldSchema {
	return []*bigquery.FieldSchema{
		{
			Name: "Path", Type: bigquery.StringFieldType, Required: true,
			Description: "Absolute path to the file",
		},
		{
			Name: "Mode", Type: bigquery.StringFieldType,
			Description: "File mode bits",
		},
		{
			Name: "Modified", Type: bigquery.TimestampFieldType,
			Description: "Timestamp of the last file modification",
		},
		{
			Name: "Size", Type: bigquery.IntegerFieldType,
			Description: "Size of the file, in bytes",
		},
		{
			Name: "Target", Type: bigquery.StringFieldType,
			Description: "Target of the symlink, if applicable",
		},
		{
			Name: "Error", Type: bigquery.StringFieldType,
			Description: "Error in retrieval of file stats",
		},
	}
}

func getWriter(
	c *bigQueryConfig,
) (
	ctx context.Context,
	loader *bigquery.Loader,
	writer *io.PipeWriter,
	err error,
) {
	reader, writer := io.Pipe()
	source := bigquery.NewReaderSource(reader)
	source.FieldDelimiter = "\t"
	source.Schema = getSchema()

	ctx = context.Background()
	opts := make([]option.ClientOption, 0, 1)
	if c.key != "" {
		opts = append(opts, option.WithCredentialsFile(c.key))
	}
	bq, err := bigquery.NewClient(ctx, c.project, opts...)
	if err != nil {
		return
	}

	loader = bq.Dataset(c.dataset).Table(c.table).LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteTruncate

	return
}

func loadJob(
	ctx context.Context,
	loader *bigquery.Loader,
) (
	err error,
) {
	job, err := loader.Run(ctx)
	if err != nil {
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return
	}
	return status.Err()
}
