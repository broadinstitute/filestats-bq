package main

import (
	"context"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
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
	re := regexp.MustCompile(regex)
	go func() {
		defer close(stats)
		err := godirwalk.Walk(path, &godirwalk.Options{
			// FollowSymbolicLinks: true,
			Unsorted:      true,
			ErrorCallback: walkErrorCallback,
			Callback:      walkCallback(re, stats),
		})
		if err != nil {
			errs <- err
		}
	}()
}

func walkErrorCallback(
	path string,
	err error,
) godirwalk.ErrorAction {
	log.Println(err)
	return godirwalk.SkipNode
}

func walkCallback(
	re *regexp.Regexp,
	stats chan<- *fileStat,
) godirwalk.WalkFunc {
	return func(path string, de *godirwalk.Dirent) error {
		errs := make(chan error, 1)
		go func() {
			errs <- walkHandler(path, re, de, stats)
		}()
		return <-errs
	}
}

func walkHandler(
	path string,
	re *regexp.Regexp,
	de *godirwalk.Dirent,
	stats chan<- *fileStat,
) (
	err error,
) {
	if de.IsDevice() || de.IsDir() || !re.MatchString(path) {
		return
	}
	target := path
	if de.IsSymlink() {
		if target, err = os.Readlink(path); err != nil {
			return
		}
	}
	stat, err := os.Stat(target)
	if err != nil {
		return
	}
	if stat.Mode()&os.ModeType != 0 { // not a regular file
		return
	}
	mode := de.ModeType() | stat.Mode()
	link := ""
	if de.IsSymlink() {
		link = target
	}
	stats <- &fileStat{
		path, mode, stat.ModTime(), stat.Size(), link,
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
		err = w.Write([]string{
			stat.path, stat.mode.String(),
			civil.DateTimeOf(stat.modTime).String(),
			strconv.FormatInt(stat.size, 10), stat.link,
		})
		if err != nil {
			break
		}
	}
	return
}

type fileStat struct {
	path    string
	mode    os.FileMode
	modTime time.Time
	size    int64
	link    string
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
	source.Schema = []*bigquery.FieldSchema{
		{Name: "Path", Type: bigquery.StringFieldType, Required: true},
		{Name: "Mode", Type: bigquery.StringFieldType, Required: true},
		{Name: "Date_modified", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "Size_bytes", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "Link", Type: bigquery.StringFieldType},
	}

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
