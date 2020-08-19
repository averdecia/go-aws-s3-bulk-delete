package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var elements = make(chan Row)
var elementsMultiple = make(chan []Row)
var deletedFilesCount = 0
var deletedFilesLastCheckBySeconds = 0
var args = getArgs()
var start = time.Now()

var endpointResolver = func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	if service == endpoints.S3ServiceID {
		return endpoints.ResolvedEndpoint{
			URL: args.Endpoint,
		}, nil
	}

	return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
}

var awsSvc = func() *s3.S3 {
	session, _ := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(args.Endpoint),
		EndpointResolver: endpoints.ResolverFunc(endpointResolver),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	return s3.New(session)
	// client.Handlers.Sign.Swap(v4.SignRequestHandler.Name, func(req *request.Request) {
	// 	// TODO custom signing logic.
	//  })
}()

var outputPointer *csv.Writer
var outputFile *os.File

// Row is used to save the csv format
type Row struct {
	Bucket      string
	FileID      string
	PreviewID   string
	PreviewName string
}

func (r *Row) toArray() []string {
	return []string{
		r.Bucket,
		r.FileID,
		r.PreviewID,
		r.PreviewName,
	}
}

// Args is used to save the args been used
type Args struct {
	FilePath       string // First argument with file path
	Endpoint       string // Second argument with the S3 domain
	GoRoutines     int    // Third argument with amount of routines
	ProgressPivot  int    // Fourth argument with pivot for printing
	RemoveMultiple int    // Fifth argument with the amount of files to remove from S3 on one request
	OutputPath     string // Sixth argument with the path to save the filed rows
	Prefix         string // Seventh argument with the prefix in the bucket
}

func main() {
	fmt.Println("Starting process")
	fmt.Printf("Arguments founds: { Path: %v, Endpoint: %v, Routines: %v, Pivot: %v, RemoteMultiple: %v , OutputPath: %v } \n", args.FilePath, args.Endpoint, args.GoRoutines, args.ProgressPivot, args.RemoveMultiple, args.OutputPath)

	outputPointer, outputFile := getOutputWriter()
	defer closeOutputWriter(outputPointer, outputFile)

	createRoutines(args.GoRoutines)
	readFile(args.FilePath, ",")

	var wg sync.WaitGroup
	wg.Add(1)

	ticker := time.NewTicker(15 * time.Second)
	quit := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ticker.C:
				if deletedFilesCount == deletedFilesLastCheckBySeconds {
					printProgress(true)
					close(quit)
				} else {
					deletedFilesLastCheckBySeconds = deletedFilesCount
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	fmt.Println("Waiting To Finish")
	wg.Wait()

	fmt.Println("\nTerminating Program")
}

func buildID(fileid string) string {
	return args.Prefix + fileid
}

func getArgs() Args {
	args := os.Args[1:]

	if len(args) < 7 {
		fmt.Printf("Not enough arguments: %v\n", args)
		os.Exit(0)
	}

	routines, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Printf("Not int parameter routines: %v, using default 10\n", routines)
		routines = 10
	}
	pivot, err := strconv.Atoi(args[3])
	if err != nil {
		fmt.Printf("Not int parameter pivot: %v, using default 100\n", pivot)
		pivot = 100
	}
	multiple, err := strconv.Atoi(args[4])
	if err != nil || multiple < 10 {
		fmt.Printf("Not int parameter multiple or lower than 10: %v, using default 0\n", multiple)
		multiple = 0
	}
	if args[6] != "" && args[6] != "urn:oid:" {
		args[6] = "urn:oid:"
	}

	return Args{
		FilePath:       args[0],
		Endpoint:       args[1],
		GoRoutines:     routines,
		ProgressPivot:  pivot,
		RemoveMultiple: multiple,
		OutputPath:     args[5],
		Prefix:         args[6],
	}
}

func readFile(filepath string, separator string) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fmt.Println("Reading file")
	var objects = make([]Row, args.RemoveMultiple)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		split := strings.Split(scanner.Text(), separator)
		if len(split) < 4 {
			fmt.Printf("Ignored small Element: %v \n", scanner.Text())
			continue
		}
		if args.RemoveMultiple == 0 {
			elements <- Row{
				Bucket:      split[0],
				FileID:      split[1],
				PreviewID:   buildID(split[2]),
				PreviewName: split[3],
			}
		} else {
			objects = append(objects, Row{
				Bucket:      split[0],
				FileID:      split[1],
				PreviewID:   buildID(split[2]),
				PreviewName: split[3],
			})
			if len(objects)%args.RemoveMultiple == 0 {
				elementsMultiple <- objects
			}
		}
	}

	if args.RemoveMultiple == 0 {
		elementsMultiple <- objects
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Ending reading file")
}

func createRoutines(count int) {
	for i := 0; i < count; i++ {
		time.Sleep(1 * time.Second)
		go func() {
			for {
				select {
				case element := <-elements:
					removeFromS3SingleByCommand(element)
					deletedFilesCount++
					printProgress(false)
				case array := <-elementsMultiple:
					if len(array) > 0 {
						removeFromS3Multiple(array, array[0].Bucket)
						deletedFilesCount += len(array)
					}
				}
			}

		}()
	}
}

func deleteFromS3(element Row) {
	fmt.Printf("Printing row: %v \n", element)
}

func printProgress(end bool) {
	if args.RemoveMultiple != 0 || end || (args.RemoveMultiple == 0 && deletedFilesCount%args.ProgressPivot == 0) {
		seconds := time.Since(start).Seconds()
		fmt.Printf("Mean velocity: %v f/s -- Deleted: %v files \n", math.Round(float64(deletedFilesCount)/seconds), deletedFilesCount)
	}
}

func removeFromS3SingleByCommand(element Row) {
	coargs := []string{"s3", "rm", "s3://" + element.Bucket + "/" + element.PreviewID, "--endpoint-url", args.Endpoint}
	out, err := exec.Command("aws", coargs...).Output()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		// fmt.Printf("Pointer: %v \n", outputPointer)
		outputPointer.Write(element.toArray())
		return
	}
	fmt.Println("Success: " + string(out))
}

func removeFromS3Single(element Row) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(element.Bucket),
		Key:    aws.String(element.PreviewID),
	}
	_, err := awsSvc.DeleteObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		outputPointer.Write(element.toArray())
		fmt.Printf("Object not removed: %v\n", element.PreviewID)
		return
	}

	//fmt.Printf("Object succesfully removed: %v\n", element.PreviewID)
}

func removeFromS3Multiple(rows []Row, bucket string) {
	// Preparing parameters for multi delete request
	var objects []*s3.ObjectIdentifier
	for _, element := range rows {
		objects = append(objects, &s3.ObjectIdentifier{
			Key: aws.String(element.PreviewID),
		})
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}
	_, err := awsSvc.DeleteObjects(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		// TODO: Save on csv the error files
		return
	}

	//fmt.Printf("Objects succesfully removed: %v \n", result)
}

func getOutputWriter() (*csv.Writer, *os.File) {
	csvfile, err := os.Create(args.OutputPath)

	if err != nil {
		fmt.Printf("Failed creating file: %s \n", err)
	}
	pointer := csv.NewWriter(csvfile)

	return pointer, csvfile
}

func closeOutputWriter(pointer *csv.Writer, file *os.File) {
	pointer.Flush()
	file.Close()
}
