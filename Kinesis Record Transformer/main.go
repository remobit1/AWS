package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

var (
	sess *session.Session
)

const (
	maxResponseSize = 6000000
)

func init() {
	sess = session.Must(session.NewSession())
}

func main() {
	lambda.Start(handler)
}

//Decode and GZIP records, check the CW data for bad logs, then return a slice of response records
func processRecords(evnt events.KinesisFirehoseEvent) []events.KinesisFirehoseResponseRecord {

	var b strings.Builder
	var processedRecord events.KinesisFirehoseResponseRecord
	var msg string
	var processedRecords []events.KinesisFirehoseResponseRecord

	for _, record := range evnt.Records {
		fmt.Printf("RecordID: %s\n", record.RecordID)
		fmt.Printf("ApproximateArrivalTimestamp: %s\n", record.ApproximateArrivalTimestamp)

		recrd, err := base64.StdEncoding.DecodeString(record.Data)

		checkError(err)

		buf := bytes.NewBuffer(recrd)

		gzr, err := gzip.NewReader(buf)

		checkError(err)

		defer gzr.Close()

		io.Copy(&b, gzr)

		var CWEventData events.CloudwatchLogsData

		err = json.Unmarshal([]byte(b.String()), &CWEventData)

		checkError(err)

		switch CWEventData.MessageType {
		case "CONTROL_MESSAGE":
			processedRecord.Result = "Dropped"
			processedRecord.RecordID = record.RecordID
		case "DATA_MESSAGE":
			processedRecord.Result = "Ok"
			processedRecord.RecordID = record.RecordID
			var data strings.Builder
			for _, logEvent := range CWEventData.LogEvents {
				msg = transformLogData(logEvent)
				_, err = data.WriteString(msg)
				checkError(err)
			}
			//encodedData := base64.StdEncoding.EncodeToString([]byte(data.String()))
			processedRecord.Data = []byte(data.String())
		default:
			processedRecord.Result = "ProcessingFailed"
			processedRecord.RecordID = record.RecordID
		}

		processedRecords = append(processedRecords, processedRecord)
		b.Reset()
	}

	return processedRecords

}

// Put transformation logic here.
func transformLogData(logEvent events.CloudwatchLogsLogEvent) string {
	/*
			Transform each log event.

		    The default implementation below just extracts the message and returns it as is.

		    Args:
		    logevent (struct): The original log event. Structure is {"id": str, "timestamp": long, "message": str}

		    Returns:
			str: The transformed log event.
	*/

	var msg strings.Builder

	_, err := msg.WriteString(logEvent.Message)

	checkError(err)

	_, err = msg.WriteString("\n \n")

	checkError(err)

	return msg.String()
}

func putRecordsToFirehoseStream(stream string, records []*firehose.Record) {
	svc := firehose.New(sess)

	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(stream),
		Records:            records,
	}

	_, err := svc.PutRecordBatch(input)

	if err != nil {
		log.Fatal(err.Error())
	}
}

func checkError(err error) {

	if err != nil {
		log.Fatal(err.Error())
	}

}

func handler(event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	fmt.Printf("InvocationID: %s\n", event.InvocationID)
	fmt.Printf("DeliveryStreamArn: %s\n", event.DeliveryStreamArn)
	fmt.Printf("Region: %s\n", event.Region)

	var response events.KinesisFirehoseResponse
	var projectedSize int
	var reingestionEvent []*firehose.Record
	var reingestionRecord firehose.Record
	streamName := strings.Split(event.DeliveryStreamArn, "/")

	transformedRecords := processRecords(event)

	for _, rec := range transformedRecords {
		if rec.Result != "Ok" {
			continue
		}

		projectedSize += len(rec.Data) + len(rec.RecordID)
		fmt.Printf("The projected size is %v\n", projectedSize)

		if projectedSize > maxResponseSize {
			reingestionRecord = firehose.Record{
				Data: rec.Data,
			}
			reingestionEvent = append(reingestionEvent, &reingestionRecord)

		} else {
			response.Records = append(response.Records, rec)
		}
	}

	if len(reingestionEvent) > 0 {
		putRecordsToFirehoseStream(streamName[1], reingestionEvent)
		fmt.Println("Records were reingested")
	}

	test, err := json.Marshal(response)

	checkError(err)

	fmt.Print(string(test))

	return response, nil
}
