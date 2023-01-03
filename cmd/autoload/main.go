package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
)

var (
	project string
	dataset string
	table   string
	ctx     context.Context
	client  *bigquery.Client
)

func init() {
	flag.StringVar(&project, "project", "", "Google Cloud project")
	flag.StringVar(&dataset, "dataset", "", "BigQuery dataset")
	flag.StringVar(&table, "table", "", "BigQuery table")
}

func main() {
	flag.Parse()

	ctx = context.Background()
	var err error
	client, err = bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", PubSub)
	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err = http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

type PubSubMessage struct {
	Message struct {
		Data       []byte            `json:"data,omitempty"`
		ID         string            `json:"id"`
		Attributes map[string]string `json:"attributes"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// PubSub receives and processes a Pub/Sub push message.
func PubSub(w http.ResponseWriter, r *http.Request) {
	var m PubSubMessage
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("ioutil.ReadAll: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// byte slice unmarshalling handles base64 decoding.
	if err := json.Unmarshal(body, &m); err != nil {
		log.Printf("json.Unmarshal: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	bucket := m.Message.Attributes["bucketId"]
	object := m.Message.Attributes["objectId"]

	switch event := m.Message.Attributes["eventType"]; event {
	case "OBJECT_FINALIZE":
		gcsRef := bigquery.NewGCSReference("gs://" + bucket + "/" + object)
		gcsRef.SourceFormat = bigquery.JSON
		loader := client.Dataset(dataset).Table(table).LoaderFrom(gcsRef)
		loader.SchemaUpdateOptions = []string{"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"}
		loader.WriteDisposition = bigquery.WriteAppend

		job, err := loader.Run(ctx)
		if err != nil {
			log.Printf("loader.Run: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		status, err := job.Wait(ctx)
		if err != nil {
			log.Printf("job.Wait: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if status.Err() != nil {
			log.Printf("status.Err(): %v", status.Err())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}
