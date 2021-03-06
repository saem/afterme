package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
)

var appendedArgs = false
var serverInited = false

func initServer(args []string) {
	if !serverInited {
		go notStupidMain(args)
		serverInited = true
	}
}

func BenchmarkResponseTime(b *testing.B) {
	dataDir := "./test-data-dir"
	args := []string{"afterme", fmt.Sprintf("-datadir=%s", dataDir), "-port=4001"}

	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0700) // Ignore directory exist errors

	initServer(args)

	payload := `[
    {
        "id": 0,
        "guid": "032bbc58-b343-4f6d-978b-550850d25731",
        "isActive": false,
        "balance": "$2,668.00",
        "picture": "http://placehold.it/32x32",
	"id": 0,
        "guid": "032bbc58-b343-4f6d-978b-550850d25731",
        "isActive": false,
        "balance": "$2,668.00",
        "picture": "http://placehold.it/32x32",
	"id": 0,
        "guid": "032bbc58-b343-4f6d-978b-550850d25731",
        "isActive": false,
        "balance": "$2,668.00",
        "picture": "http://placehold.it/32x32",
	"id": 0,
        "guid": "032bbc58-b343-4f6d-978b-550850d25731",
        "isActive": false,
        "balance": "$2,668.00",
        "picture": "http://placehold.it/32x32"
    }
]`

	successChannel := make(chan bool)
	client := &http.Client{}
	b.ResetTimer()

	for iterations := 0; iterations < b.N; iterations++ {
		totalRequests := 100
		success := 0
		failure := 0

		for i := 0; i < totalRequests; i++ {
			go post(client, payload, successChannel)
		}

		for success+failure < totalRequests {
			status := <-successChannel
			if status {
				success++
			} else {
				failure++
			}
		}

		if success != totalRequests {
			b.Error("All requests not successful, %s/%s", success, totalRequests)
		}
	}
}

func post(client *http.Client, payload string, successChannel chan bool) {
	status := false
	resp, err := client.Post("http://localhost:4001/message", "application/json", strings.NewReader(payload))
	if err != nil {
		fmt.Println(err.Error())
		successChannel <- status

		return
	}

	resp.Body.Close()
	if http.StatusOK == resp.StatusCode {
		status = true
	}

	successChannel <- status
}
