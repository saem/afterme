package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var appendedArgs = false

func TestResponseTime(t *testing.T) {
	dataDir := "./test-data-dir"
	args := []string{"afterme", fmt.Sprintf("-datadir=%s", dataDir), "-port=4001"}

	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0700) // Ignore directory exist errors

	go notStupidMain(args)

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
	totalRequests := 100

	before := time.Now().UnixNano()
	for iterations := 0; iterations < 10; iterations++ {
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
			t.Error("All requests not successful, %s/%s", success, totalRequests)
		}
	}
	after := (time.Now().UnixNano() - before) / 1000 / 1000
	if after > 280 {
		t.Error("Something is broken, or test misconfigured, but it's taking more than 280ms for a 10 iterations by 100 concurrent request run")
	}
}

func post(client *http.Client, payload string, successChannel chan bool) {
	status := false
	resp, err := client.Post("http://localhost:4001/message", "application/json", strings.NewReader(payload))
	if err != nil {
		fmt.Println(err)
	}
	resp.Body.Close()
	if http.StatusOK == resp.StatusCode {
		status = true
	}

	successChannel <- status
}
