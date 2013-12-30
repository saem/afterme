package server

import (
	"fmt"
	"github.com/saem/afterme/app"
	"io"
	"net/http"
)

// Package private instance that the handler methods use
var appServer *app.App = nil

// Starts a server listening, handling requests and forwarding them to the App as needed
func Start(addr string, a *app.App) (err error) {
	http.HandleFunc("/message", messageHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", http.NotFound)

	appServer = a

	return http.ListenAndServe(addr, nil)
}

// A write
func messageHandler(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength < 0 || r.ContentLength > app.MaxMessageSize {
		fmt.Fprintf(w, "Do some HTTP error code stuff")
	}

	var body []byte
	bytesRead, err := r.Body.Read(body)

	if err != nil && err != io.EOF {
		fmt.Fprintf(w, "Do some HTTP error code stuff, got and error: %s", err.Error())
	}
	if bytesRead != int(r.ContentLength) {
		fmt.Fprintf(w, "Do some HTTP error code stuff, content length incorrect")
	}

	r.Body.Close()

	notifier := appServer.RequestWrite(body)
	writeResponse := <-notifier
	if writeResponse.Err != nil {
		fmt.Fprintf(w, "Something went wrong when writing")
	} else {
		fmt.Fprintf(w, "Successfully written, sequence: %s", writeResponse.Sequence)
	}
}

// Check the current status (sequence, version, configs, etc...)
func statusHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Finish implementing me
	fmt.Fprintf(w, "Return the current status.")
}

// Check the health (failed writes, latencies, blah),
// this would be more expensive than status checks, I would imagine
func healthHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Finish implementing me
	fmt.Fprintf(w, "Return the result of health checks.")
}
