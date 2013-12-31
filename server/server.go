package server

import (
	"fmt"
	"github.com/saem/afterme/app"
	"io"
	"io/ioutil"
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
		msg := fmt.Sprintf("Content-Length header required, and no bigger than: %db", app.MaxMessageSize)
		http.Error(w, msg, http.StatusLengthRequired)

		return
	}

	body, err := ioutil.ReadAll(r.Body)
	bytesRead := len(body)

	if err != nil && err != io.EOF {
		msg := fmt.Sprintf("Unanticipated error ocurred while reading the request body: %s", err.Error())
		http.Error(w, msg, http.StatusBadRequest)

		return
	}
	if bytesRead != int(r.ContentLength) {
		msg := fmt.Sprintf("Content-Length %d b, does not match body length %d b", r.ContentLength, bytesRead)
		http.Error(w, msg, http.StatusPreconditionFailed)

		return
	}
	if bytesRead == 0 {
		http.Error(w, "Empty body", http.StatusPreconditionFailed)

		return
	}

	notifier := appServer.RequestWrite(body)
	wr := <-notifier
	if wr.Err != nil {
		fmt.Fprintf(w, "Something went wrong when writing")
	} else {
		fmt.Fprintf(w, "Successfully written, sequence: %s, sha1: %s", wr.Sequence, wr.Hash)
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
