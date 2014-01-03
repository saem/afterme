package main

import (
	"flag"
	"fmt"
	"github.com/saem/afterme/app"
	"github.com/saem/afterme/server"
	"log"
	"os"
	"runtime"
)

func main() {
	notStupidMain(os.Args)
}

// notStupidMain is a fix for go's moronic way of handling argv, whoever though a global variable than a sane
// parameter pass to the main function was a good idea should have their head checked. Seriously, why create
// more global state, rather than less. Now testing around main is more difficult, congrats, for what benefit?
func notStupidMain(argv []string) {
	// Command Line Parameters/Flags
	flags := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	var dataDir string
	flags.StringVar(&dataDir, "datadir",
		app.DefaultDataDir,
		fmt.Sprintf("Sets the data-dir, defaults to: %s", app.DefaultDataDir))
	var port int
	flags.IntVar(&port, "port",
		server.DefaultPort,
		fmt.Sprintf("Sets the port, defaults to: %d", server.DefaultPort))

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	flags.Parse(argv[1:])

	logger := log.New(os.Stdout, "", log.LstdFlags)

	var appServer = app.CreateAppServer(dataDir, logger)

	go appServer.ProcessMessages()

	err := server.Start(fmt.Sprintf("localhost:%d", port), appServer)

	if err != nil {
		appServer.Logger.Fatalf("Could not start http server: %s", err.Error())
	}
}
