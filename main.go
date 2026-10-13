package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"testing.com/pipes/winpipe"
)

const pipeName = `\\.\pipe\\testPipe`

func main(){
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	srv, err := winpipe.NewWindowsPipe(pipeName)

	if(err != nil){
		log.Fatalf("Error creating pipe: %v", err)
	}

	errCh := make(chan error, 1)
	go func(){
		errCh <- srv.RunPipeListener(ctx)
	}()

	log.Println("Listening on ", pipeName)

	for cfg := range srv.Events() {
		log.Println("Received config: ", cfg.ConfigData)
	}

	if err := <-errCh; err != nil{
		log.Fatalf("Server: %v", err)
	}

	log.Println("Pipe closed")
}