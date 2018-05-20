package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/yuichiro-h/go/aws/sqsrouter"
)

func main() {
	logger, _ := zap.NewDevelopment()
	r, err := sqsrouter.New(sqsrouter.WithLogger(logger))
	if err != nil {
		fmt.Println(err)
		return
	}
	r.AddAsyncHandler(os.Getenv("SQS_URL"), func(context *sqsrouter.Context) {
		context.SetDeleteOnFinish(true)

		msg, err := context.GetSNSMessage()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("%s\n", msg.Message)
	})
	r.Start()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	r.Stop()
}
