package main

import (
	"fmt"
	"github.com/cihub/seelog"
	"os"
	"os/signal"
	"syscall"

	"github.com/yuichiro-h/go/aws/sqsrouter"
)

func main() {
	r, err := sqsrouter.New(sqsrouter.WithLogger(seelog.Current))
	if err != nil {
		fmt.Println(err)
		return
	}
	r.AddHandler(os.Getenv("SQS_URL"), func(context *sqsrouter.Context) {
		context.SetDeleteOnFinish(true)

		msg, err := context.GetSNSMessage()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("%+v", msg)
	})
	r.Start()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	r.Stop()
}
