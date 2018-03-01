package sqsrouter

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type SQSRouter struct {
	handlers []handler
	termChs  []chan chan int
	logger   *zap.Logger
}

type Option func(r *SQSRouter) error

func WithLogger(l *zap.Logger) Option {
	return func(r *SQSRouter) error {
		r.logger = l
		return nil
	}
}

type handlerFunc func(*Context)

type handler struct {
	function handlerFunc
	queueURL string
	async    bool
}

type Context struct {
	Message       *sqs.Message
	deleteMessage bool
}

type SNSMessage struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SignatureCertURL string `json:"SignatureCertURL"`
	UnsubscribeURL   string `json:"UnsubscribeURL"`
}

func (c *Context) GetSNSMessage() (*SNSMessage, error) {
	var msg SNSMessage
	err := json.Unmarshal([]byte(*c.Message.Body), &msg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &msg, nil
}

func (c *Context) SetDeleteOnFinish(delete bool) {
	c.deleteMessage = delete
}

func New(options ...Option) (*SQSRouter, error) {
	r := SQSRouter{
		logger: zap.NewNop(),
	}
	for _, o := range options {
		if err := o(&r); err != nil {
			return nil, err
		}
	}

	return &r, nil
}

func (s *SQSRouter) addHandler(queueURL string, async bool, h handlerFunc) {
	s.handlers = append(s.handlers, handler{
		function: h,
		queueURL: queueURL,
		async:    async,
	})
}

func (s *SQSRouter) AddHandler(queueURL string, h handlerFunc) {
	s.addHandler(queueURL, false, h)
}

func (s *SQSRouter) AddAsyncHandler(queueURL string, h handlerFunc) {
	s.addHandler(queueURL, true, h)
}

func (s *SQSRouter) Start() {
	var termChs []chan chan int
	for i := 0; i < len(s.handlers); i++ {
		termChs = append(termChs, make(chan chan int))
	}
	s.termChs = termChs

	for i := range s.handlers {
		go func(i int) {
			s.listen(&s.handlers[i], termChs[i])
		}(i)
	}
}

func (s *SQSRouter) Stop() {
	for i := 0; i < len(s.handlers); i++ {
		exitCh := make(chan int)
		s.termChs[i] <- exitCh
		<-exitCh
	}
}

func (s *SQSRouter) listen(h *handler, termCh chan chan int) {
	for {
		select {
		case exitCh := <-termCh:
			exitCh <- 0
			break
		default:
			sqsClient := sqs.New(session.Must(session.NewSession()), &aws.Config{})
			res, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				MaxNumberOfMessages: aws.Int64(1),
				QueueUrl:            aws.String(h.queueURL),
			})
			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					if awsErr.Code() == sqs.ErrCodeQueueDoesNotExist {
						panic(fmt.Sprintf("Not found queue. url=%s", h.queueURL))
					}
					s.logger.Error(awsErr.Message(), zap.Any("error", awsErr))
					continue
				} else {
					s.logger.Error(err.Error())
				}
			}

			f := func(msg *sqs.Message) {
				c := Context{
					Message:       msg,
					deleteMessage: false,
				}

				h.function(&c)

				if c.deleteMessage {
					_, err := sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      aws.String(h.queueURL),
						ReceiptHandle: msg.ReceiptHandle,
					})
					if err != nil {
						s.logger.Error(err.Error())
					}
				}
			}

			for _, msg := range res.Messages {
				if h.async {
					go f(msg)
				} else {
					f(msg)
				}
			}
		}
	}
}
