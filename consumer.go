package pulsar

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/tuya/pulsar-client-go/core/manage"
	"github.com/tuya/pulsar-client-go/core/msg"
	"github.com/tuya/tuya-pulsar-sdk-go/pkg/tylog"
)

const (
	SpendDecode     = "decode"
	SpendUnactive   = "unactive"
	SpendConsumerId = "ConsumerID"
	SpendHandle     = "handle"
	SpendAck        = "ack"
	SpendAll        = "totall"
)

type SpendLogFunc func(m *Message, spends map[string]time.Duration)

var defaultLogHandle = func(m *Message, spends map[string]time.Duration) {
	fields := make([]zap.Field, 0, 10)
	fields = append(fields, tylog.Any("msgID", m.Msg.GetMessageId()))
	fields = append(fields, tylog.String("topic", m.Topic))
	for slug, spend := range spends {
		fields = append(fields, tylog.String(slug+" spend", spend.String()))
	}
	tylog.Debug("Handler trace info", fields...)
}

type ConsumerConfig struct {
	Topic string
	Auth  AuthProvider
	Errs  chan error
	Log   SpendLogFunc
}

type consumerImpl struct {
	topic      string
	csm        *manage.ManagedConsumer
	cancelFunc context.CancelFunc
	stopFlag   uint32
	stopped    chan struct{}
	logHandle  SpendLogFunc
}

func (c *consumerImpl) ReceiveAsync(ctx context.Context, queue chan Message) {
	go func() {
		err := c.csm.ReceiveAsync(ctx, queue)
		if err != nil {
			tylog.Debug("consumer stopped", tylog.String("topic", c.topic))
		}
	}()
}

func (c *consumerImpl) ReceiveAndHandle(ctx context.Context, handler PayloadHandler) {
	queue := make(chan Message, 228)
	c.stopped = make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	c.cancelFunc = cancel
	go c.ReceiveAsync(ctx, queue)

	for {
		select {
		case <-ctx.Done():
			close(c.stopped)
			return
		case m := <-queue:
			if atomic.LoadUint32(&c.stopFlag) == 1 {
				close(c.stopped)
				return
			}
			tylog.Debug("consumerImpl receive message", tylog.String("topic", c.topic))
			bgCtx := context.Background()
			c.Handler(bgCtx, handler, &m)
		}
	}
}

func (c *consumerImpl) Handler(ctx context.Context, handler PayloadHandler, m *Message) {
	spends := make(map[string]time.Duration, 10)

	defer func(start time.Time) {
		spends[SpendAll] = time.Since(start)
		if c.logHandle != nil {
			c.logHandle(m, spends)
		} else {
			defaultLogHandle(m, spends)
		}
	}(time.Now())

	var list []*msg.SingleMessage
	now := time.Now()
	var err error
	num := m.Meta.GetNumMessagesInBatch()
	if num > 0 && m.Meta.NumMessagesInBatch != nil {
		list, err = msg.DecodeBatchMessage(m)
		if err != nil {
			tylog.Error("DecodeBatchMessage failed", tylog.ErrorField(err))
			return
		}
	}
	spends[SpendDecode] = time.Since(now)

	now = time.Now()
	if c.csm.Unactive() {
		tylog.Warn("unused msg because of consumer is unactivated", tylog.Any("payload", string(m.Payload)))
		return
	}
	spends[SpendUnactive] = time.Since(now)

	idCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	now = time.Now()
	idCheck := c.csm.ConsumerID(idCtx) == m.ConsumerID
	spends[SpendConsumerId] = time.Since(now)
	cancel()
	if !idCheck {
		tylog.Warn("unused msg because of different ConsumerID", tylog.Any("payload", string(m.Payload)))
		return
	}

	now = time.Now()
	if len(list) == 0 {
		err = handler.HandlePayload(ctx, m, m.Payload)
	} else {
		for i := 0; i < len(list); i++ {
			err = handler.HandlePayload(ctx, m, list[i].SinglePayload)
			if err != nil {
				break
			}
		}
	}
	spends[SpendHandle] = time.Since(now)
	if err != nil {
		tylog.Error("handle message failed", tylog.ErrorField(err),
			tylog.String("topic", m.Topic))
		if _, ok := err.(*BreakError); ok {
			return
		}
	}

	now = time.Now()
	ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = c.csm.Ack(ackCtx, *m)
	cancel()
	spends[SpendAck] = time.Since(now)
	if err != nil {
		tylog.Error("ack failed", tylog.ErrorField(err))
	}

}

func (c *consumerImpl) Stop() {
	atomic.AddUint32(&c.stopFlag, 1)
	c.cancelFunc()
	<-c.stopped
}
