package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"errors"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dgraph-io/ristretto/v2"
)

// TransactionalPublisher is a Kafka Publisher with transactional support.
// The publisher will send all messages given in a Publish call in a single transaction.
// If configured with ExactlyOnce, it will also add the consumer message to the transaction, implementing an
// exactly-once delivery semantic.
// The information about the consumed message is taken from the context of the published messages, which are filled
// by the Subscriber. Please note that the Subscriber has also to be configured with ExactlyOnce = true.
// With ExactlyOnce = false, the TransactionalPublisher will still send the messages in a single transaction, but without
// adding the consumed message to the transaction.
//
// Make sure, that the consumers of the messages published by the TransactionalPublisher have their Consumer.IsolationLevel
// set to ReadCommited. Otherwise, messages of aborted transactions will still be processed.
type TransactionalPublisher struct {
	config TransactionalPublisherConfig

	// producerPool pools transactional sarama.SyncProducer instances
	producerPool producerPool

	logger watermill.LoggerAdapter

	closed atomic.Bool
	wg     sync.WaitGroup
}

// NewTransactionalPublisher creates a new TransactionalPublisher. The appName must be the same as used for the consumer
// group id of the consumed messages.
func NewTransactionalPublisher(
	config TransactionalPublisherConfig,
	logger watermill.LoggerAdapter,
) (*TransactionalPublisher, error) {
	logger = logger.With(watermill.LogFields{"transactional_publisher_id": watermill.NewUUID()})
	logger.Debug("creating new TransactionalPublisher", nil)

	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.OTELEnabled && config.Tracer == nil {
		config.Tracer = NewOTELSaramaTracer()
	}

	var pool producerPool
	if config.ExactlyOnce {
		if p, err := newExactlyOnceProducerPool(config, logger); err != nil {
			return nil, err
		} else {
			pool = p
		}
	} else {
		pool = newSimpleProducerPool(config, logger)
	}

	return &TransactionalPublisher{
		config:       config,
		producerPool: pool,
		logger:       logger,
	}, nil

}

type TransactionalPublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// If true then each sent message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	OTELEnabled bool

	// Tracer is used to trace Kafka messages.
	// If nil, then no tracing will be used.
	Tracer SaramaTracer

	// ExactlyOnce configures if the TransactionalProducer will also take care of committing the offset of the consumed message
	// Messages must be consumed by a Subscriber with ExactlyOnce = true.
	ExactlyOnce bool

	// ProducerPoolSize limits the number of producers that can be created.
	// Defaults to 10
	ProducerPoolSize int
}

func (c *TransactionalPublisherConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSyncTransactionalPublisherConfig()
	}

	if c.ProducerPoolSize == 0 {
		c.ProducerPoolSize = 10
	}
}

func (c TransactionalPublisherConfig) Validate() error {
	var errs []error

	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("missing brokers"))
	}
	if c.Marshaler == nil {
		errs = append(errs, errors.New("missing marshaler"))
	}

	if err := c.OverwriteSaramaConfig.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("invalid sarama config: %w", err))
	}

	return errors.Join(errs...)
}

func DefaultSaramaSyncTransactionalPublisherConfig() *sarama.Config {
	config := DefaultSaramaSyncPublisherConfig()

	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Version = sarama.DefaultVersion

	return config
}

func getConsumerData(msgs ...*message.Message) (consumerData *ConsumerData, err error) {
	for i, msg := range msgs {

		msgConsumerData, ok := ConsumerDataFromCtx(msg.Context())
		if !ok {
			return nil, errors.New("consumer data not found - make sure that you are using a kafka subscriber and your handler copied the message context to the published message")
		}

		if i == 0 {
			consumerData = &msgConsumerData
		} else if consumerData.Partition != msgConsumerData.Partition || consumerData.Offset != msgConsumerData.Offset ||
			consumerData.GroupID != msgConsumerData.GroupID || consumerData.Topic != msgConsumerData.Topic {
			return nil, errors.New("messages have inconsistent consumer data")
		}

	}
	return consumerData, nil
}

// Publish publishes message to Kafka with transactional support. All messages are sent in a single transaction,
// and the consumed message is added to the transaction.
// All messages must have the same consumer data, i.e. the same topic, partition, offset, and group ID.
func (p *TransactionalPublisher) Publish(topic string, msgs ...*message.Message) (err error) {
	if p.closed.Load() {
		return errors.New("publisher closed")
	}
	p.wg.Add(1)
	defer p.wg.Done()

	if len(msgs) == 0 {
		return nil
	}

	logger := p.logger.With(watermill.LogFields{"topic": topic})

	var consumerData *ConsumerData
	if p.config.ExactlyOnce {
		consumerData, err = getConsumerData(msgs...)
		if err != nil {
			return fmt.Errorf("could not get consumer data: %w", err)
		}
		logger = logger.With(
			watermill.LogFields{
				"consume_partition": consumerData.Partition,
				"consume_offset":    consumerData.Offset,
				"consume_group_id":  consumerData.GroupID,
				"consume_topic":     consumerData.Topic},
		)
	}

	poolHandle, err := p.producerPool.getHandle(consumerData)
	if err != nil {
		logger.Error("could not get producer pool handle", err, nil)
		return fmt.Errorf("could not get producer pool handle: %w", err)
	}

	producer, err := poolHandle.acquire()
	if err != nil {
		logger.Error("could not acquire producer", err, nil)
		return fmt.Errorf("could not acquire producer: %w", err)
	}
	defer func() {
		logger.Debug("releasing producer", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
		poolHandle.release(producer)
	}()

	logger.Debug("beginning transaction", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
	if err = producer.BeginTxn(); err != nil {
		logger.Error("could not begin transaction", err, nil)
		return fmt.Errorf("could not begin transaction: %w; txn_status: %v", err, producer.TxnStatus().String())
	}
	defer func() {
		if err != nil {
			logger.Error("publishing failed", err, watermill.LogFields{"txn_status": producer.TxnStatus().String()})
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			logger.Debug("aborting transaction", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
			if abortErr := producer.AbortTxn(); abortErr != nil {
				logger.Error("could not abort transaction", abortErr, watermill.LogFields{"txn_status": producer.TxnStatus().String()})
				err = fmt.Errorf("could not abort transaction: %w, originalError: %w", abortErr, err)
			} else {
				logger.Debug("aborted transaction", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
			}
		}
	}()

	for _, msg := range msgs {
		logger.Debug("sending message to Kafka", watermill.LogFields{"message_uuid": msg.UUID})

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			logger.Error("could not marshal message", err, watermill.LogFields{"message_uuid": msg.UUID})
			return fmt.Errorf("could not marshal message %s: %w", msg.UUID, err)
		}

		partition, offset, err := producer.SendMessage(kafkaMsg)
		if err != nil {
			logger.Error("could not produce message", err, watermill.LogFields{"txn_status": producer.TxnStatus().String(), "message_uuid": msg.UUID})
			return fmt.Errorf("could not produce message %s: %w", msg.UUID, err)
		}

		logger.Debug("message sent to Kafka", watermill.LogFields{
			"message_uuid":           msg.UUID,
			"kafka_partition":        partition,
			"kafka_partition_offset": offset,
			"txn_status":             producer.TxnStatus().String(),
		})
	}

	if p.config.ExactlyOnce {
		logger.Debug("adding consume message to transaction", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
		if err := addMessageToTxn(producer, *consumerData); err != nil {
			logger.Error("could not add consume message to transaction", err, watermill.LogFields{"txn_status": producer.TxnStatus().String()})
			return fmt.Errorf("could not add consume message to transaction: %w", err)
		}
	}

	logger.Debug("committing transaction", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
	if err := producer.CommitTxn(); err != nil {
		logger.Error("could not commit transaction", err, watermill.LogFields{"txn_status": producer.TxnStatus().String()})
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	logger.Debug("transaction committed", watermill.LogFields{"txn_status": producer.TxnStatus().String()})

	return nil
}

func addMessageToTxn(producer sarama.SyncProducer, consumerData ConsumerData) error {
	offsets := make(map[string][]*sarama.PartitionOffsetMetadata)
	offsets[consumerData.Topic] = []*sarama.PartitionOffsetMetadata{
		{
			Partition: consumerData.Partition,
			// see e.g. the implementation of producer.AddMessageToTxn, that offset + 1 is correct
			Offset: consumerData.Offset + 1,
		},
	}
	return producer.AddOffsetsToTxn(offsets, consumerData.GroupID)

}

func (p *TransactionalPublisher) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	p.logger.Debug("closing TransactionalPublisher, waiting for all publish calls to exit", nil)
	p.wg.Wait()
	p.logger.Debug("all publish calls exited, closing producer pool", nil)
	if err := p.producerPool.close(); err != nil {
		return fmt.Errorf("could not close producer pool: %w", err)
	}

	return nil
}

type producerPool interface {
	getHandle(consumerData *ConsumerData) (producerHandle, error)
	close() error
}

type producerHandle interface {
	acquire() (sarama.SyncProducer, error)
	release(producer sarama.SyncProducer)
}

func newExactlyOnceProducerPool(config TransactionalPublisherConfig, logger watermill.LoggerAdapter) (*exactlyOnceProducerPool, error) {
	maxCost := int64(config.ProducerPoolSize)
	numCounters := maxCost * 10
	cache, err := ristretto.NewCache(&ristretto.Config[string, *syncProducer]{
		NumCounters: numCounters,
		MaxCost:     maxCost,
		BufferItems: 64,
		OnEvict: func(item *ristretto.Item[*syncProducer]) {
			if item == nil || item.Value == nil {
				logger.Error("cannot evict producer", errors.New("item or item value is nil"), nil)
				return
			}
			logger.Debug("evicting producer", watermill.LogFields{"transaction_id": item.Key})
			item.Value.Lock()
			defer item.Value.Unlock()
			if item.Value != nil {
				if err := item.Value.Close(); err != nil {
					logger.Error("cannot close producer", err, watermill.LogFields{"transaction_id": item.Key})
				}
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot create producer pool: %w", err)
	}

	logger.Debug("created new exactlyOnceProducerPool", watermill.LogFields{"max_cost": maxCost, "num_counters": numCounters})

	return &exactlyOnceProducerPool{
		logger: logger,
		config: config,
		cache:  cache,
	}, nil
}

// exactlyOnceProducerPool pools transactional sarama.SyncProducer instances based on the topic and partition of an incoming message,
// supporting an atomic "read-process-write" pattern.
type exactlyOnceProducerPool struct {
	config TransactionalPublisherConfig
	logger watermill.LoggerAdapter

	// producers is a map of groupID-topic-partition to producer.
	// If the value exists and is nil, it means that the producer is acquired.
	cache *ristretto.Cache[string, *syncProducer]

	closed atomic.Bool
}

func (p *exactlyOnceProducerPool) getHandle(consumerData *ConsumerData) (producerHandle, error) {
	if consumerData == nil {
		return nil, errors.New("cannot get producer handle: consumerData is nil")
	}
	return &exactlyOnceProducerPoolHandle{
		pool: p,
		tp:   topicPartition{groupID: consumerData.GroupID, topic: consumerData.Topic, partition: consumerData.Partition},
	}, nil
}

type exactlyOnceProducerPoolHandle struct {
	pool *exactlyOnceProducerPool
	tp   topicPartition
}

func (h *exactlyOnceProducerPoolHandle) acquire() (sarama.SyncProducer, error) {
	return h.pool.acquire(h.tp)
}

func (h *exactlyOnceProducerPoolHandle) release(producer sarama.SyncProducer) {
	syncProducer, ok := producer.(*syncProducer)
	if !ok {
		h.pool.logger.Error("cannot release producer", errors.New("producer is not a *syncProducer"), nil)
		panic("producer is not a *syncProducer")
	}
	h.pool.release(h.tp, syncProducer)

}

// topicPartition is used as the key for the exactlyOnceProducerPool
type topicPartition struct {
	groupID   string
	topic     string
	partition int32
}

func (tp topicPartition) String() string {
	return fmt.Sprintf("%s-%s-%d", tp.groupID, tp.topic, tp.partition)
}

// acquire returns a producer for the given topic and partition.
// It makes sure, that only one producer is created for each topic-partition pair.
// It is assumed that the caller makes sure that there is only one concurrent call to acquire for the same topic-partition pair.
// If the producer is already acquired, it returns an error.
// This is to support the "zombie fencing" done by kafka based on the transactional id. See [transactions-apache-kafka] for
// more information.
//
// [transactions-apache-kafka]: https://www.confluent.io/blog/transactions-apache-kafka/
func (p *exactlyOnceProducerPool) acquire(tp topicPartition) (*syncProducer, error) {

	if p.closed.Load() {
		return nil, errors.New("pool closed")
	}

	producer, ok := p.cache.Get(tp.String())
	if ok {
		producer.Lock()
		if !producer.closed {
			p.logger.Debug("acquired existing producer", watermill.LogFields{"transaction_id": tp.String()})
			return producer, nil
		}
		p.logger.Debug("producer is closed, removing from cache", watermill.LogFields{"transaction_id": tp.String()})
		producer.Unlock()
	}
	producer, err := p.new(tp)
	if err != nil {
		return nil, fmt.Errorf("cannot create producer for topic %s and partition %d: %w", tp.topic, tp.partition, err)
	}
	producer.Lock()
	set := p.cache.Set(tp.String(), producer, 1)
	if !set {
		p.logger.Debug("cannot set producer in cache", watermill.LogFields{"transaction_id": tp.String()})
	}
	p.cache.Wait()
	p.logger.Debug("acquired new producer", watermill.LogFields{"transaction_id": tp.String()})
	return producer, nil

}

func (p *exactlyOnceProducerPool) release(tp topicPartition, producer *syncProducer) {
	p.logger.Debug("releasing producer", watermill.LogFields{"groupID": tp.groupID, "topic": tp.topic, "partition": tp.partition, "txn_status": producer.TxnStatus().String()})
	defer producer.Unlock()

	alive, err := closeOnNotReady(producer)
	if err != nil {
		p.logger.Error("cannot close producer", err, nil)
	}
	if alive {
		p.logger.Debug("releasing producer", watermill.LogFields{"transaction_id": tp.String()})
		p.cache.Set(tp.String(), producer, 1)
	} else {
		p.logger.Debug("removing producer from pool", watermill.LogFields{"transaction_id": tp.String()})
		p.cache.Del(tp.String())
	}

}

func (p *exactlyOnceProducerPool) new(tp topicPartition) (*syncProducer, error) {
	producerConfig := *p.config.OverwriteSaramaConfig
	producerConfig.Producer.Transaction.ID = tp.String()

	p.logger.Debug("creating new producer", watermill.LogFields{"transaction_id": producerConfig.Producer.Transaction.ID})
	producer, err := newSyncProducer(p.logger, p.config.Brokers, &producerConfig)
	if err != nil {
		p.logger.Error("cannot create producer", err, watermill.LogFields{"transaction_id": producerConfig.Producer.Transaction.ID})
		return nil, fmt.Errorf("cannot create producer: %w", err)
	}
	p.logger.Debug("created new producer", watermill.LogFields{"transaction_id": producerConfig.Producer.Transaction.ID})

	if p.config.Tracer != nil {
		producer.SyncProducer = p.config.Tracer.WrapSyncProducer(&producerConfig, producer.SyncProducer)
	}

	return producer, nil

}

func (p *exactlyOnceProducerPool) close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	p.cache.Clear()

	return nil
}

type token struct{}

// simpleProducerPool is a simple pool of sarama.SyncProducer instances.
// The implementation is based on [Bryan Mills's talk on concurrency patterns], as it is recommended in the comment of sync.Cond.
//
// [Bryan Mills's talk on concurrency patterns]: https://drive.google.com/file/d/1nPdvhB0PutEJzdCq5ms6UI58dp50fcAN/view
type simpleProducerPool struct {
	// sem is a semaphore to limit the number of producers. If the channel is full, no more producers can be created.
	sem chan token
	// idle is a channel of idle producers.
	idle   chan sarama.SyncProducer
	config TransactionalPublisherConfig
	logger watermill.LoggerAdapter
	closed atomic.Bool
}

func newSimpleProducerPool(config TransactionalPublisherConfig, logger watermill.LoggerAdapter) *simpleProducerPool {
	return &simpleProducerPool{
		sem:    make(chan token, config.ProducerPoolSize),
		idle:   make(chan sarama.SyncProducer, config.ProducerPoolSize),
		config: config,
		logger: logger,
	}
}

func (p *simpleProducerPool) getHandle(_ *ConsumerData) (producerHandle, error) {
	return p, nil
}

func (p *simpleProducerPool) acquire() (sarama.SyncProducer, error) {
	if p.closed.Load() {
		return nil, errors.New("pool closed")
	}

	select {
	case producer := <-p.idle:
		return producer, nil
	case p.sem <- token{}:
		producer, err := p.new()
		if err != nil {
			<-p.sem
		}
		return producer, err
	}
}

func (p *simpleProducerPool) new() (sarama.SyncProducer, error) {
	producerConfig := *p.config.OverwriteSaramaConfig
	producerConfig.Producer.Transaction.ID = fmt.Sprintf("producer-%s", watermill.NewUUID())
	p.logger.Debug("creating new producer", watermill.LogFields{"transaction_id": producerConfig.Producer.Transaction.ID})

	producer, err := newSyncProducer(p.logger, p.config.Brokers, &producerConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create producer: %w", err)
	}

	if p.config.Tracer != nil {
		producer.SyncProducer = p.config.Tracer.WrapSyncProducer(&producerConfig, producer.SyncProducer)
	}

	return producer, nil
}

func (p *simpleProducerPool) release(producer sarama.SyncProducer) {
	p.logger.Debug("releasing producer", watermill.LogFields{"txn_status": producer.TxnStatus().String()})
	alive, err := closeOnNotReady(producer)
	if err != nil {
		p.logger.Error("cannot close producer", err, nil)
	}
	if alive {
		p.idle <- producer
	} else {
		// remove one token from the semaphore to allow creating a new producer
		<-p.sem
	}
}

func (p *simpleProducerPool) close() error {

	p.logger.Trace("closing producerPool", nil)

	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(p.sem)
	close(p.idle)

	var errs []error
	for producer := range p.idle {
		if err := producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error while closing producerPool: cannot close producer: %w", err))
		}
	}
	p.logger.Trace("producerPool closed", nil)
	return errors.Join(errs...)
}

func closeOnNotReady(producer sarama.SyncProducer) (alive bool, err error) {
	// after aborting, the producer is somehow moved to the state "ProducerTxnStateInitializing", but will never be ready again
	// thus we close all producers that are not ready
	if producer.TxnStatus()&sarama.ProducerTxnFlagReady == 0 {
		if err := producer.Close(); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

type syncProducer struct {
	sarama.SyncProducer
	sync.Mutex
	closed bool
	logger watermill.LoggerAdapter
}

func (s *syncProducer) Close() error {
	s.logger.Debug("closing syncProducer", nil)
	if s.TryLock() {
		defer s.Unlock()
		return fmt.Errorf("called Close() on an unlocked producer")
	}
	s.closed = true
	return s.SyncProducer.Close()
}

func newSyncProducer(logger watermill.LoggerAdapter, addrs []string, config *sarama.Config) (*syncProducer, error) {
	attemptsRemaining := config.Producer.Transaction.Retry.Max
	var lastError error
	var producer sarama.SyncProducer
	for attemptsRemaining >= 0 {
		producer, lastError = sarama.NewSyncProducer(addrs, config)
		switch {
		case lastError == nil:
			return &syncProducer{SyncProducer: producer, logger: logger}, nil
		case errors.Is(lastError, sarama.ErrConcurrentTransactions):
			backoff := computeBackoff(config, attemptsRemaining)
			logger.Debug(
				fmt.Sprintf("newSyncProducer retrying after %dms... (%d attempts remaining)", backoff/time.Millisecond, attemptsRemaining),
				watermill.LogFields{"transaction_id": config.Producer.Transaction.ID, "error": lastError.Error()},
			)
			time.Sleep(backoff)
			attemptsRemaining--
			continue
		default:
			return nil, fmt.Errorf("cannot create producer: %w", lastError)
		}
	}
	return nil, lastError
}

func computeBackoff(saramaConfig *sarama.Config, attemptsRemaining int) time.Duration {
	if saramaConfig.Producer.Transaction.Retry.BackoffFunc != nil {
		maxRetries := saramaConfig.Producer.Transaction.Retry.Max
		retries := maxRetries - attemptsRemaining
		return saramaConfig.Producer.Transaction.Retry.BackoffFunc(retries, maxRetries)
	}
	return saramaConfig.Producer.Transaction.Retry.Backoff
}
