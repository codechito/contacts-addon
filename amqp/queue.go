package amqp

// Package amqp wraps the github.com/streadway/amqp in an opinionated way
// for use with TransmitSMS and its components. We take the bits we need
// and leave out the bits we don't want.

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
)

// DefaultRetryScale is used as the default retry scale for retrying jobs
var DefaultRetryScale = []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute}

// Connection is a struct that wraps the AMQP connection context.
type Connection struct {
	amqpURI  string
	adminURI string

	amqpClient  *amqp.Connection
	adminClient *rabbithole.Client

	vhost string
}

// NewConnection creates a new connection to the AMQP server given by the URI.
//
// amqpURI should specify an AMQP URI: e.g. amqp://user:password@hostname:5672/vhost
//
// adminURI is only required to declare a sharded exchange, an empty string can be passed
//          in if not required.
func NewConnection(amqpURI, adminURI string) (*Connection, error) {
	con := &Connection{amqpURI: amqpURI, adminURI: adminURI}

	u, err := url.Parse(amqpURI)

	if err != nil {
		return con, err
	}

	con.vhost = u.Path

	if u.Path != "" {
		con.vhost = con.vhost[1:]
	}

	amqpClient, err := amqp.Dial(amqpURI)

	if err != nil {
		return con, err
	}

	con.amqpClient = amqpClient

	if adminURI == "" {
		return con, nil
	}

	con.adminClient, err = rhclient(con.adminURI)

	if err != nil {
		return con, err
	}

	return con, nil
}

// QueueConfig is used to supply configuration parameters to NewQueue.
type QueueConfig struct {
	// QueueName is the name of the queue.
	QueueName string

	// BindQueue tells RabbitMQ to map the routing key to the supplied queue for the
	// given exchange.
	BindQueue bool

	// Exchange is the name of the exchange.
	Exchange string

	// RouteKey is the routing key for BindQueue.
	RouteKey string
}

// DeclareQueue declares a queue, and optionally declares a retry queue and sets
// up a queue binding.
func (con *Connection) DeclareQueue(conf QueueConfig, args amqp.Table) error {
	ch, err := con.amqpClient.Channel()

	if err != nil {
		return err
	}

	defer ch.Close()

	_, err = ch.QueueDeclare(
		conf.QueueName,
		true,  // durable
		false, // delete
		false, // exclusive
		false, // noWait
		args,
	)

	if err != nil {
		return err
	}

	if conf.BindQueue {
		err = ch.QueueBind(conf.QueueName, conf.RouteKey, conf.Exchange, false /*noWait*/, nil)

		if err != nil {
			return err
		}
	}

	return nil
}

// RetryQueueConfig is used to supply configuration parameters to NewQueue.
type RetryQueueConfig struct {
	// Queue is the name of the queue. It will be used as a prefix when declaring the retry queue(s).
	Queue string

	SourceExChange string

	// Exchange is the name of the exchange.
	TargetExchange string

	RetryCount int
}

// DeclareRetryQueues sets the resource up with the bindigns for a retry queue to a target DLX
func (con *Connection) DeclareRetryQueues(conf RetryQueueConfig) error {
	ch, err := con.amqpClient.Channel()

	if err != nil {
		return err
	}

	defer ch.Close()

	for i := 1; i <= conf.RetryCount; i++ {
		queue, err := ch.QueueDeclare(
			fmt.Sprintf("%s%d", conf.Queue, i),
			true,  // durable
			false, // delete
			false, // exclusive
			false, // noWait
			amqp.Table{
				"x-dead-letter-exchange": conf.TargetExchange,
				"x-dead-letter-routing-key": conf.TargetExchange,
			},
		)
		if err != nil {
			return err
		}

		bindKey := fmt.Sprintf("#.retry-%d", i)
		err = ch.QueueBind(queue.Name, bindKey, conf.SourceExChange, false /*noWait*/, nil)

		if err != nil {
			return err
		}
	}

	return nil
}

// ExchangeConfig is used to supply parameters to DeclareExchange
type ExchangeConfig struct {
	// Exchange is the name of the exchange
	Exchange string

	// ExchangeType is the type of Exchange: "direct", "topic", "x-modulus-hash", etc.
	ExchangeType string

	// ShardsPerNode specifies the number of shards for a node,
	// where a node is a member of a RabbitMQ cluster (or a single
	// node if unclustered).
	ShardsPerNode int

	// ShardPolicy defines the name of the shard policy.
	ShardPolicy string
}

// DeclareExchange declares an AMQP Exchange based on the given ExchangeConfig.
func (con *Connection) DeclareExchange(conf ExchangeConfig) error {
	ch, err := con.amqpClient.Channel()

	if err != nil {
		return err
	}

	defer ch.Close()

	err = ch.ExchangeDeclare(
		conf.Exchange,
		conf.ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // noWait
		nil,
	)

	if err != nil {
		return err
	}

	if conf.ExchangeType == "x-modulus-hash" {
		return con.shardExchange(
			conf.Exchange,
			conf.ShardPolicy,
			conf.ShardsPerNode,
			"", // Note: this is ignored bec exchange-type is already x-modulus-hash. It's just defined
			// here so that cloudamqp won't give us error response.
			// (see https://github.com/rabbitmq/rabbitmq-sharding#about-the-routing-key-policy-definition)
		)
	}

	return nil
}

func (con *Connection) shardExchange(exchange, policyName string, shardsPerNode int, routingKey string) error {
	p := rabbithole.Policy{
		Vhost:      con.vhost,
		Pattern:    fmt.Sprintf("^%s$", exchange),
		Definition: rabbithole.PolicyDefinition{"shards-per-node": shardsPerNode, "routing-key": routingKey},
		ApplyTo:    "exchanges",
	}

	res, err := con.adminClient.PutPolicy(con.vhost, policyName, p)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(body)) != "" {
		rs := struct {
			Error  string `json:"error,omitempty"`
			Reason string `json:"reason,omitempty"`
		}{}

		if err = json.Unmarshal(body, &rs); err != nil && err != io.EOF {
			return fmt.Errorf("%s while unmarshalling response body: %s", err, body)
		}

		if strings.TrimSpace(rs.Error) != "" {
			return fmt.Errorf("%s in response body: %#v", err, rs)
		}
	}

	return nil
}

// Delivery specifies a received job.
type Delivery struct {
	val amqp.Delivery
	ch  *amqp.Channel
}

// Body returns the payload contained in the delivery.
func (d Delivery) Body() []byte {
	return d.val.Body
}

// Headers returns the headers sent with the AMQP message
func (d Delivery) Headers() map[string]interface{} {
	return d.val.Headers
}

// Priority Return the current Priority
func (d Delivery) Priority() uint8 {
	return d.val.Priority
}

// Ack notifies the queue the delivery is received.
func (d Delivery) Ack() error {
	defer d.Close()
	return d.val.Ack(false)
}

// Nack notifies the queue the delivery is rejected.
func (d Delivery) Nack(multiple, requeue bool) error {
	defer d.Close()
	return d.val.Nack(multiple, requeue)
}

// Close closes the underlying channel associated with the delivery.
func (d Delivery) Close() error {
	if d.ch == nil {
		return nil
	}

	return d.ch.Close()
}

func rhclient(adminURI string) (*rabbithole.Client, error) {
	ru, err := url.Parse(adminURI)

	if err != nil {
		return nil, err
	}

	user := ru.User.Username()
	pw, _ := ru.User.Password()

	if ru.Scheme == "https" {
		t := &http.Transport{TLSClientConfig: &tls.Config{}}
		return rabbithole.NewTLSClient(adminURI, user, pw, t)
	}

	return rabbithole.NewClient(adminURI, user, pw)
}

// Next Deprecated: This is found to be problematic and is causing "closed network connection" error
// when processing large amount of jobs. Please use Consume() function instead.
//
// Next returns a Delivery containing a message received from the queue.
// The caller must either call Deliver.Ack() to acknowledge the message
// (preventing a requeue of the message) and subsequently close the channel,
// or Delivery.Close() which requeues and redelivers the message.
//
// Note: you would use this instead of Connection.Consume() if you want to
//       (1) ensure that you only receive only a single message
//       (2) working with low-throughput queues
func (con *Connection) Next(queue string) (Delivery, error) {
	// Due to how RabbitMQ and AMQP work, a decision has been made to create/close
	// a channel for every job. This averts fetching a new incoming job which can
	// otherwise be delivered to another consumer. This matters especially if job
	// processing consumes an inordinate amount of time.
	ch, err := con.amqpClient.Channel()

	if err != nil {
		return Delivery{}, err
	}

	err = ch.Qos(1, 0, false)

	if err != nil {
		defer ch.Close()
		return Delivery{}, err
	}

	c, err := ch.Consume(
		queue,
		"",
		false, /*autoAck*/
		false, /*exclusive*/
		false, /*noLocal*/
		false, /*noWait*/
		nil,
	)

	if err != nil {
		defer ch.Close()
		return Delivery{}, err
	}

	d, ok := <-c

	if !ok {
		defer ch.Close()
		return Delivery{}, fmt.Errorf("unable to receive from amqp channel (backing socket was probably closed/terminated)")
	}

	// Tell rabbitmq to piss off with any incoming messages.
	// We're only interested in one at a time. RabbitMQ will
	// not deliver the same message to the same channel twice.
	go func() {
		for d := range c {
			d.Reject(true /*requeue*/)
		}
	}()

	return Delivery{val: d, ch: ch}, nil
}

// Consume should be used when high-throughput performance is required,
// and the consumer will not explicitly close the channel.
// It returns a Delivery channel. Only the Delivery.Ack method can be used.
func (con *Connection) Consume(queue string) (chan Delivery, error) {

	ch, err := con.amqpClient.Channel()

	if err != nil {
		return nil, err
	}

	// TODO: accept a prefetchCount parameter to improve throughput
	//       but prefetch of 1 is normally fine and should be the default.
	err = ch.Qos(1, 0, false)

	if err != nil {
		defer ch.Close()
		return nil, err
	}

	c, err := ch.Consume(
		queue,
		"",
		false, /*autoAck*/
		false, /*exclusive*/
		false, /*noLocal*/
		false, /*noWait*/
		nil,
	)

	if err != nil {
		defer ch.Close()
		return nil, err
	}

	dch := make(chan Delivery)

	go func() {
		for d := range c {
			dch <- Delivery{val: d}
		}
		// Need to ensure we close this copied channel if c is closed (i.e. range ends)
		// so that if the server closes the connection/channel the caller
		// can handle this by knowing dch is closed
		close(dch);
	}()

	return dch, nil
}

// PublishOptions specify headers and properties for a message before publishing.
type PublishOptions struct {
	// RouteKey specifies the routing key when publishing to exchange.
	RouteKey string

	// Exchange is the name of the exchange to publish.
	Exchange string

	// Headers specified for the message to be published.
	Headers map[string]interface{}

	// Message expiration in queue
	Expiration time.Duration

	Priority uint8
}

// Publish will send each message specified by data to the exchange
// using the supplied routing key.
func (con *Connection) Publish(options PublishOptions, body ...[]byte) error {
	ch, err := con.amqpClient.Channel()

	if err != nil {
		return err
	}

	// Enabling confirm mode is a performance killer but very necessary as
	// this will ensure Rabbit reliably queues every message delivered.
	// Without confirm mode we can see performance of ~15000 msgs/s but with
	// a loss rate of about 0.2%. However in our business, a non-zero loss is unacceptable.
	// Confirm mode drops performance to about ~500 msgs/s at 0.0% loss.
	err = ch.Confirm(false /*noWait*/)

	if err != nil {
		return err
	}

	defer ch.Close()

	confirm := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	p := amqp.Publishing{
		Headers:      amqp.Table(options.Headers),
		DeliveryMode: amqp.Persistent,
		Priority:     options.Priority,
	}

	if options.Expiration > 0 {
		// RabbitMQ expects Expiration specified in milliseconds
		p.Expiration = strconv.Itoa(int(options.Expiration.Nanoseconds() / 1e6))
	}

	for i, b := range body {
		p.Body = b
		err = ch.Publish(options.Exchange, options.RouteKey, false /*mandatory*/, false /*immediate*/, p)

		if err != nil {
			return fmt.Errorf("failed to publish message %d (%q): %s", i, string(b), err)
		}

		c, ok := <-confirm

		if !ok {
			return errors.New("confirmation channel unexpectedly closed (likely amqp timeout)")
		}

		if !c.Ack {
			return errors.New("expecting ack after publishing to amqp")
		}
	}

	return nil
}

// PublishRetryOptions specify headers and properties for a message before publishing.
type PublishRetryOptions struct {
	// RouteKey specifies the routing key when publishing to exchange.
	RouteKey string

	// Exchange is the name of the exchange to publish.
	Exchange string

	Delivery Delivery

	RetryScale []time.Duration

	Priority uint8
}

// PublishRetry send a retry job with exponential backoff
func (con *Connection) PublishRetry(options PublishRetryOptions, body ...[]byte) error {
	headers := options.Delivery.val.Headers

	retryCount := 0

	// Use length of the x-death array to determine how many times job has been retried
	// See https://www.rabbitmq.com/dlx.html
	if headers["x-death"] != nil {
		retries, _ := headers["x-death"]
		retryCount = len(retries.([]interface{}))
	}

	if retryCount >= len(options.RetryScale) {
		return fmt.Errorf("Message exceeded > 5 attempts so not retrying")
	}

	// Republish with new expiry to retry queue
	expiration := options.RetryScale[retryCount]
	retryCount++
	bindKey := fmt.Sprintf("%s.retry-%d", options.RouteKey, retryCount)
	opt := PublishOptions{
		RouteKey:   bindKey,
		Exchange:   options.Exchange,
		Headers:    headers,
		Expiration: expiration,
		Priority:   options.Priority,
	}

	return con.Publish(opt, body...)
}

// Reconnect reestablishes a connection to the AMQP server.
// It will disconnect a previous AMQP session if one already exists.
func (con *Connection) Reconnect() error {
	if con.amqpClient != nil {
		con.amqpClient.Close()
	}

	newc, err := NewConnection(con.amqpURI, con.adminURI)

	if err != nil {
		return err
	}

	*con = *newc

	return nil
}
