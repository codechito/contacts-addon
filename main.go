package main

import (
	"log"
	"fmt"
	"time"
	"net/http"
	"strings"
	"github.com/BurntSushi/toml"
	"github.com/codechito/contacts-addon/amqp"
	"github.com/ip2location/ip2location-go"
)

var (
	httpClient http.Client
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Rabbit struct {
	URI      string `toml:"uri"`
	AdminURI string `toml:"admin_uri"`
}

type RabbitQueue struct {
	Queue        string `toml:"queue"`
	Exchange     string `toml:"exchange"`
	ExchangeType string `toml:"exchange_type"`
	RouteKey     string `toml:"route_key"`
	MaxPriority  byte   `toml:"max_priority"`
	ShardNodes   int    `toml:"shard_nodes"`
	ShardPolicy  string `toml:"shard_policy"`
}

type bespokeServices struct {
	ac    *amqp.Connection
}

var (
	cc bespokeServices
)

func readConfig(pathname string) (bespokeServices, error) {

	p := struct {
		Rabbit   Rabbit           `toml:"rabbit"`
		IP2LQ    RabbitQueue      `toml:"linkhits"`
	}{}

	var c bespokeServices

	_, err := toml.DecodeFile(pathname, &p)
	if err != nil {
		return c, fmt.Errorf("decode %s: %s", pathname, err)
	}

	ac, err := amqp.NewConnection(p.Rabbit.URI, p.Rabbit.AdminURI)
	c.ac = ac

	return c, nil
}

func getLocation(ipaddress string) (ip2location.IP2Locationrecord){
	ip2location.Open("./IP-COUNTRY-REGION-CITY-LATITUDE-LONGITUDE-ZIPCODE-SAMPLE.BIN")

	results := ip2location.Get_all(ipaddress)

	return results
}

func sendNotification(location ip2location.IP2Locationrecord){

	req, err := http.NewRequest("POST",
		"https://hooks.slack.com/services/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",
		strings.NewReader("{'text':'Location updated: " + location.City + "/" + location.Region+ "'}"))
	if err != nil {
		log.Printf("error: %s", err)

	}
	req.Header.Set("content-type", "application/json")
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf("error: %s", err)
	}
	defer res.Body.Close()
}

func main() {

	var err error
	cc, err = readConfig("./config.toml")
	result := getLocation("8.8.8.8");
	sendNotification(result);
	if err != nil {
		log.Fatal(err)
	}

}
