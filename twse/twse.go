package twse

import (
	"fmt"
	"log"
	"time"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/logger"
)

var location *time.Location

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
}

func Process(symbol string, period string, ts int64, client *cassandra.Client) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse process...", datetime.GetTimeString(ts, location)))

	return
}
