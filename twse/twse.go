package twse

import (
	"fmt"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/logger"
)

func Process(symbol string, period string, ts int64, strategy string, client *cassandra.Client) (err error) {

	logger.Info(fmt.Sprintf("%s: %d %s %s", "Starting twse.Process...", ts, symbol, strategy))

	return
}
