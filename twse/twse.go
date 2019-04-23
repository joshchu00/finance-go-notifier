package twse

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/http"
	"github.com/joshchu00/finance-go-common/logger"
)

type Body struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

func Process(symbol string, period string, ts int64, strategy string, token string, chatID string, url string, text string, client *cassandra.Client) (err error) {

	logger.Info(fmt.Sprintf("%s: %d %s %s", "Starting twse.Process...", ts, symbol, strategy))

	var location *time.Location
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		return
	}

	dateString := datetime.GetDateString(ts, location)

	var sr *cassandra.StrategyRow

	sr, err = client.SelectStrategyRowByPrimaryKey(
		&cassandra.StrategyPrimaryKey{
			StrategyPartitionKey: cassandra.StrategyPartitionKey{
				Exchange: "TWSE",
				Symbol:   symbol,
				Period:   period,
			},
			Datetime: datetime.GetTime(ts, location),
		},
	)
	if err != nil {
		return
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}

	var value string
	switch strategy {
	case string(cassandra.StrategyColumnSSMA):
		value = fmt.Sprintf(text, dateString, symbol, sr.Name, strategy, sr.SSMA)
	case string(cassandra.StrategyColumnLSMA):
		value = fmt.Sprintf(text, dateString, symbol, sr.Name, strategy, sr.LSMA)
	}

	var bodyBytes []byte
	bodyBytes, err = json.Marshal(
		&Body{
			ChatID: chatID,
			Text:   value,
		},
	)
	if err != nil {
		return
	}

	_, err = http.Post(
		fmt.Sprintf(url, token),
		headers,
		bodyBytes,
	)

	time.Sleep(5 * time.Second)

	return
}
