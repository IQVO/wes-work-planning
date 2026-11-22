package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

// TestProductClassificationConsumerLogsNoEventDerivedLineBreaks feeds the
// decoder a payload that is not a CloudEvent and a ProductClassified whose
// id, subject and data carry CR/LF and whose data does not decode: no log
// attribute may carry the line break.
func TestProductClassificationConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &ProductClassificationConsumer{logger: slog.New(rec)}
	ctx := context.Background()
	_, _ = c.decode(ctx, kafkago.Message{Topic: "logsafe.topic", Value: []byte("not a CloudEvent" + crlfForged)})
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "id" + crlfForged, "type": cloudevents.TypeProductClassified, "source": "urn:logsafe",
		"subject": "subject" + crlfForged, "datacontenttype": "application/json",
		"data": []string{"not an object" + crlfForged},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.decode(ctx, kafkago.Message{Topic: "logsafe.topic", Key: []byte("key" + crlfForged), Value: b})
	rec.assertNoLineBreaks(t, 2)
}
