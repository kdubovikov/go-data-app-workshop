package main

import (
	"context"
	"testing"
	"time"

	"github.com/pixisai/metrics-aggregator/adapters/db"
)

func TestCountMetricsForAd(t *testing.T) {
	ctx := context.Background()
	conn := dbConnect(ctx)
	defer conn.Close()
	q := db.New(conn)

	// setup test data
	err := q.CreateAdAccount(ctx, db.CreateAdAccountParams{
		ID:   777,
		Name: "777",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = q.CreateCampaign(ctx, db.CreateCampaignParams{
		ID:          777,
		AdAccountID: 777,
		Name:        "777",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = q.CreateAd(ctx, db.CreateAdParams{
		ID:         777,
		CampaignID: 777,
		Name:       "777",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = q.CreateMetric(ctx, db.CreateMetricParams{
		AdID:      777,
		Timestamp: int32(time.Now().Unix()),
		Value:     1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// test
	count, err := CountMetricsForAd(777)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}
