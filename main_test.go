package main

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/pixisai/metrics-aggregator/adapters/db"
)

func BenchmarkAggregate(b *testing.B) {
	ctx := context.Background()
	conn := dbConnect(ctx)
	defer conn.Close()
	q := db.New(conn)

	// Disable logging
	log.SetOutput(io.Discard)

	generateTestData(q, 1000, 100, 100, 100, 365)

	b.Run("Naive", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aggregateDataNaive(q, ctx)
		}
	})

	b.Run("ParallelAdAccount", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aggregateDataParallelAdAccounts(q, ctx)
		}
	})

	b.Run("ParallelCampaign", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aggregateDataParallelCampaigns(q, ctx)
		}
	})

	clearTestData(q, ctx)
}
