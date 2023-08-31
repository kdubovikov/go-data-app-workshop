package main

// - Why consider Go?
// 	- Productive from day 1
// 	- Portable
// 	- Fast
// 		- Much less CPU and RAM footprint than Python
// 	- Handles concurrency very-vell
// 	- Good for mixing CPU and IO-bound workloads
// 	- Powerful toolchain. No need for formatters and linters, already built-in
// - Notable differences
// 	- Focused on using stdlib and limiting dependencies
// 	- Simple language with less features
// 	- Explicit error handling
// 	- Easy to write testable code, powerful automated testing tools right out-of-the-box
// 		- Unit tests
// 		- Fuzzy tests
// 		- Benchmarks
// - Running Go programs
// 	- `go run` - script mode
// 	- `go build` - compile to binary
// 		- `GOARCH`
// - Database access with Go

// go generate is a tool that automates the running of commands when you run "go generate".
//go:generate migrate -database postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable -path adapters/db/scripts/migrations up
//go:generate sqlc generate

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pixisai/metrics-aggregator/adapters/db"
)

const DB_URL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

func main() {
	ctx := context.Background()
	generateDataCmd := flag.NewFlagSet("generate-data", flag.ExitOnError)
	aggregateDataCmd := flag.NewFlagSet("aggregate-data", flag.ExitOnError)

	// generate-data flags
	nAdAccounts := generateDataCmd.Int("adacc", 400, "Number of AdAccounts to generate")
	nCampaigns := generateDataCmd.Int("camp", 100, "Number of Campaigns to generate")
	nAds := generateDataCmd.Int("ad", 1000, "Number of Ads to generate")
	nMetrics := generateDataCmd.Int("met", 1000, "Number of Metrics to generate")
	nDays := generateDataCmd.Int("days", 365, "Number of days of Metrics to generate")

	// Parse flags
	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Println("generate-data or aggregate-data subcommand is required")
		return
	}

	conn := dbConnect(ctx)
	defer conn.Close()

	q := db.New(conn)

	switch flag.Args()[0] {
	case "generate-data":
		generateDataCmd.Parse(flag.Args()[1:])
		generateTestData(q, *nAdAccounts, *nCampaigns, *nAds, *nMetrics, *nDays)
	case "aggregate-data":
		aggregateDataCmd.Parse(flag.Args()[1:])
		aggregateDataNaive(q, ctx)
		// aggregateDataParallelCampaigns(q, ctx))
		// aggregateDataParallelAdAccounts(q, ctx)
	default:
		fmt.Println("generate-data or aggregate-data subcommand is required")
		return
	}
}

// Generate test data in the database
func generateTestData(q *db.Queries, nAdAccounts int, nCampaigns int, nAds int, nMetrics int, nDays int) {
	ctx := context.Background()

	log.Println("Clearing test data")
	clearTestData(q, ctx)

	// Create AdAccounts
	log.Println("Generating test ad accounts")
	for i := 0; i < nAdAccounts; i++ {
		id := int32(i)
		adAccountName := fmt.Sprintf("AdAccount %d", i)
		err := q.CreateAdAccount(ctx, db.CreateAdAccountParams{
			ID:   id,
			Name: adAccountName,
		})
		if err != nil {
			panic(err)
		}
	}

	// Create Campaigns
	log.Println("Generating test campaigns")
	for i := 0; i < nCampaigns; i++ {
		id := int32(i)
		adAccountId := int32(i % nAdAccounts)
		campaignName := fmt.Sprintf("Campaign %d", i)
		err := q.CreateCampaign(ctx, db.CreateCampaignParams{
			ID:          id,
			Name:        campaignName,
			AdAccountID: adAccountId,
		})
		if err != nil {
			panic(err)
		}
	}

	// Create Ads
	log.Println("Generating test ads")
	for i := 0; i < nAds; i++ {
		id := int32(i)
		campaignId := int32(i % nCampaigns)
		adName := fmt.Sprintf("Ad %d", i)
		err := q.CreateAd(ctx, db.CreateAdParams{
			ID:         id,
			Name:       adName,
			CampaignID: campaignId,
		})
		if err != nil {
			panic(err)
		}
	}

	// createMetricsNaive(ctx, *q, nMetrics, nAds, nDays)
	createMetricsFast(ctx, *q, nMetrics, nAds, nDays)
	// createMetricsFastParallel(ctx, *q, nMetrics, nAds, nDays, 8)
}

// Clear all test data from the database
func clearTestData(q *db.Queries, ctx context.Context) {
	err := q.ClearMetrics(ctx)
	if err != nil {
		panic(err)
	}
	err = q.ClearAds(ctx)
	if err != nil {
		panic(err)
	}
	err = q.ClearCampaigns(ctx)
	if err != nil {
		panic(err)
	}
	err = q.ClearAdAccounts(ctx)
	if err != nil {
		panic(err)
	}
}

// Create Metrics with random values, each metric having nDays of values
func createMetricsNaive(ctx context.Context, q db.Queries, nMetrics int, nAds int, nDays int) {
	log.Println("Generating test metrics")
	// Create Metrics with random values, each metric having nDays of values
	startTime := time.Now()
	for i := 0; i < nMetrics; i++ {
		adId := int32(i % nAds)
		metricName := fmt.Sprintf("Metric %d", i)
		for j := 0; j < nDays; j++ {
			timestamp := 1609459200 + j*86400 // 2021-01-01 + j days
			err := q.CreateMetric(ctx, db.CreateMetricParams{
				Name:      metricName,
				AdID:      adId,
				Timestamp: int32(timestamp),
				Value:     10,
			})
			if err != nil {
				panic(err)
			}

			// Log the insert time per 10000 records
			if (i*nDays+j+1)%10000 == 0 {
				elapsedTime := time.Since(startTime)
				log.Printf("Inserted %d records in %s", i*nDays+j+1, elapsedTime)
				startTime = time.Now()

				estimateTillEnd := elapsedTime * time.Duration(nMetrics*nDays-i*nDays-j-1) / 10000
				log.Printf("Estimated time till end: %s", estimateTillEnd)
			}
		}
	}
}

// Create Metrics with random values, each metric having nDays of values
func createMetricsFast(ctx context.Context, q db.Queries, nMetrics int, nAds int, nDays int) {
	log.Println("Generating test metrics")
	// Create Metrics with random values, each metric having nDays of values
	params := make([]db.CreateMetricCopyFromParams, 0, nMetrics*nDays)
	for i := 0; i < nMetrics; i++ {
		adId := int32(i % nAds)
		metricName := fmt.Sprintf("Metric %d", i)
		for j := 0; j < nDays; j++ {
			timestamp := 1609459200 + j*86400 // 2021-01-01 + j days
			params = append(params, db.CreateMetricCopyFromParams{
				Name:      metricName,
				AdID:      adId,
				Timestamp: int32(timestamp),
				Value:     10,
			})
		}
	}

	log.Printf("Starting bulk insert of %d records", len(params))
	startTime := time.Now()
	nrows, err := q.CreateMetricCopyFrom(ctx, params)
	if err != nil {
		panic(err)
	}
	elapsedTime := time.Since(startTime)
	log.Printf("Inserted %d records in %s", nrows, elapsedTime)
}

// Create Metrics with random values, each metric having nDays of values
func createMetricsFastParallel(ctx context.Context, q db.Queries, nMetrics int, nAds int, nDays int, nWorkers int) {
	log.Println("Generating test metrics")
	// Create Metrics with random values, each metric having nDays of values
	params := make([]db.CreateMetricCopyFromParams, 0, nMetrics*nDays)
	for i := 0; i < nMetrics; i++ {
		adId := int32(i % nAds)
		metricName := fmt.Sprintf("Metric %d", i)
		for j := 0; j < nDays; j++ {
			timestamp := 1609459200 + j*86400 // 2021-01-01 + j days
			params = append(params, db.CreateMetricCopyFromParams{
				Name:      metricName,
				AdID:      adId,
				Timestamp: int32(timestamp),
				Value:     10,
			})
		}
	}

	log.Printf("Starting bulk insert of %d records using %d workers", len(params), nWorkers)
	startTime := time.Now()

	// Create a channel to distribute the work among the workers
	workCh := make(chan []db.CreateMetricCopyFromParams, nWorkers)
	var wg sync.WaitGroup
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go func() {
			defer wg.Done()
			for params := range workCh {
				nrows, err := q.CreateMetricCopyFrom(ctx, params)
				if err != nil {
					panic(err)
				}
				log.Printf("Inserted %d records", nrows)
			}
		}()
	}

	// Distribute the work among the workers
	chunkSize := (len(params) + nWorkers - 1) / nWorkers
	for i := 0; i < len(params); i += chunkSize {
		j := i + chunkSize
		if j > len(params) {
			j = len(params)
		}
		workCh <- params[i:j]
	}

	// Close the channel to signal the workers to exit
	close(workCh)

	// Wait for the workers to finish
	wg.Wait()

	elapsedTime := time.Since(startTime)
	log.Printf("Inserted %d records in %s using %d workers", len(params), elapsedTime, nWorkers)
}

// Aggregate data for all campaigns using a naive approach
func aggregateDataNaive(q *db.Queries, ctx context.Context) {
	startTime := time.Now()
	metricsAgg := make(map[int32]float64)

	// Get all ad account IDs
	log.Printf("Getting ad account IDs")
	adAccounts, err := q.GetAdAccounts(ctx)
	if err != nil {
		panic(err)
	}

	log.Printf("Processing %d ad accounts", len(adAccounts))
	for _, adAccount := range adAccounts {
		log.Printf("Processing ad account %d", adAccount.ID)
		// Get campaign IDs
		campaigns, err := q.GetCampaignsForAdAccount(ctx, adAccount.ID)
		if err != nil {
			panic(err)
		}

		log.Printf("Processing %d campaigns", len(campaigns))
		for j, campaign := range campaigns {
			startTime := time.Now()
			// Aggregate metrics
			aggValue, err := q.AggregateMetricsForCampaign(ctx, campaign.ID)
			if err != nil {
				panic(err)
			}

			// Store aggregated value
			metricsAgg[campaign.ID] = float64(aggValue.TotalValue)

			if j%3 == 0 {
				elapsedTime := time.Since(startTime)
				log.Printf("Processed %d campaigns in %s", j, elapsedTime)
				estimatedTime := elapsedTime * time.Duration(len(campaigns)-j) / 10
				log.Printf("Estimated time till end: %s", estimatedTime)
			}
		}
	}

	elapsedTime := time.Since(startTime)
	log.Printf("Aggregated data: %v in %s", metricsAgg, elapsedTime)
}

// Aggregate data for all campaigns in parallel for each ad account
func aggregateDataParallelAdAccounts(q *db.Queries, ctx context.Context) {
	startTime := time.Now()
	// Get all ad account IDs
	log.Printf("Getting ad account IDs")
	adAccounts, err := q.GetAdAccounts(ctx)
	if err != nil {
		panic(err)
	}

	// Create a channel to distribute the work among the workers
	workCh := make(chan db.AdAccount, len(adAccounts))
	for _, adAccount := range adAccounts {
		workCh <- adAccount
	}
	close(workCh)

	// Create a wait group to synchronize the workers
	var wg sync.WaitGroup

	// Process each ad account in parallel
	log.Printf("Processing %d ad accounts", len(adAccounts))
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for adAccount := range workCh {
				log.Printf("Processing ad account %d", adAccount.ID)
				// Get campaign IDs
				campaigns, err := q.GetCampaignsForAdAccount(ctx, adAccount.ID)
				if err != nil {
					panic(err)
				}

				log.Printf("Processing %d campaigns", len(campaigns))
				for _, campaign := range campaigns {
					// Aggregate metrics
					aggValue, err := q.AggregateMetricsForCampaign(ctx, campaign.ID)
					if err != nil {
						panic(err)
					}

					// Store aggregated value
					key := fmt.Sprintf("%d:%d", adAccount.ID, campaign.ID)
					value := float64(aggValue.TotalValue)
					log.Printf("Aggregated data for %s: %f", key, value)
				}
			}
		}()
	}

	// Wait for the workers to finish
	wg.Wait()
	elapsedTime := time.Since(startTime)
	log.Printf("Aggregated data in %s", elapsedTime)
}

// Aggregate data for all campaigns in parallel for each campaign
func aggregateDataParallelCampaigns(q *db.Queries, ctx context.Context) {
	startTime := time.Now()
	metricsAgg := make(map[int32]float64)

	// Get all ad account IDs
	log.Printf("Getting ad account IDs")
	adAccounts, err := q.GetAdAccounts(ctx)
	if err != nil {
		panic(err)
	}

	// Create a channel to distribute the work among the workers
	workCh := make(chan db.Campaign, 100)
	for _, adAccount := range adAccounts {
		campaigns, err := q.GetCampaignsForAdAccount(ctx, adAccount.ID)
		if err != nil {
			panic(err)
		}
		for _, campaign := range campaigns {
			workCh <- campaign
		}
	}
	close(workCh)

	// Create a wait group to synchronize the workers
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Process each campaign in parallel
	log.Printf("Processing campaigns")
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for campaign := range workCh {
				startTime := time.Now()
				// Aggregate metrics
				aggValue, err := q.AggregateMetricsForCampaign(ctx, campaign.ID)
				if err != nil {
					panic(err)
				}

				// Store aggregated value
				mu.Lock()
				metricsAgg[campaign.ID] = float64(aggValue.TotalValue)
				mu.Unlock()

				elapsedTime := time.Since(startTime)
				log.Printf("Processed campaign %d in %s", campaign.ID, elapsedTime)
			}
		}()
	}

	// Wait for the workers to finish
	wg.Wait()

	elapsedTime := time.Since(startTime)
	log.Printf("Aggregated data: %v in %s", metricsAgg, elapsedTime)
}

// Connect to the database
func dbConnect(ctx context.Context) *pgxpool.Pool {
	conn, err := pgxpool.New(ctx, DB_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	if err != nil {
		panic(err)
	}
	return conn
}
