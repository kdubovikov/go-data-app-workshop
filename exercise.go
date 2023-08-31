package main

import "fmt"

// CountMetricsForAd returns the number of metrics for the given ad.
func CountMetricsForAd(adId int) (int, error) {
	// Implement this function.
	// You can reuse dbConnect function from main.go to get the database connection
	// You will need to:
	// 1. Add your query to the adapters/db/scripts/queries.sql file
	// 2. Run go generate to generate the db package
	// 3. Use generated query wrapper to get metrics count for an ad
	// 4. Run the test and make sure that it passes
	return 0, fmt.Errorf("not implemented")
}
