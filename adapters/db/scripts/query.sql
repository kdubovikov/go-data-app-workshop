-- name: GetAdAccounts :many
SELECT * FROM ad_account;

--#region
-- name: GetCampaignsForAdAccount :many
SELECT * FROM campaign WHERE ad_account_id = $1;
--#endregion

--#region
-- name: AggregateMetricsForCampaign :one
SELECT
  campaign.id,
  campaign.name,
  AVG(metric.value) AS total_value
FROM campaign
JOIN ad ON ad.campaign_id = campaign.id
JOIN metric ON metric.ad_id = ad.id
WHERE campaign.id = $1
GROUP BY campaign.id, campaign.name;

-- name: CreateAdAccount :exec
INSERT INTO ad_account (id, name) VALUES ($1, $2);

-- name: CreateCampaign :exec
INSERT INTO campaign (id, name, ad_account_id) VALUES ($1, $2, $3);

-- name: CreateAd :exec
INSERT INTO ad (id, name, campaign_id) VALUES ($1, $2, $3);

-- name: CreateMetric :exec
INSERT INTO metric (name, ad_id, timestamp, value) VALUES ($1, $2, $3, $4);

-- name: CreateMetricCopyFrom :copyfrom
INSERT INTO metric (name, ad_id, timestamp, value) VALUES ($1, $2, $3, $4);

-- name: ClearMetrics :exec
DELETE FROM metric;

-- name: ClearAds :exec
DELETE FROM ad;

-- name: ClearCampaigns :exec
DELETE FROM campaign;

-- name: ClearAdAccounts :exec
DELETE FROM ad_account;
--#endregion