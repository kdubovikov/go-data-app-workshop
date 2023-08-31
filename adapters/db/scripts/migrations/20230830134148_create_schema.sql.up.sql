CREATE TABLE ad_account (
  id   SERIAL PRIMARY KEY,
  name text    NOT NULL
);

CREATE TABLE campaign (
  id   SERIAL PRIMARY KEY,
  name text    NOT NULL,
  ad_account_id INTEGER NOT NULL,
  FOREIGN KEY(ad_account_id) REFERENCES ad_account(id)
);

CREATE TABLE ad (
  id   SERIAL PRIMARY KEY,
  name text    NOT NULL,
  campaign_id INTEGER NOT NULL,
  FOREIGN KEY(campaign_id) REFERENCES campaign(id)
);

CREATE TABLE metric (
  id   SERIAL PRIMARY KEY,
  name text    NOT NULL,
  ad_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  value REAL NOT NULL,
  FOREIGN KEY(ad_id) REFERENCES ad(id)
)