CREATE OR REPLACE EXTERNAL TABLE {{project_id}}.{{dataset_id}}.{{table_id}}
(
YearMonth STRING		
, Year STRING
, Month STRING
, Site STRING
, PeopleCostsUSD STRING
, PeopleCostsForecastOrActuals STRING
, AllSiteTotalHeads STRING
, SiteHeads STRING
, LicencingAndTelcoAllSiteCosts STRING
, LicencingAndTelcoPerSiteUSD STRING
, LicencingAndTelcoCostsForecastOrActuals STRING
, FullyLoadedCostsUSD STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['{{ sheet_uri }}'],
  skip_leading_rows = 1
);