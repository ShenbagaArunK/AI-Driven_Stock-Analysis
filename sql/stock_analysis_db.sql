-- 1.Asset summary table:
CREATE TABLE dimens_assets_details (
	Asset_Id Serial Primary key,
	Ticker VARCHAR(20) UNIQUE Not Null,
	Company_Name VARCHAR(255),
	Sector VARCHAR(100),
	Industry VARCHAR(100),
	Market_Cap_Cat VARCHAR(20),
	Cap_Value BIGINT,
	Is_Anchor BOOLEAN DEFAULT FALSE,
	Created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.Fundamental Metrics Table
CREATE TABLE fact_fundamentals(
	Stats_ID SERIAL PRIMARY KEY,
	Asset_Id INT REFERENCES dimens_assets_details(Asset_Id) ON DELETE CASCADE,
	As_of_Date DATE DEFAULT CURRENT_DATE,
	
	PE_ratio DECIMAL(10,2),
	PEG_ratio DECIMAL(10,2),
	ROE_percent DECIMAL(10,2),

	Gross_profits BIGINT,
	Revenue_growth DECIMAL(10,4),
	Earnings_growth DECIMAL(10,4),
	Operating_margins DECIMAL(10,4),
	EBITA_margins DECIMAL(10,4),

	Debt_to_equity DECIMAL(10,2),
	Institutional_held_percent DECIMAL(10,4),
	Fifty_two_week_change DECIMAL(10,4),
	UNIQUE(Asset_Id,AS_of_Date)
);

-- 3. Price Table (Moving Averages)
CREATE TABLE fact_prices(
	Price_Id SERIAL PRIMARY KEY,
	Asset_Id INT REFERENCES dimens_assets_details(Asset_Id) ON DELETE CASCADE,
	Trade_Date DATE,
	Close_Price DECIMAL(15,2),
	Volume BIGINT,
	UNIQUE(Asset_Id,Trade_Date)
);

-- 4.NEWS - Sentiment Score Table
CREATE TABLE fact_news (
	Sentiment_Id SERIAL PRIMARY Key,
	Asset_Id INT REFERENCES dimens_assets_details(Asset_Id) ON DELETE CASCADE,
	Scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	Sentiment_score DECIMAL(3,2),
	News_summary TEXT
);

-- 5. Queries Table 
CREATE TABLE user_queries (
	query_id SERIAL PRIMARY KEY,
	user_id Text,
	generated_sql TEXT,
	executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
----------------------------------------
TRUNCATE Table dimens_assets_details restart identity cascade; --to reset to push data again
select * from dimens_assets_details;

-- If need to drop the tables:
drop table dimens_assets_details;
drop table fact_fundamentals;
drop table fact_prices;
drop table fact_news;
--------------------------------------

select company_name from dimens_assets_details
group by market_cap_cat,company_name
having market_cap_cat =	'Large';