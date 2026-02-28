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

-- Creating a table for sample 100 stocks
-- Taking 50 30 20 split for large, mid and small cap

create table sample_set_100 as
(
select * from dimens_assets_details
where market_cap_cat = 'Large'
order by random()
limit 50
)
union all
(
select * from dimens_assets_details
where market_cap_cat = 'Mid'
order by random()
limit 30
)
union all
(
select * from dimens_assets_details
where market_cap_cat = 'Small'
order by random()
limit 20
);

-- Re_run to randomzie the sample 
----------------------------------
select * from sample_set_100;
-- Chk the distribution
select market_cap_cat, count(*) as stock_count
from sample_set_100
group by market_cap_cat
order by stock_count desc;
---------------------------------
select * from fact_fundamentals;
------- Find non_null count
select
count(pe_ratio) as pe,
count(peg_ratio) as peg,
count(roe_percent) as roe,
count(Gross_profits) as gr_profit,
count(Revenue_growth) as rev_gro,
count(Earnings_growth) as ear_gro,
count(Operating_margins) as op_marg,
count(EBITA_margins) as ebi_marg,
count(Debt_to_equity) as d_e,
count(Institutional_held_percent) as inst_hold,
count(Fifty_two_week_change) as week_change
from fact_fundamentals;
---------------------------------------------------

select * from fact_prices;
-- Check for total_days data collected
SELECT 
    d.ticker, p.asset_id,
    MIN(trade_date) as start_date, 
    MAX(trade_date) as end_date, 
    COUNT(*) as total_days
FROM fact_prices p
JOIN sample_set_100 d ON p.asset_id = d.asset_id
GROUP BY d.ticker,p.asset_id
ORDER BY p.asset_id ASC;
-----------------------------------------------------
select * from fact_news;
select n.asset_id,company_name,sentiment_score,news_summary
from fact_news n join sample_set_100 d
on n.asset_id = d.asset_id
order by n.asset_id asc;

select  count(distinct asset_id) from fact_news;

with news_table as (
select n.asset_id,company_name,sentiment_score,news_summary
from fact_news n join sample_set_100 d
on n.asset_id = d.asset_id
order by n.asset_id asc
)
select asset_id,ticker,company_name from sample_set_100
where asset_id not in (
select n.asset_id
from fact_news n join sample_set_100 d
on n.asset_id = d.asset_id
group by n.asset_id
order by n.asset_id asc
)
-------------------------------------------------

-- Filling the missing PEG, PE,roe and Debt null values with max number or zero.

select max(peg_ratio) as max_peg
from fact_fundamentals

create or replace view stock_scoring as
with fundamental_adj as (
select fa.asset_id,
COALESCE(case when fa.peg_ratio<=0 then 99 else fa.peg_ratio end,99) as adj_peg,
coalesce(fa.roe_percent,0) as adj_roe_percent,
coalesce(fa.debt_to_equity,5) as adj_debt_to_equity
from fact_fundamentals fa
),
sentiment_avg as(
select asset_id,
avg(sentiment_score) as avg_senti_score
from fact_news n
group by asset_id
)

select  d.asset_id,d.ticker,d.company_name,
		d.sector, d.industry,d.market_cap_cat,
		f.adj_peg,f.adj_roe_percent,
		f.adj_debt_to_equity,
		coalesce(s.avg_senti_score,0) as adj_senti_score ,

		rank() over (order by f.adj_peg asc) as peg_rank,
		rank() over (order by f.adj_roe_percent desc) as roe_rank,
		rank() over (order by f.adj_debt_to_equity asc) as de_rank

from sample_set_100 d left join fundamental_adj f
on d.asset_id = f.asset_id
left join sentiment_avg s 
on d.asset_id = s.asset_id;

select * from stock_scoring;










SELECT 
        f.asset_id,
        -- Clean PEG: If null or negative, assign a terrible score (99) to push it to the bottom
        COALESCE(CASE WHEN f.peg_ratio <= 0 THEN 99 ELSE f.peg_ratio END, 99) as clean_peg,
        -- Clean ROE: If null, assign 0
        COALESCE(f.roe_percent, 0) as clean_roe,
        -- Clean Debt: If null, assume high debt (e.g., 5.0) to penalize
        COALESCE(f.debt_to_equity, 5.0) as clean_debt
    FROM fact_fundamentals f




