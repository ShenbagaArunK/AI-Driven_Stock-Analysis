# AI-Driven Multi-Asset Intelligence & Portfolio Engine

An end-to-end stock intelligence pipeline for the Indian equity market (NSE / Nifty 500). It
ingests market data, stores it in a PostgreSQL star-schema warehouse, scores stocks with a
quantitative + sentiment ranking model to produce a curated **"Anchor 50"** list, generates
**LLM-written investment summaries**, exposes a **natural-language SQL agent**, and visualizes
the results in a **Power BI dashboard**.

> ⚠️ **Disclaimer:** This is an educational / portfolio project. It is **not** investment advice
> and is not produced by a SEBI-registered financial advisor. Do not use it to make real trading
> decisions.

---

## Architecture

```
ind_nifty500list.csv  (raw 500 tickers)
        │
        ▼  stock_list_generater.ipynb  ── yfinance: market cap, sector, industry
fetched_stocks.csv  (500 stocks, categorized Large / Mid / Small)
        │
        ▼  ingest.ipynb  ── push dimension table
PostgreSQL  ◄── stock_analysis_db.sql  (creates schema + sample_set_100, a 50/30/20 random split)
   ├── dimens_assets_details   (dimension: all 500 stocks)
   ├── sample_set_100          (working set of 100 stocks)
   ├── fact_fundamentals       (PE, PEG, ROE, D/E, margins …)        ◄─ ingest.ipynb
   ├── fact_prices             (2y close + volume)                   ◄─ ingest.ipynb
   ├── fact_news               (VADER sentiment on headlines)        ◄─ ingest.ipynb
   ├── stock_momentum  (view)  (SMA-50 / SMA-200 momentum signal)
   ├── stock_scoring   (view)  (rank PEG + ROE + D/E + sentiment)
   ├── anchor_50       (table) (top 50 by composite rank)            ◄─ ingest.ipynb
   ├── fact_ai_insights        (LLM 2-sentence BUY/HOLD/AVOID verdict) ◄─ processor.ipynb
   └── user_queries            (NL→SQL audit log)
        │
        ├──► processor.ipynb   Groq Llama-3.3-70b → per-stock insight
        ├──► llm_agent.ipynb   Gemini 2.5-flash SQL agent → NL questions
        └──► Bi/anchor_50_dashboard.pbix   Power BI dashboard
```

---

## Tech Stack

| Layer            | Tools |
|------------------|-------|
| Data source      | yfinance (NSE), Nifty 500 CSV |
| Processing       | Python, pandas, numpy |
| Sentiment        | vaderSentiment |
| Storage          | PostgreSQL (SQLAlchemy + psycopg2) |
| LLM insights     | Groq Llama-3.3-70b (`langchain-groq`) |
| NL→SQL agent     | Google Gemini 2.5-flash (`langchain-google-genai`) |
| Orchestration    | Jupyter notebooks, python-dotenv |
| BI               | Power BI (`.pbix`) |

---

## Project Structure

```
.
├── data/
│   ├── ind_nifty500list.csv      # Raw Nifty 500 constituents (input)
│   └── fetched_stocks.csv        # Generated: 500 stocks w/ market-cap category
├── scripts/
│   ├── stock_list_generater.ipynb  # Stage 1: build the stock universe
│   ├── ingest.ipynb                # Stage 2-3: fetch data + score → Anchor 50
│   ├── processor.ipynb             # Stage 4: LLM insight generation (Groq)
│   ├── llm_agent.ipynb             # Stage 5: NL→SQL agent (Gemini)
│   └── .env                        # Your secrets (NOT committed)
├── sql/
│   ├── stock_analysis_db.sql     # Full schema, sample set, scoring views, Anchor 50
│   ├── database_schema.sql       # Database bootstrap
│   └── database_analysis.sql     # Ad-hoc analysis queries
├── Bi/
│   └── anchor_50_dashboard.pbix  # Power BI dashboard
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Prerequisites
- Python 3.13+
- PostgreSQL 14+ (running locally or remotely)
- Power BI Desktop (to open the dashboard)
- API keys: a [Groq](https://console.groq.com/) key and a [Google AI Studio](https://aistudio.google.com/) (Gemini) key

### 2. Install dependencies
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure environment variables
Create `scripts/.env` (already gitignored) with:
```dotenv
# Database Credentials
DB_USER=your_pg_user
DB_PASS=your_pg_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stocks_fetched

# API Keys
GOOGLE_API_KEY=your_gemini_api_key
GROQ_API_KEY=your_groq_api_key
```

### 4. Create the database schema
Run the schema script against your PostgreSQL instance:
```bash
psql -U your_pg_user -d stocks_fetched -f sql/stock_analysis_db.sql
```
This creates the dimension/fact tables, the `sample_set_100` working set, the scoring and
momentum views, and the `anchor_50` table.

---

## How to Run (pipeline order)

Run the notebooks **in this order** (each depends on the previous):

| # | Notebook | What it does |
|---|----------|--------------|
| 1 | `scripts/stock_list_generater.ipynb` | Fetches market cap / sector for the Nifty 500, categorizes into Large / Mid / Small, writes `data/fetched_stocks.csv`. |
| 2 | `scripts/ingest.ipynb` | Pushes the dimension table, fetches **fundamentals**, **2y price history**, and **news sentiment** (VADER) for the 100-stock sample, then runs the SQL scoring views and builds the **Anchor 50**. |
| 3 | `scripts/processor.ipynb` | Generates a 2-sentence **BUY / HOLD / AVOID** insight per stock with Groq Llama-3.3-70b → `fact_ai_insights`. |
| 4 | `scripts/llm_agent.ipynb` | Natural-language → SQL agent (Gemini) for ad-hoc questions over the warehouse. |
| 5 | `Bi/anchor_50_dashboard.pbix` | Open in Power BI Desktop and point it at your PostgreSQL instance to refresh. |

> **Note:** `sample_set_100` is re-randomized each time the SQL sample query runs, so the Anchor 50
> composition can change between runs unless you freeze the sample.

---

## Data Model

| Table / View | Type | Key columns |
|--------------|------|-------------|
| `dimens_assets_details` | dimension | asset_id, ticker, company_name, sector, industry, market_cap_cat, cap_value, is_anchor |
| `sample_set_100`        | table     | 100-stock stratified sample (50 Large / 30 Mid / 20 Small) |
| `fact_fundamentals`     | fact      | pe_ratio, peg_ratio, roe_percent, debt_to_equity, margins, growth |
| `fact_prices`           | fact      | trade_date, close_price, volume (2 years) |
| `fact_news`             | fact      | sentiment_score (VADER compound), news_summary |
| `stock_momentum`        | view      | sma_50, sma_200, momentum_signal (Strongly Bullish → Strongly Bearish) |
| `stock_scoring`         | view      | per-metric ranks for PEG, ROE, D/E, sentiment |
| `anchor_50`             | table     | top 50 stocks by summed rank (lower = better) |
| `fact_ai_insights`      | fact      | ai_summary (LLM verdict) |
| `user_queries`          | audit     | generated_sql, executed_at |

**Anchor 50 scoring logic:** each stock is ranked on PEG (asc), ROE (desc), Debt-to-Equity (asc),
and average news sentiment (desc). The four ranks are summed; the 50 stocks with the **lowest total
rank** become the Anchor 50.

---

## Known Limitations

- **Sentiment is thin** — only the latest ~2 headlines per stock, scored with VADER (tuned for
  social media, not financial text).
- **Equal-weight ranking** — PEG, ROE, D/E and sentiment are weighted equally with no normalization.
- **yfinance gaps** — some fundamentals (e.g. PEG) are frequently missing and fall back to manual
  calculation or sentinel values.
- **Reproducibility** — the random sample is not seeded, so re-runs can shift the Anchor 50.

---

## License

Educational / personal project. No warranty. Not financial advice.
