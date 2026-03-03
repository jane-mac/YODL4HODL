# raw_data

This folder contains the core datasets used for FOMC decision modeling. All files are CSVs with a header row.

---

## fed_funds_target_daily.csv

Daily federal funds target rate history.

| Column | Description |
|--------|-------------|
| `date` | Date |
| `target_rate` | Fed funds target rate (single value, pre-2008) |
| `target_upper` | Upper bound of target range (post-2008) |
| `target_lower` | Lower bound of target range (post-2008) |
| `target_rate_unified` | Unified rate combining single target and midpoint of range |
| `rate_change_bps` | Change in rate in basis points |
| `decision` | FOMC decision (hike / cut / hold) |

- **Rows:** 13,205
- **Blank cells:** 20,131 (mostly `target_upper`/`target_lower` pre-2008 and `target_rate` post-2008)

---

## fed_futures_daily.csv

Daily time series of fed funds futures-implied rates and market expectations.

| Column | Description |
|--------|-------------|
| `date` | Date |
| `implied_rate_median` | Median implied fed funds rate from futures |
| `implied_rate_mode` | Mode implied fed funds rate from futures |
| `effective_ff_rate` | Effective federal funds rate |
| `expected_inflation_1y` | 1-year expected inflation |
| `expected_inflation_10y` | 10-year expected inflation |
| `ZQ` | Generic fed funds futures contract (ZQ) |
| `ZQM25` | March 2025 fed funds futures contract |
| `treasury_3m` | 3-month Treasury yield |
| `treasury_5y` | 5-year Treasury yield |
| `treasury_10y` | 10-year Treasury yield |
| `tbill_ff_spread` | T-bill to fed funds spread |
| `curve_10y3m` | 10y–3m yield curve spread |
| `curve_inverted` | Boolean: whether yield curve is inverted |
| `expected_rate_change` | Expected rate change implied by futures |
| `prob_hike` | Probability of a rate hike |
| `prob_cut` | Probability of a rate cut |
| `prob_hold` | Probability of no change |
| `ff_rate_5d_change` | 5-day change in fed funds rate |
| `ff_rate_21d_change` | 21-day change in fed funds rate |
| `implied_rate_volatility` | Volatility of the implied rate |
| `rate_deviation` | Deviation of effective rate from target |
| `expectation_momentum` | Momentum in rate expectations |

- **Rows:** 9,553
- **Blank cells:** 90,881

---

## fed_futures.csv

FOMC-meeting-frequency version of fed futures data (one row per meeting window). Same columns as `fed_futures_daily.csv`.

- **Rows:** 314
- **Blank cells:** 2,984

---

## fomc_bios.csv

Background and biographical data for FOMC members.

| Column | Description |
|--------|-------------|
| `name` | Member name |
| `wikipedia_title` | Wikipedia page title used for scraping |
| `universities` | Universities attended |
| `has_phd` | Boolean: holds a PhD |
| `has_jd_law` | Boolean: holds a JD/law degree |
| `has_mba` | Boolean: holds an MBA |
| `college_sports` | College sports participation (if any) |
| `political_family_connections` | Notable political family ties |
| `needs_review` | Flag for records needing manual review |

- **Rows:** 88
- **Blank cells:** 253

---

## fomc_decisions.csv

Historical FOMC rate decisions, one row per meeting.

| Column | Description |
|--------|-------------|
| `date` | Meeting date |
| `rate_before` | Fed funds rate before the decision |
| `rate_after` | Fed funds rate after the decision |
| `rate_change_bps` | Change in rate in basis points |
| `decision` | Decision label (hike / cut / hold) |

- **Rows:** 297
- **Blank cells:** 0

---

## fomc_membership.csv

FOMC voting membership composition at each meeting date.

| Column | Description |
|--------|-------------|
| `date` | Meeting date |
| `chair` | Fed Chair name |
| `num_governors` | Number of Board Governors voting |
| `num_bank_presidents` | Number of Reserve Bank Presidents voting |
| `avg_policy_stance` | Average hawk/dove score of voting members |
| `hawk_count` | Number of hawkish voters |
| `dove_count` | Number of dovish voters |
| `governor_names` | Names of voting Governors |
| `president_names` | Names of voting Bank Presidents |

- **Rows:** 297
- **Blank cells:** 0

---

## fomc_membership_backup.csv

Backup copy of `fomc_membership.csv`. Same columns and structure.

- **Rows:** 297
- **Blank cells:** 0

---

## fred_macro_data.csv

Monthly macroeconomic indicators pulled from FRED, aligned to FOMC meeting dates.

| Column | Description |
|--------|-------------|
| `date` | Date |
| `fed_funds_upper` | Fed funds upper target |
| `fed_funds_lower` | Fed funds lower target |
| `fed_funds_target` | Fed funds single target (pre-2008) |
| `fed_funds_effective` | Effective fed funds rate |
| `cpi` | Consumer Price Index (all items) |
| `core_pce` | Core PCE price index |
| `core_cpi` | Core CPI |
| `breakeven_5y` | 5-year breakeven inflation rate |
| `breakeven_10y` | 10-year breakeven inflation rate |
| `unemployment_rate` | Unemployment rate |
| `nonfarm_payrolls` | Nonfarm payroll employment |
| `initial_claims` | Initial jobless claims |
| `labor_force_part` | Labor force participation rate |
| `u6_unemployment` | U-6 broader unemployment measure |
| `real_gdp` | Real GDP |
| `industrial_production` | Industrial production index |
| `retail_sales` | Retail sales |
| `michigan_sentiment` | University of Michigan consumer sentiment |
| `personal_consumption` | Personal consumption expenditures |
| `treasury_1y` | 1-year Treasury yield |
| `treasury_2y` | 2-year Treasury yield |
| `treasury_5y` | 5-year Treasury yield |
| `treasury_10y` | 10-year Treasury yield |
| `treasury_30y` | 30-year Treasury yield |
| `yield_spread_10y2y` | 10y–2y yield spread |
| `yield_spread_10y3m` | 10y–3m yield spread |
| `baa_spread` | BAA corporate bond spread |
| `ted_spread` | TED spread |
| `housing_starts` | Housing starts |
| `case_shiller_index` | Case-Shiller home price index |
| `m2_money_supply` | M2 money supply |
| `fed_funds_change` | Change in fed funds rate |
| `rate_decision` | FOMC rate decision |
| `cpi_mom` | CPI month-over-month change |
| `cpi_yoy` | CPI year-over-year change |
| `core_pce_mom` | Core PCE month-over-month change |
| `core_pce_yoy` | Core PCE year-over-year change |
| `core_cpi_mom` | Core CPI month-over-month change |
| `core_cpi_yoy` | Core CPI year-over-year change |
| `nonfarm_payrolls_mom` | Nonfarm payrolls month-over-month change |
| `nonfarm_payrolls_yoy` | Nonfarm payrolls year-over-year change |
| `retail_sales_mom` | Retail sales month-over-month change |
| `retail_sales_yoy` | Retail sales year-over-year change |
| `industrial_production_mom` | Industrial production month-over-month change |
| `industrial_production_yoy` | Industrial production year-over-year change |
| `real_gdp_mom` | Real GDP month-over-month change |
| `real_gdp_yoy` | Real GDP year-over-year change |
| `housing_starts_mom` | Housing starts month-over-month change |
| `housing_starts_yoy` | Housing starts year-over-year change |
| `m2_money_supply_mom` | M2 month-over-month change |
| `m2_money_supply_yoy` | M2 year-over-year change |
| `yield_curve_inverted` | Boolean: whether yield curve is inverted |
| `unemployment_3m_change` | 3-month change in unemployment rate |

- **Rows:** 434
- **Blank cells:** 1,583

---

## google_trends_daily.csv

Daily Google Trends search volume indices for economic sentiment categories.

Columns track search interest for terms grouped into categories: **fear**, **inflation**, **fed**, **jobs**, **housing**, **desperation**, and **optimism** indicators. Also includes composite indices, z-scores, week-over-week and 4-week changes, and spike flags for each category.

- **Columns:** 58
- **Rows:** 574
- **Blank cells:** 17,816

<details>
<summary>Full column list</summary>

`date`, `fear_indicators_recession`, `fear_indicators_market crash`, `fear_indicators_layoffs`, `fear_indicators_unemployment`, `fear_indicators_depression`, `inflation_indicators_inflation`, `inflation_indicators_prices rising`, `inflation_indicators_cost of living`, `inflation_indicators_gas prices`, `inflation_indicators_grocery prices`, `fed_indicators_federal reserve`, `fed_indicators_interest rates`, `fed_indicators_fed rate hike`, `fed_indicators_fed rate cut`, `fed_indicators_fomc`, `job_indicators_jobs`, `job_indicators_hiring`, `job_indicators_job openings`, `job_indicators_job market`, `job_indicators_find a job`, `housing_indicators_housing market`, `housing_indicators_mortgage rates`, `housing_indicators_home prices`, `housing_indicators_housing crash`, `housing_indicators_buy a house`, `desperation_indicators_sell my car`, `desperation_indicators_pawn shop`, `desperation_indicators_payday loan`, `desperation_indicators_food stamps`, `desperation_indicators_bankruptcy`, `optimism_indicators_invest in stocks`, `optimism_indicators_buy stocks`, `optimism_indicators_stock market`, `optimism_indicators_retirement`, `optimism_indicators_401k`, `fed_chair_searches`, `fear_indicators_index`, `inflation_indicators_index`, `fed_indicators_index`, `job_indicators_index`, `housing_indicators_index`, `desperation_indicators_index`, `optimism_indicators_index`, `fear_optimism_ratio`, `desperation_zscore`, `fed_attention_zscore`, `fed_chair_zscore`, `fed_chair_spike`, `fear_indicators_recession_wow_change`, `fear_indicators_recession_4w_change`, `inflation_indicators_inflation_wow_change`, `inflation_indicators_inflation_4w_change`, `fed_indicators_interest_rates_wow_change`, `fed_indicators_interest_rates_4w_change`, `job_indicators_jobs_wow_change`, `job_indicators_jobs_4w_change`, `recession_search_spike`

</details>

---

## google_trends.csv

FOMC-meeting-frequency aggregation of Google Trends data. For each search term, provides mean, max, and trend across the pre-meeting window.

- **Columns:** 172
- **Rows:** 130
- **Blank cells:** 10,242

---

## market_data_daily.csv

Daily market data including equity indices, volatility, yields, commodities, and derived metrics.

| Column | Description |
|--------|-------------|
| `date` | Date |
| `sp500` | S&P 500 index level |
| `sp500_volume` | S&P 500 trading volume |
| `vix` | CBOE Volatility Index |
| `dow_jones` | Dow Jones Industrial Average |
| `dow_jones_volume` | Dow Jones trading volume |
| `nasdaq` | NASDAQ Composite index level |
| `nasdaq_volume` | NASDAQ trading volume |
| `treasury_10y_yield` | 10-year Treasury yield |
| `treasury_30y_yield` | 30-year Treasury yield |
| `treasury_5y_yield` | 5-year Treasury yield |
| `treasury_3m_yield` | 3-month Treasury yield |
| `dollar_index` | US Dollar Index (DXY) |
| `gold` | Gold price |
| `crude_oil` | Crude oil price |
| `high_yield_bonds` | High yield bond index |
| `investment_grade` | Investment grade bond index |
| `sp500_return_1d` | S&P 500 1-day return |
| `sp500_return_5d` | S&P 500 5-day return |
| `sp500_return_21d` | S&P 500 21-day return |
| `sp500_volatility_21d` | S&P 500 21-day realized volatility |
| `sp500_52w_high` | S&P 500 52-week high |
| `sp500_52w_low` | S&P 500 52-week low |
| `sp500_pct_from_high` | S&P 500 % below 52-week high |
| `vix_change_1d` | VIX 1-day change |
| `vix_change_5d` | VIX 5-day change |
| `vix_ma_21d` | VIX 21-day moving average |
| `vix_high_regime` | Boolean: VIX in high-volatility regime |
| `vix_extreme` | Boolean: VIX at extreme level |
| `yield_curve_10y3m` | 10y–3m yield curve spread |
| `yield_curve_inverted` | Boolean: whether yield curve is inverted |
| `yield_curve_10y5y` | 10y–5y yield curve spread |
| `hy_ig_ratio` | High yield to investment grade ratio |
| `hy_ig_ratio_change` | Change in HY/IG ratio |
| `gold_return_21d` | Gold 21-day return |
| `dollar_return_21d` | Dollar index 21-day return |

- **Rows:** 9,218
- **Blank cells:** 45,981

---

## market_data.csv

FOMC-meeting-frequency version of market data. Same columns as `market_data_daily.csv`.

- **Rows:** 434
- **Blank cells:** 1,937

---

## news_sentiment.csv

News sentiment metrics aggregated over the pre-FOMC-meeting window, one row per meeting.

| Column | Description |
|--------|-------------|
| `decision_date` | FOMC meeting date |
| `window_start` | Start of the news window |
| `window_end` | End of the news window |
| `total_articles` | Total articles in the window |
| `fed_related_articles` | Number of Fed-related articles |
| `avg_sentiment` | Average sentiment score |
| `sentiment_std` | Standard deviation of sentiment scores |
| `positive_pct` | Share of articles with positive sentiment |
| `negative_pct` | Share of articles with negative sentiment |
| `avg_hawkish` | Average hawkish tone score |
| `avg_dovish` | Average dovish tone score |
| `hawk_dove_balance` | Net hawk minus dove score |
| `market_sentiment` | Overall market sentiment composite |
| `pre_labeled_sentiment` | Pre-labeled sentiment category |
| `fed_pre_labeled_sentiment` | Fed-specific pre-labeled sentiment |

- **Rows:** 220
- **Blank cells:** 97
