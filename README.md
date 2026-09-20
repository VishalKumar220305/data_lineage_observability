# Data Lineage & Observability Dashboard

A small e-commerce data pipeline (built on the Olist public dataset) with a
self-built observability layer sitting on top of it. Automatic lineage
tracing, data quality checks, and alerting, so that when something breaks you
find out from the system instead of from a confused business user three weeks
later.



## Why I built this

Most portfolio projects that touch a warehouse stop at "here's a star schema
and a dashboard." The part that's actually hard in real data teams isn't
building the pipeline, it's knowing when the pipeline is lying to you. So
instead of another dashboard project, I built the thing that watches the
pipeline: where does this table's data actually come from, is it still
correct today, and if it isn't, what else does that break.

I didn't want to just claim it works, either. There's a `failure_injection/`
folder in here with scripts that deliberately break real things: drop a
column, corrupt some foreign keys, simulate stale data. The DQ engine catches
all three, for real, with real timestamps. That's what the GIF above is
showing.

## What's actually in here

There are 5 source tables — customers, orders, order items, products, sellers,
all from the Olist dataset — loaded with PySpark into a proper star schema
(`Dim_Customer`, `Dim_Product`, `Dim_Seller`, `Dim_Date`, `Fact_Order_Items`).
Nothing exotic, just a real dimensional warehouse at a small scale.

The interesting part sits on top of that. Lineage isn't hand-maintained
anywhere; a script reads the PySpark job files themselves using Python's `ast`
module (it never actually runs the code, just parses it) and figures out which
tables feed which. It's precise enough to get fan-in and fan-out right, so it
won't tell you a sellers table somehow feeds a customer dimension just because
they happen to live in the same script.

On top of that sits five kinds of data quality checks (nulls, duplicates,
referential integrity, schema drift, freshness), all driven by one YAML config
so adding a check to a new table doesn't mean writing new code. When a check
fails, the dashboard doesn't stop at "table X is broken" — it walks the
lineage graph and tells you every downstream table that's now potentially
wrong too. And it doesn't just sit in the dashboard waiting for someone to
look; failures trigger a real email.

The Streamlit dashboard ties all of this together so you're not querying
MySQL by hand to see any of it.

## Tech stack

PySpark (Hive-style warehouse, no real Hive server, just Spark's embedded
metastore), MySQL for metadata only (run history, lineage edges, DQ results),
Python's `ast` module, networkx, Streamlit + Plotly, smtplib for alerting.

## Project structure

```
pipeline/           the actual data pipeline (5 load jobs + warehouse build)
lineage/             the AST parser and the lineage graph / blast radius logic
data_quality/        the 5 check types + the orchestrator + MTTD calculation
alerting/             email notifications
dashboards/           the Streamlit app
failure_injection/   scripts that deliberately break things, to prove detection works
config/               everything configurable lives in one config.yaml
docs/                 architecture notes, data dictionary, and the reasoning behind the harder decisions
```

## Running it locally

You'll need Python, MySQL, and Java 17 specifically. Spark doesn't yet play
nice with Java 21+, which trips up a lot of people trying to run PySpark on a
newer machine — ask me about this if you hit it, I have a working fix.

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your real MySQL and (optionally)
Gmail SMTP details. Then:

```bash
# set up the metadata store
mysql -u your_user -p your_db < metadata_db/schema.sql

# run the pipeline
python -m pipeline.jobs.load_customers
python -m pipeline.jobs.load_orders
python -m pipeline.jobs.load_order_items
python -m pipeline.jobs.load_products
python -m pipeline.jobs.load_sellers
python -m pipeline.jobs.build_warehouse

# extract lineage
python -m lineage.parser.lineage_builder

# run the DQ suite
python -m data_quality.dq_runner

# see it all
streamlit run dashboards/streamlit_app/app.py
```

Want to see it actually catch something? Try:

```bash
python -m failure_injection.inject_schema_drift
python -m data_quality.dq_runner
```

then refresh the dashboard.

## Where the data comes from

The [Olist Brazilian E-Commerce Public
Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on
Kaggle. Not included in this repo (it's not mine to redistribute) — download
it yourself and drop the CSVs in a `data/` folder.

## Docs

If you want the longer version of any of this, [`docs/architecture.md`](docs/architecture.md)
has the full system design and [`docs/data_dictionary.md`](docs/data_dictionary.md)
covers every table and column. [`docs/lineage_methodology.md`](docs/lineage_methodology.md)
is probably the most interesting one: it's where I explain why I made the
calls I made, including which ones I'd make differently at real company
scale. [`docs/interview_qna.md`](docs/interview_qna.md) is the questions I'd
expect to get asked about this, answered honestly rather than after the fact.

## What this isn't

Batch, not streaming — freshness means "as of the last run," not live. It's
scoped to one pipeline; the config-driven design would probably generalize to
others, but I haven't actually tested that, so I'm not claiming it. The AST
parser can't resolve table names that are built dynamically at runtime — it
flags those instead of guessing, and you add the real mapping by hand.

None of that is hidden — it's all in `docs/lineage_methodology.md` if you
want the details.
