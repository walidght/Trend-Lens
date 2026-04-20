# TrendLens

Internal tool for me (agency owner) and my clients. SQLite-backed ETL that ingests social-media metrics from Apify, flags viral outliers per creator via Z-score, and extracts their audio hooks via Whisper. The downstream goal is to display outliers in Metabase and generate reports of popular hooks in a given niche at a given time.

## Hard architectural split

- **Streamlit (`app.py`)** is a **control panel only** — add sheets, sync creators, run scrapes, ingest CSVs, run AI insights. Never add analytics/dashboards/visualizations here.
- **Metabase** owns all viewing, charts, and reporting. If the user asks to "see" data, the answer is almost always "that belongs in Metabase."

## Running the app

- Start Streamlit control panel: `run_app.bat` (activates conda env `trend-lens`, runs `streamlit run app.py`).
- Start Metabase locally: `run_dashboard.bat` (misleading name — this launches the Metabase JAR, not the Streamlit app).
- First-time DB setup: `python init_db.py` (creates `data/trendlens.db` and tables via SQLAlchemy).

## Working agreements

1. **Plan before code.** On anything beyond a single-file tweak, explain your approach and wait for confirmation. The user has been burned by agents that code first and ask later.
2. **Confirm before spending money.** Apify scrapes and Whisper downloads cost real resources. Any UI action that triggers a scrape must go through an explicit confirmation step in the Streamlit UI — never kick off Apify implicitly after another action.
3. **Flag TODOs, don't fix them silently.** When you encounter a `TODO` or `FIXME` in code you're editing, surface it in your response instead of doing it on the side. Let the user decide whether it's in scope.
4. **Case-by-case on architecture.** Existing patterns (Repository in `core/repository.py`, platform specifics in `config/mappings.py`, orchestrators in `core/*.py`) are defaults, not laws. If you want to deviate, explain why and get confirmation first.
5. **No tests unless they fit.** A test suite is being added gradually. Write tests for new code when they make sense (business logic, data transformations) — do not write tests for Streamlit UI glue or trivial passthroughs. Older code gets backfilled on demand, not preemptively.

## Domain glossary

- **Creator**: a social account (unique by `(username, platform)`), tracked across sheets.
- **Sheet**: a client/niche grouping — maps to a published Google Sheet CSV listing creators.
- **Video metric**: one daily snapshot of views/likes/comments for one video (unique index on `video_id + DATE(scraped_at)`).
- **Outlier / viral video**: a video whose view count's Z-score (within its creator's recent baseline) exceeds the threshold (`AppConfig.z_score_threshold`, default 1.5).
- **Baseline**: per-creator mean/std of views over the last `baseline_days` (default 30). A creator needs history before they have a meaningful baseline — hence the backfill flow.
- **Backfill**: on first add, scrape `AppConfig.backfill_max_items` (21) recent posts per new creator to seed their baseline. Retryable via `last_scraped_at IS NULL`.
- **Hook**: the first few sentences (or full transcript) of a viral video's audio, extracted by Whisper and stored in `video_insights.hook_text`.

## Known broken state

- **`main.py` is broken.** Pre-Streamlit CLI entry point. Imports `InstagramAnalyzer` (renamed to `TrendAnalyzer`) and uses an `AppConfig(input_csv=..., output_csv=...)` constructor signature that no longer exists. Needs fixing — do not use it as a reference for current APIs.

## File map (where things live)

- `app.py` — single-file Streamlit control panel. Sidebar owns the active-sheet selector; sections below are scoped to it.
- `config/settings.py` — `AppConfig` (env-driven, validates required secrets).
- `config/mappings.py` — `PLATFORM_MAPPINGS` registry + `build_profile_url()` helper. All platform-specific column mappings, actor IDs, and URL templates go here.
- `core/database.py` — SQLAlchemy declarative models + `DatabaseManager`.
- `core/repository.py` — all SQL. Never write raw SQL elsewhere.
- `core/ingestors.py` — normalizes raw DataFrames (any platform) into the standard schema.
- `core/sheet_ingestor.py` — Google Sheet → DB sync, scrape-list generation.
- `core/apify_adapter.py` — wraps Apify client.
- `core/automation.py` — `AutomationOrchestrator`: `run_auto_sync` (due profiles) + `run_backfill` (targeted new creators).
- `core/pipeline.py` — `PipelineOrchestrator`: outlier detection → audio download → Whisper → save hook.
- `core/transcriber.py`, `core/downloader.py` — Whisper + audio download.
- `analyzers/trend_analyzer.py` — Z-score outlier detection.
