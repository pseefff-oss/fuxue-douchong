# IVE Cross-Platform Video Analytics

Data-driven member popularity analysis for K-pop group **IVE** across TikTok, YouTube Shorts, and Douyin.

**[View the Interactive Report](https://pseefff-oss.github.io/fuxue-douchong/)**

## Dataset

| Platform | Videos | Metric | Date Range |
|----------|--------|--------|------------|
| TikTok | 1,349 | Views, Likes, Comments, Shares | 2021-11 to 2026-02 |
| YouTube Shorts | 1,447 | Views, Likes, Comments | N/A |
| Douyin | 808 | Likes, Comments, Favorites, Shares | 2023-04 to 2026-02 |

**3,604 total videos** from IVE's official accounts. 1,623 solo videos (tagged to a single member) used for per-member analysis.

## What's Inside

### Interactive Report (`index.html`)
- 6 sections: All/Solo x TikTok/YouTube/Douyin
- 60 tab panels with sortable tables
- Bar charts synced with table sorting
- Distribution histograms (% of videos per range)
- Percentile curves on log scale (P1 to P99)
- Written analysis with normal and toxic versions

### Analysis Highlights

**The Hierarchy (by solo avg views):**

| Rank | TikTok | YouTube | Douyin (likes) |
|------|--------|---------|----------------|
| 1 | WONYOUNG (7.6M) | WONYOUNG (1.3M) | WONYOUNG (507K) |
| 2 | REI (4.2M) | YUJIN (1.1M) | LIZ (161K) |
| 3 | YUJIN (3.4M) | REI (952K) | LEESEO (100K) |
| 4 | GAEUL (2.8M) | GAEUL (695K) | GAEUL (74K) |
| 5 | LIZ (2.7M) | LIZ (680K) | YUJIN (67K) |
| 6 | LEESEO (2.1M) | LEESEO (670K) | REI (54K) |

**Key findings:**
- WONYOUNG is #1 on 17 of 19 metrics, on the lowest posting volume (177 solo videos)
- GAEUL has the highest YouTube engagement rate (7.0%) and comment rate — her audience is the most invested
- REI posts the most content (505 solo videos) but ranks last in YouTube engagement rate (5.67%)
- YUJIN ranks last in comment rate on both TikTok and YouTube despite top-3 views
- LIZ jumps from 5th globally to 2nd on Douyin — the Chinese market has different preferences
- LEESEO finishes last on 12 of 19 metrics across all platforms
- The Douyin hierarchy is completely different from TikTok/YouTube, with REI dropping from 2nd to last and LIZ/LEESEO gaining 3 ranks each

### Data Files

| File | Description |
|------|-------------|
| `ive_member_rankings.csv` | Per-member per-platform stats (avg, median, std, percentiles) |
| `ive_viral_top_videos.csv` | Top performing videos with member, views/likes, title |
| `ive_monthly_trends.csv` | Monthly posting frequency and performance |
| `ive_full_video_data.csv` | Every video with all fields (3,604 rows) |

### Analysis

| File | Description |
|------|-------------|
| `IVE_ANALYSIS.md` | Detailed cross-platform member analysis |
| `IVE_ANALYSIS_TOXIC.md` | Same analysis, less diplomatic |

## Tech

- **Data collection**: yt-dlp (TikTok/YouTube engagement), Douyin API, Playwright
- **Analysis**: Python stdlib only (json, csv, statistics, datetime)
- **Visualization**: Self-contained HTML with [Chart.js](https://www.chartjs.org/) via CDN
- **No dependencies** needed to run `analyze_ive.py` — just Python 3.10+

## Usage

```bash
# Regenerate the report from data
python analyze_ive.py

# Open the report
open ive_report.html
```

## Data Collection Date

February 2026
