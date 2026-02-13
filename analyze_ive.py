"""
IVE Cross-Platform Analysis
Analyzes member popularity across TikTok, YouTube Shorts, and Douyin.
Outputs: terminal summary, JSON, CSV files, HTML report with Chart.js.
Usage: python analyze_ive.py
"""

import csv
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).parent

MEMBER_PATTERNS = {
    "WONYOUNG": [r"wonyoung", r"원영", r"장원영", r"JANGWONYOUNG", r"张元英", r"ウォニョン"],
    "YUJIN": [r"yujin", r"유진", r"안유진", r"ANYUJIN", r"安宥真", r"ユジン"],
    "REI": [r"\brei\b", r"레이", r"怜", r"レイ"],
    "GAEUL": [r"gaeul", r"가을", r"秋", r"ガウル"],
    "LIZ": [r"\bliz\b", r"리즈", r"丽兹", r"リズ"],
    "LEESEO": [r"leeseo", r"이서", r"李瑞", r"イソ"],
}

MEMBERS_ORDER = ["WONYOUNG", "YUJIN", "REI", "GAEUL", "LIZ", "LEESEO", "GROUP/UNKNOWN"]

MEMBER_COLORS = {
    "WONYOUNG": "#FF6B9D",
    "YUJIN": "#C084FC",
    "REI": "#60A5FA",
    "GAEUL": "#34D399",
    "LIZ": "#FBBF24",
    "LEESEO": "#FB923C",
    "GROUP/UNKNOWN": "#94A3B8",
}


# ─── Data Loading ───────────────────────────────────────────────────────────


def parse_views(view_str: str) -> int:
    if not view_str:
        return 0
    clean = view_str.replace(" views", "").replace(",", "").strip()
    try:
        num = float(re.sub(r"[KMBkmb]", "", clean))
        if "B" in view_str or "b" in view_str:
            num *= 1e9
        elif "M" in view_str or "m" in view_str:
            num *= 1e6
        elif "K" in view_str or "k" in view_str:
            num *= 1e3
        return int(num)
    except (ValueError, TypeError):
        return 0


def tiktok_id_to_date(video_id: str) -> datetime | None:
    try:
        ts = int(video_id) >> 32
        if 1_500_000_000 < ts < 2_000_000_000:
            return datetime.fromtimestamp(ts)
    except (ValueError, OSError):
        pass
    return None


def detect_members(title: str) -> list[str]:
    if not title:
        return ["GROUP/UNKNOWN"]
    found = []
    for member, patterns in MEMBER_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, title, re.IGNORECASE):
                found.append(member)
                break
    return found if found else ["GROUP/UNKNOWN"]


def parse_douyin_likes(val) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        clean = val.replace(",", "").strip()
        try:
            if "万" in clean:
                return int(float(clean.replace("万", "")) * 10000)
            elif "K" in clean.upper():
                return int(float(re.sub(r"[Kk]", "", clean)) * 1000)
            elif "M" in clean.upper():
                return int(float(re.sub(r"[Mm]", "", clean)) * 1_000_000)
            return int(float(clean))
        except (ValueError, TypeError):
            return 0
    return 0


def load_data() -> dict:
    # TikTok + YouTube from ive_all_stats.json
    with open(BASE_DIR / "ive_all_stats.json", "r", encoding="utf-8") as f:
        all_stats = json.load(f)

    # Parse TikTok
    tiktok = []
    for v in all_stats.get("tiktok", []):
        entry = {
            "id": v["id"],
            "url": v.get("url", ""),
            "title": v.get("title", ""),
            "members": v.get("members", ["GROUP/UNKNOWN"]),
            "views_num": parse_views(v.get("views", "")),
            "views_str": v.get("views", ""),
            "likes": v.get("likes"),
            "comments": v.get("comments"),
            "shares": v.get("shares"),
            "platform": "tiktok",
        }
        dt = tiktok_id_to_date(v["id"])
        entry["date"] = dt.strftime("%Y-%m-%d") if dt else None
        entry["month"] = dt.strftime("%Y-%m") if dt else None
        tiktok.append(entry)

    # Parse YouTube
    youtube = []
    for v in all_stats.get("youtube", []):
        youtube.append({
            "id": v["id"],
            "url": v.get("url", ""),
            "title": v.get("title", ""),
            "members": v.get("members", ["GROUP/UNKNOWN"]),
            "views_num": parse_views(v.get("views", "")),
            "views_str": v.get("views", ""),
            "likes": v.get("likes"),
            "comments": v.get("comments"),
            "shares": v.get("shares"),
            "platform": "youtube",
            "date": None,
            "month": None,
        })

    # Douyin — merge all sources
    douyin_by_id = {}

    # Source 1: douyin_full_stats.json (API batch results, most complete)
    douyin_full = BASE_DIR / "douyin_full_stats.json"
    if douyin_full.exists():
        with open(douyin_full, "r", encoding="utf-8") as f:
            for v in json.load(f):
                vid = str(v["id"])
                dt = None
                if v.get("createTime"):
                    try:
                        dt = datetime.fromtimestamp(v["createTime"])
                    except (OSError, ValueError):
                        pass
                douyin_by_id[vid] = {
                    "id": vid,
                    "url": f"https://www.douyin.com/video/{vid}",
                    "title": v.get("desc", ""),
                    "likes": parse_douyin_likes(v.get("likes", 0)),
                    "comments": parse_douyin_likes(v.get("comments", 0)),
                    "favorites": parse_douyin_likes(v.get("favorites", 0)),
                    "shares": parse_douyin_likes(v.get("shares", 0)),
                    "plays": parse_douyin_likes(v.get("plays", 0)),
                    "date": dt.strftime("%Y-%m-%d") if dt else None,
                    "month": dt.strftime("%Y-%m") if dt else None,
                    "platform": "douyin",
                }

    # Source 2: douyin_stats.json (browser-scraped, 18 videos with engagement)
    douyin_browser = BASE_DIR / "douyin_stats.json"
    if douyin_browser.exists():
        with open(douyin_browser, "r", encoding="utf-8") as f:
            for v in json.load(f):
                vid = str(v.get("video_id", ""))
                if vid and vid not in douyin_by_id:
                    douyin_by_id[vid] = {
                        "id": vid,
                        "url": v.get("url", f"https://www.douyin.com/video/{vid}"),
                        "title": v.get("title", ""),
                        "likes": parse_douyin_likes(v.get("likes", 0)),
                        "comments": parse_douyin_likes(v.get("comments", 0)),
                        "favorites": parse_douyin_likes(v.get("favorites", 0)),
                        "shares": parse_douyin_likes(v.get("shares", 0)),
                        "plays": 0,
                        "date": None,
                        "month": None,
                        "platform": "douyin",
                    }

    # Detect members for Douyin
    douyin = []
    for v in douyin_by_id.values():
        v["members"] = detect_members(v["title"])
        douyin.append(v)

    # Sort douyin by likes descending
    douyin.sort(key=lambda x: x["likes"], reverse=True)

    print(f"Loaded: TikTok={len(tiktok)}, YouTube={len(youtube)}, Douyin={len(douyin)}")
    return {"tiktok": tiktok, "youtube": youtube, "douyin": douyin}


# ─── Analysis Functions ─────────────────────────────────────────────────────


def compute_member_stats(videos: list, metric_key: str) -> dict:
    """Compute per-member statistics."""
    member_videos = defaultdict(list)
    for v in videos:
        val = v.get(metric_key)
        if val is None:
            continue
        for m in v.get("members", ["GROUP/UNKNOWN"]):
            member_videos[m].append(val)

    result = {}
    for member in MEMBERS_ORDER:
        vals = member_videos.get(member, [])
        if not vals:
            continue
        sorted_vals = sorted(vals, reverse=True)
        result[member] = {
            "count": len(vals),
            "total": sum(vals),
            "mean": statistics.mean(vals),
            "median": statistics.median(vals),
            "stdev": statistics.stdev(vals) if len(vals) > 1 else 0,
            "min": min(vals),
            "max": max(vals),
            "p25": sorted_vals[int(len(sorted_vals) * 0.75)] if vals else 0,
            "p75": sorted_vals[int(len(sorted_vals) * 0.25)] if vals else 0,
            "p90": sorted_vals[int(len(sorted_vals) * 0.10)] if vals else 0,
            "p99": sorted_vals[int(len(sorted_vals) * 0.01)] if vals else 0,
            "top5_avg": statistics.mean(sorted_vals[:5]) if len(sorted_vals) >= 5 else statistics.mean(sorted_vals),
        }
    return result


def compute_viral_analysis(videos: list, metric_key: str, thresholds: list) -> dict:
    """Compute viral hit rates and top videos per member."""
    member_videos = defaultdict(list)
    for v in videos:
        val = v.get(metric_key)
        if val is None:
            continue
        for m in v.get("members", ["GROUP/UNKNOWN"]):
            member_videos[m].append({"value": val, "video": v})

    result = {"thresholds": thresholds, "hit_rates": {}, "top_videos": {}}

    for member in MEMBERS_ORDER:
        entries = member_videos.get(member, [])
        if not entries:
            continue
        total = len(entries)

        # Hit rates at each threshold
        rates = []
        for t in thresholds:
            above = sum(1 for e in entries if e["value"] >= t)
            rates.append({"threshold": t, "count": above, "rate": above / total if total else 0})
        result["hit_rates"][member] = rates

        # Top 10 videos
        sorted_entries = sorted(entries, key=lambda x: -x["value"])
        result["top_videos"][member] = [
            {
                "id": e["video"]["id"],
                "title": e["video"].get("title", "")[:80],
                "value": e["value"],
                "url": e["video"].get("url", ""),
            }
            for e in sorted_entries[:10]
        ]

    # Overall top 20
    all_sorted = sorted(videos, key=lambda x: -x.get(metric_key, 0))
    result["overall_top20"] = [
        {
            "id": v["id"],
            "title": v.get("title", "")[:80],
            "value": v.get(metric_key, 0),
            "members": v.get("members", []),
            "url": v.get("url", ""),
        }
        for v in all_sorted[:20]
    ]

    return result


def compute_time_trends(videos: list, metric_key: str) -> dict:
    """Compute monthly trends per member."""
    monthly = defaultdict(lambda: defaultdict(list))

    for v in videos:
        month = v.get("month")
        if not month:
            continue
        val = v.get(metric_key, 0)
        for m in v.get("members", ["GROUP/UNKNOWN"]):
            monthly[month][m].append(val)

    # Build sorted month list
    months = sorted(monthly.keys())

    # Build per-member trends
    trends = {}
    for member in MEMBERS_ORDER:
        series = []
        for month in months:
            vals = monthly[month].get(member, [])
            series.append({
                "month": month,
                "count": len(vals),
                "total": sum(vals),
                "avg": statistics.mean(vals) if vals else 0,
            })
        if any(s["count"] > 0 for s in series):
            trends[member] = series

    return {"months": months, "trends": trends}


# ─── Output: Terminal ───────────────────────────────────────────────────────


def fmt_num(n: float, decimals: int = 1) -> str:
    if n >= 1e9:
        return f"{n / 1e9:.{decimals}f}B"
    elif n >= 1e6:
        return f"{n / 1e6:.{decimals}f}M"
    elif n >= 1e3:
        return f"{n / 1e3:.{decimals}f}K"
    return f"{n:.0f}"


def print_terminal_summary(analysis: dict):
    print("\n" + "=" * 80)
    print("  IVE CROSS-PLATFORM MEMBER ANALYSIS")
    print("=" * 80)

    for platform, metric_label in [("tiktok", "Views"), ("youtube", "Views"), ("douyin", "Likes")]:
        rankings = analysis[f"{platform}_rankings"]
        viral = analysis[f"{platform}_viral"]

        print(f"\n{'─' * 80}")
        print(f"  {platform.upper()} — Member Rankings by {metric_label}")
        print(f"{'─' * 80}")
        print(f"  {'Member':<16} {'Videos':>7} {'Total':>10} {'Average':>10} {'Median':>10} {'Max':>10} {'Top5 Avg':>10}")
        print(f"  {'─' * 15} {'─' * 7} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")

        for member in MEMBERS_ORDER:
            s = rankings.get(member)
            if not s:
                continue
            print(
                f"  {member:<16} {s['count']:>7} {fmt_num(s['total']):>10} "
                f"{fmt_num(s['mean']):>10} {fmt_num(s['median']):>10} "
                f"{fmt_num(s['max']):>10} {fmt_num(s['top5_avg']):>10}"
            )

        # Viral hit rates
        thresholds = viral["thresholds"]
        print(f"\n  Viral Hit Rates:")
        header = f"  {'Member':<16}"
        for t in thresholds:
            header += f" {'>' + fmt_num(t, 0):>10}"
        print(header)
        print(f"  {'─' * 15}" + f" {'─' * 10}" * len(thresholds))

        for member in MEMBERS_ORDER:
            rates = viral["hit_rates"].get(member, [])
            if not rates:
                continue
            line = f"  {member:<16}"
            for r in rates:
                line += f" {r['count']:>4} ({r['rate']*100:>4.1f}%)"
            print(line)

        # Top 5 overall
        print(f"\n  Top 5 {platform.upper()} Videos:")
        for i, v in enumerate(viral["overall_top20"][:5], 1):
            members_str = ", ".join(v["members"])
            print(f"  {i}. {fmt_num(v['value']):>8} | {members_str:<16} | {v['title'][:50]}")

    # Douyin engagement by other metrics
    print(f"\n{'─' * 80}")
    print(f"  DOUYIN — Rankings by Comments / Favorites / Shares")
    print(f"{'─' * 80}")
    for metric_name, key in [("Comments", "douyin_comments_rankings"), ("Favorites", "douyin_favorites_rankings"), ("Shares", "douyin_shares_rankings")]:
        rankings = analysis[key]
        print(f"\n  {metric_name}:")
        print(f"  {'Member':<16} {'Videos':>7} {'Total':>10} {'Average':>10} {'Max':>10}")
        print(f"  {'─' * 15} {'─' * 7} {'─' * 10} {'─' * 10} {'─' * 10}")
        for member in MEMBERS_ORDER:
            s = rankings.get(member)
            if not s:
                continue
            print(f"  {member:<16} {s['count']:>7} {fmt_num(s['total']):>10} {fmt_num(s['mean']):>10} {fmt_num(s['max']):>10}")

    # Single-member analysis
    print(f"\n{'═' * 80}")
    print(f"  SINGLE-MEMBER VIDEO ANALYSIS")
    print(f"  (Only videos featuring exactly 1 identified member)")
    print(f"{'═' * 80}")
    for platform, metric_label, key in [
        ("tiktok", "Views", "solo_tiktok_rankings"),
        ("youtube", "Views", "solo_youtube_rankings"),
        ("douyin", "Likes", "solo_douyin_rankings"),
    ]:
        rankings = analysis[key]
        solo_count = analysis["solo_counts"].get(platform, 0)
        print(f"\n{'─' * 80}")
        print(f"  {platform.upper()} Solo Videos ({solo_count} total) — by {metric_label}")
        print(f"{'─' * 80}")
        print(f"  {'Member':<16} {'Videos':>7} {'Total':>10} {'Average':>10} {'Median':>10} {'Max':>10} {'Top5 Avg':>10}")
        print(f"  {'─' * 15} {'─' * 7} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")
        for member in MEMBERS_ORDER:
            s = rankings.get(member)
            if not s:
                continue
            print(
                f"  {member:<16} {s['count']:>7} {fmt_num(s['total']):>10} "
                f"{fmt_num(s['mean']):>10} {fmt_num(s['median']):>10} "
                f"{fmt_num(s['max']):>10} {fmt_num(s['top5_avg']):>10}"
            )

    # Time trends summary
    for platform in ["tiktok", "douyin"]:
        trends_key = f"{platform}_trends"
        if trends_key not in analysis:
            continue
        trends = analysis[trends_key]
        months = trends["months"]
        if not months:
            continue
        print(f"\n{'─' * 80}")
        print(f"  {platform.upper()} — Time Range: {months[0]} to {months[-1]}")
        print(f"{'─' * 80}")
        for member in MEMBERS_ORDER:
            series = trends["trends"].get(member, [])
            if not series:
                continue
            total_count = sum(s["count"] for s in series)
            total_val = sum(s["total"] for s in series)
            peak = max(series, key=lambda s: s["total"])
            if peak["total"] > 0:
                print(
                    f"  {member:<16} {total_count:>5} videos | Peak: {peak['month']} "
                    f"({peak['count']} videos, {fmt_num(peak['total'])} total)"
                )


# ─── Output: JSON ───────────────────────────────────────────────────────────


def save_json(analysis: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nSaved JSON: {path}")


# ─── Output: CSV ────────────────────────────────────────────────────────────


def save_csvs(analysis: dict, data: dict, base_dir: Path):
    # 1. Member rankings CSV
    rankings_path = base_dir / "ive_member_rankings.csv"
    with open(rankings_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "Platform", "Member", "Videos", "Total", "Average", "Median",
            "StdDev", "Min", "Max", "P25", "P75", "P90", "P99", "Top5Avg"
        ])
        for platform in ["tiktok", "youtube", "douyin"]:
            rankings = analysis[f"{platform}_rankings"]
            for member in MEMBERS_ORDER:
                s = rankings.get(member)
                if not s:
                    continue
                w.writerow([
                    platform.upper(), member, s["count"], round(s["total"]),
                    round(s["mean"]), round(s["median"]), round(s["stdev"]),
                    s["min"], s["max"], s["p25"], s["p75"], s["p90"], s["p99"],
                    round(s["top5_avg"]),
                ])
    print(f"Saved CSV: {rankings_path}")

    # 2. Top viral videos CSV
    viral_path = base_dir / "ive_viral_top_videos.csv"
    with open(viral_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Platform", "Rank", "Member", "Value", "Title", "URL"])
        for platform in ["tiktok", "youtube", "douyin"]:
            viral = analysis[f"{platform}_viral"]
            for i, v in enumerate(viral["overall_top20"], 1):
                w.writerow([
                    platform.upper(), i, "/".join(v["members"]),
                    v["value"], v["title"], v["url"],
                ])
    print(f"Saved CSV: {viral_path}")

    # 3. Monthly trends CSV
    trends_path = base_dir / "ive_monthly_trends.csv"
    with open(trends_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Platform", "Month", "Member", "VideoCount", "TotalMetric", "AvgMetric"])
        for platform in ["tiktok", "douyin"]:
            trends_key = f"{platform}_trends"
            if trends_key not in analysis:
                continue
            trends = analysis[trends_key]
            for member, series in trends["trends"].items():
                for s in series:
                    if s["count"] > 0:
                        w.writerow([
                            platform.upper(), s["month"], member,
                            s["count"], round(s["total"]), round(s["avg"]),
                        ])
    print(f"Saved CSV: {trends_path}")

    # 4. Full video data CSV
    full_path = base_dir / "ive_full_video_data.csv"
    with open(full_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "Platform", "ID", "Title", "Members", "Views", "Likes",
            "Comments", "Favorites", "Shares", "Date", "URL"
        ])
        for platform_key in ["tiktok", "youtube", "douyin"]:
            for v in data[platform_key]:
                w.writerow([
                    platform_key.upper(), v["id"], v.get("title", ""),
                    "/".join(v.get("members", [])),
                    v.get("views_num", ""), v.get("likes", ""),
                    v.get("comments", ""), v.get("favorites", ""),
                    v.get("shares", ""), v.get("date", ""), v.get("url", ""),
                ])
    print(f"Saved CSV: {full_path}")


# ─── HTML Table Builders ──────────────────────────────────────────────────


def _th(label, sort_type="num"):
    return f'<th data-sort="{sort_type}">{label} <span class="sort-arrow">&#x25B2;&#x25BC;</span></th>'


def _td_member(member):
    c = MEMBER_COLORS.get(member, "#666")
    return f'<td data-sort-value="{member}"><span class="member-tag" style="background:{c}22;border-color:{c}">{member}</span></td>'


def _td_num(value, display=None):
    if display is None:
        display = fmt_num(value) if isinstance(value, (int, float)) and abs(value) >= 1 else f"{value}"
    return f'<td class="num" data-sort-value="{value}">{display}</td>'


def _tbl_distribution(rankings, members_list):
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member")
    for c in ["Videos", "Total", "Average", "Median", "StdDev", "Min", "Max", "P25", "P75", "P90", "P99", "Top5 Avg"]:
        h += _th(c)
    h += '</tr></thead><tbody>'
    for m in members_list:
        s = rankings.get(m)
        if not s:
            continue
        h += f'<tr>{_td_member(m)}{_td_num(s["count"], str(s["count"]))}'
        for k in ["total", "mean", "median", "stdev", "min", "max", "p25", "p75", "p90", "p99", "top5_avg"]:
            h += _td_num(s[k], fmt_num(s[k]))
        h += '</tr>'
    h += '</tbody></table>'
    return h


_chart_counter = [0]


def _metric_panel(rankings, members_list, metric_label, videos=None, metric_key=None, section_title=""):
    """Distribution table + bar chart + distribution curve chart."""
    ctx = f"IVE {section_title}" if section_title else "IVE"
    table = _tbl_distribution(rankings, members_list)
    _chart_counter[0] += 1
    cid = f"mchart{_chart_counter[0]}"
    chart_items = []
    for m in members_list:
        s = rankings.get(m)
        if not s:
            continue
        chart_items.append({
            "m": m, "c": MEMBER_COLORS.get(m, "#666"),
            "count": s["count"], "total": s["total"], "mean": s["mean"],
            "median": s["median"], "stdev": s["stdev"], "min": s["min"],
            "max": s["max"], "p25": s["p25"], "p75": s["p75"],
            "p90": s["p90"], "p99": s["p99"], "top5_avg": s["top5_avg"],
        })
    chart_data = json.dumps(chart_items, ensure_ascii=False)
    bar_title = f"{ctx} — {metric_label} per Member (sorted by current column)"
    bar_canvas = (f'<div class="chart-wrap" style="margin-top:16px">'
                  f'<h4 style="color:#94a3b8;margin-bottom:8px;font-size:0.95em">{bar_title}</h4>'
                  f'<canvas id="{cid}" class="metric-chart" '
                  f"data-chart='{chart_data}' "
                  f'data-label="{metric_label}"></canvas></div>')

    # Distribution curve (histogram)
    dist_canvas = ""
    if videos and metric_key:
        all_vals = [v[metric_key] for v in videos if v.get(metric_key) is not None and v[metric_key] > 0]
        if all_vals:
            all_vals_sorted = sorted(all_vals)
            p95 = all_vals_sorted[min(int(len(all_vals_sorted) * 0.95), len(all_vals_sorted) - 1)]
            cap = max(p95, 1)
            num_bins = 20
            bin_w = cap / num_bins
            bin_edges = [round(i * bin_w) for i in range(num_bins + 1)]
            bin_labels = [fmt_num(e) for e in bin_edges[:-1]]

            hist_members = []
            for m in members_list:
                s = rankings.get(m)
                if not s or s["count"] == 0:
                    continue
                member_vals = [v[metric_key] for v in videos
                               if m in v.get("members", []) and v.get(metric_key) is not None]
                total_m = len(member_vals) if member_vals else 1
                counts = [0] * num_bins
                for val in member_vals:
                    placed = False
                    for i in range(num_bins):
                        if val < bin_edges[i + 1]:
                            counts[i] += 1
                            placed = True
                            break
                    if not placed:
                        counts[-1] += 1
                pcts = [round(c / total_m * 100, 1) for c in counts]
                hist_members.append({"m": m, "c": MEMBER_COLORS.get(m, "#666"), "pcts": pcts})

            _chart_counter[0] += 1
            did = f"dchart{_chart_counter[0]}"
            hist_data = json.dumps({"bins": bin_labels, "members": hist_members}, ensure_ascii=False)
            hist_title = f"{ctx} — {metric_label} Distribution: % of Each Member's Videos per {metric_label} Range"
            dist_canvas = (f'<div class="chart-wrap" style="margin-top:12px">'
                           f'<h4 style="color:#94a3b8;margin-bottom:8px;font-size:0.95em">'
                           f'{hist_title}</h4>'
                           f'<canvas id="{did}" class="dist-chart" '
                           f"data-hist='{hist_data}'></canvas></div>")

            # Percentile curve: x=P1..P99, y=value at that percentile
            pctl_points = [1, 5, 10, 25, 50, 75, 90, 95, 99]
            pctl_labels = [f"P{p}" for p in pctl_points]
            pctl_members = []
            for m in members_list:
                s = rankings.get(m)
                if not s or s["count"] == 0:
                    continue
                member_vals = sorted(
                    v.get(metric_key) for v in videos
                    if m in v.get("members", []) and v.get(metric_key) is not None
                )
                n = len(member_vals)
                if n == 0:
                    continue
                vals = []
                for p in pctl_points:
                    idx = min(int(n * p / 100), n - 1)
                    val = member_vals[idx]
                    vals.append(max(val, 1))  # floor to 1 for log scale
                pctl_members.append({"m": m, "c": MEMBER_COLORS.get(m, "#666"), "vals": vals})

            _chart_counter[0] += 1
            pid = f"pchart{_chart_counter[0]}"
            pctl_data = json.dumps({"labels": pctl_labels, "members": pctl_members}, ensure_ascii=False)
            pctl_title = f"{ctx} — {metric_label} Percentile Curve: Value at P1 to P99 per Member (Log Scale)"
            pctl_canvas = (f'<div class="chart-wrap" style="margin-top:12px">'
                           f'<h4 style="color:#94a3b8;margin-bottom:8px;font-size:0.95em">'
                           f'{pctl_title}</h4>'
                           f'<canvas id="{pid}" class="pctl-chart" '
                           f"data-pctl='{pctl_data}'></canvas></div>")
            dist_canvas += "\n" + pctl_canvas

    return table + "\n" + bar_canvas + "\n" + dist_canvas


def _tbl_viral_rates(viral, rankings, members_list):
    thresholds = viral["thresholds"]
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member") + _th("Videos")
    for t in thresholds:
        h += _th(f">{fmt_num(t, 0)} Cnt") + _th("Rate")
    h += '</tr></thead><tbody>'
    for m in members_list:
        rates = viral["hit_rates"].get(m, [])
        s = rankings.get(m, {})
        if not rates or not s:
            continue
        h += f'<tr>{_td_member(m)}{_td_num(s.get("count", 0), str(s.get("count", 0)))}'
        for r in rates:
            h += _td_num(r["count"], str(r["count"]))
            h += _td_num(r["rate"] * 100, f'{r["rate"]*100:.1f}%')
        h += '</tr>'
    h += '</tbody></table>'
    return h


def _tbl_tiers(videos, metric_key, tiers, members_list):
    mt = defaultdict(lambda: {t[0]: 0 for t in tiers})
    totals = defaultdict(int)
    members_set = set(members_list)
    for v in videos:
        val = v.get(metric_key, 0)
        for m in v.get("members", ["GROUP/UNKNOWN"]):
            if m not in members_set:
                continue
            totals[m] += 1
            for label, lo, hi in tiers:
                if lo <= val < hi:
                    mt[m][label] += 1
                    break
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member") + _th("Total")
    for label, _, _ in tiers:
        h += _th(label) + _th("%")
    h += '</tr></thead><tbody>'
    for m in members_list:
        t = totals.get(m, 0)
        if t == 0:
            continue
        h += f'<tr>{_td_member(m)}{_td_num(t, str(t))}'
        for label, _, _ in tiers:
            c = mt[m][label]
            p = c / t * 100 if t else 0
            h += _td_num(c, str(c)) + _td_num(p, f'{p:.1f}%')
        h += '</tr>'
    h += '</tbody></table>'
    return h


def _tbl_top20(viral, metric_label):
    top20 = viral.get("overall_top20", [])
    if not top20:
        return '<p class="note">No data</p>'
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("#") + _th("Member", "member") + _th(metric_label) + '<th>Title</th>'
    h += '</tr></thead><tbody>'
    for i, v in enumerate(top20, 1):
        ms = ", ".join(v["members"])
        fm = v["members"][0] if v["members"] else "GROUP/UNKNOWN"
        ts = v["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        url = v.get("url", "")
        h += f'<tr>{_td_num(i, str(i))}'
        c = MEMBER_COLORS.get(fm, "#666")
        h += f'<td data-sort-value="{fm}"><span class="member-tag" style="background:{c}22;border-color:{c}">{ms}</span></td>'
        h += f'{_td_num(v["value"], fmt_num(v["value"]))}'
        h += f'<td><a href="{url}" target="_blank">{ts}</a></td></tr>'
    h += '</tbody></table>'
    return h


def _tbl_member_top5(viral, members_list):
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member") + _th("#") + _th("Value") + '<th>Title</th>'
    h += '</tr></thead><tbody>'
    for m in members_list:
        top_vids = viral.get("top_videos", {}).get(m, [])
        for i, v in enumerate(top_vids[:5], 1):
            ts = v["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            url = v.get("url", "")
            h += f'<tr>{_td_member(m)}{_td_num(i, str(i))}'
            h += f'{_td_num(v["value"], fmt_num(v["value"]))}'
            h += f'<td><a href="{url}" target="_blank">{ts}</a></td></tr>'
    h += '</tbody></table>'
    return h


def _tbl_consistency(rankings, members_list):
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member") + _th("Videos")
    for c in ["Mean", "Median", "StdDev", "CV (σ/μ)", "Med/Mean", "Max/Mean", "P25/P75"]:
        h += _th(c)
    h += '</tr></thead><tbody>'
    for m in members_list:
        s = rankings.get(m, {})
        if not s:
            continue
        cv = s["stdev"] / s["mean"] if s["mean"] > 0 else 0
        mm = s["median"] / s["mean"] if s["mean"] > 0 else 0
        xm = s["max"] / s["mean"] if s["mean"] > 0 else 0
        pp = s["p25"] / s["p75"] if s["p75"] > 0 else 0
        h += f'<tr>{_td_member(m)}{_td_num(s["count"], str(s["count"]))}'
        h += _td_num(s["mean"], fmt_num(s["mean"]))
        h += _td_num(s["median"], fmt_num(s["median"]))
        h += _td_num(s["stdev"], fmt_num(s["stdev"]))
        h += _td_num(cv, f'{cv:.2f}')
        h += _td_num(mm, f'{mm:.2f}')
        h += _td_num(xm, f'{xm:.1f}x')
        h += _td_num(pp, f'{pp:.2f}')
        h += '</tr>'
    h += '</tbody></table>'
    return h


def _tbl_power_rankings(rankings, viral, members_list):
    metrics = [
        ("Total", lambda m: rankings.get(m, {}).get("total", 0)),
        ("Average", lambda m: rankings.get(m, {}).get("mean", 0)),
        ("Median", lambda m: rankings.get(m, {}).get("median", 0)),
        ("Max", lambda m: rankings.get(m, {}).get("max", 0)),
        ("Top5 Avg", lambda m: rankings.get(m, {}).get("top5_avg", 0)),
        ("Viral Rate", lambda m: viral["hit_rates"].get(m, [{}])[0].get("rate", 0) if viral.get("hit_rates") else 0),
    ]
    active = [m for m in members_list if rankings.get(m)]
    member_ranks = {m: {} for m in active}
    for name, fn in metrics:
        for rank, m in enumerate(sorted(active, key=fn, reverse=True), 1):
            member_ranks[m][name] = rank
    h = '<table class="data-table sortable"><thead><tr>'
    h += _th("Member", "member")
    for name, _ in metrics:
        h += _th(name)
    h += _th("Avg Rank")
    h += '</tr></thead><tbody>'
    for m in members_list:
        if m not in member_ranks:
            continue
        ranks = member_ranks[m]
        avg = sum(ranks.values()) / len(ranks) if ranks else 99
        h += f'<tr>{_td_member(m)}'
        for name, _ in metrics:
            r = ranks.get(name, "-")
            h += _td_num(r, f'#{r}')
        h += _td_num(avg, f'{avg:.1f}')
        h += '</tr>'
    h += '</tbody></table>'
    return h


def _build_section(sid, title, note, tabs):
    """tabs: list of (key, label, html_content) tuples."""
    h = f'<div class="section" id="{sid}">\n<h2>{title}</h2>\n'
    if note:
        h += f'<p class="note">{note}</p>\n'
    h += '<div class="tab-nav">\n'
    for i, (key, label, _) in enumerate(tabs):
        act = " active" if i == 0 else ""
        h += f'<button class="tab-btn{act}" data-target="{sid}-{key}">{label}</button>\n'
    h += '</div>\n'
    for i, (key, _, content) in enumerate(tabs):
        act = " active" if i == 0 else ""
        h += f'<div class="tab-panel{act}" id="{sid}-{key}">\n{content}\n</div>\n'
    h += '</div>\n'
    return h


# ─── Output: HTML Report ─────────────────────────────────────────────────


TT_TIERS = [("<1M", 0, 1e6), ("1-5M", 1e6, 5e6), ("5-10M", 5e6, 10e6), ("10-20M", 10e6, 20e6), ("20M+", 20e6, float("inf"))]
YT_TIERS = [("<500K", 0, 5e5), ("500K-2M", 5e5, 2e6), ("2-5M", 2e6, 5e6), ("5-10M", 5e6, 10e6), ("10M+", 10e6, float("inf"))]
DY_TIERS = [("<50K", 0, 5e4), ("50-200K", 5e4, 2e5), ("200-500K", 2e5, 5e5), ("500K-1M", 5e5, 1e6), ("1M+", 1e6, float("inf"))]


def generate_html(analysis: dict, data: dict, path: Path):
    members_all = MEMBERS_ORDER
    members_solo = [m for m in MEMBERS_ORDER if m != "GROUP/UNKNOWN"]
    solo = data.get("solo", {})

    # Summary stats
    total_tt = len(data["tiktok"])
    total_yt = len(data["youtube"])
    total_dy = len(data["douyin"])
    total_all = total_tt + total_yt + total_dy
    tt_total_views = sum(v["views_num"] for v in data["tiktok"])
    yt_total_views = sum(v["views_num"] for v in data["youtube"])
    dy_total_likes = sum(v.get("likes", 0) for v in data["douyin"])
    tt_dates = [v["date"] for v in data["tiktok"] if v.get("date")]
    tt_date_range = f"{min(tt_dates)} to {max(tt_dates)}" if tt_dates else "N/A"
    dy_dates = [v["date"] for v in data["douyin"] if v.get("date")]
    dy_date_range = f"{min(dy_dates)} to {max(dy_dates)}" if dy_dates else "N/A"


    # ── Helper: build tabs for TikTok/YouTube (likes first, then views/comments/shares) ──
    def _ttyt_tabs(videos, views_r, likes_r, comments_r, shares_r, viral, mlist, metric_key, tiers, stitle=""):
        return [
            ("likes", "Likes", _metric_panel(likes_r, mlist, "Likes", videos, "likes", stitle)),
            ("views", "Views", _metric_panel(views_r, mlist, "Views", videos, "views_num", stitle)),
            ("comments", "Comments", _metric_panel(comments_r, mlist, "Comments", videos, "comments", stitle)),
            ("shares", "Shares", _metric_panel(shares_r, mlist, "Shares", videos, "shares", stitle)),
            ("viral", "Viral Rates", _tbl_viral_rates(viral, views_r, mlist)),
            ("tiers", "Tiers", _tbl_tiers(videos, metric_key, tiers, mlist)),
            ("top20", "Top 20", _tbl_top20(viral, "Views")),
            ("top5", "Member Top 5", _tbl_member_top5(viral, mlist)),
            ("consistency", "Consistency", _tbl_consistency(views_r, mlist)),
            ("rankings", "Rankings", _tbl_power_rankings(views_r, viral, mlist)),
        ]

    # ── Helper: build tabs for Douyin (likes first, then comments/favorites/shares) ──
    def _douyin_tabs(videos, likes_r, comments_r, favorites_r, shares_r, viral, mlist, tiers, stitle=""):
        return [
            ("likes", "Likes", _metric_panel(likes_r, mlist, "Likes", videos, "likes", stitle)),
            ("comments", "Comments", _metric_panel(comments_r, mlist, "Comments", videos, "comments", stitle)),
            ("favorites", "Favorites", _metric_panel(favorites_r, mlist, "Favorites", videos, "favorites", stitle)),
            ("shares", "Shares", _metric_panel(shares_r, mlist, "Shares", videos, "shares", stitle)),
            ("viral", "Viral Rates", _tbl_viral_rates(viral, likes_r, mlist)),
            ("tiers", "Tiers", _tbl_tiers(videos, "likes", tiers, mlist)),
            ("top20", "Top 20", _tbl_top20(viral, "Likes")),
            ("top5", "Member Top 5", _tbl_member_top5(viral, mlist)),
            ("consistency", "Consistency", _tbl_consistency(likes_r, mlist)),
            ("rankings", "Rankings", _tbl_power_rankings(likes_r, viral, mlist)),
        ]

    # ── Build 6 sections ──
    sections_html = ""

    sections_html += _build_section("all-tiktok", "All TikTok",
        f"{total_tt} videos &bull; {fmt_num(tt_total_views)} total views &bull; Date range: {tt_date_range}",
        _ttyt_tabs(data["tiktok"], analysis["tiktok_rankings"],
                   analysis["tiktok_likes_rankings"], analysis["tiktok_comments_rankings"],
                   analysis["tiktok_shares_rankings"], analysis["tiktok_viral"],
                   members_all, "views_num", TT_TIERS, "All TikTok"))

    sections_html += _build_section("all-youtube", "All YouTube",
        f"{total_yt} videos &bull; {fmt_num(yt_total_views)} total views",
        _ttyt_tabs(data["youtube"], analysis["youtube_rankings"],
                   analysis["youtube_likes_rankings"], analysis["youtube_comments_rankings"],
                   analysis["youtube_shares_rankings"], analysis["youtube_viral"],
                   members_all, "views_num", YT_TIERS, "All YouTube"))

    sections_html += _build_section("all-douyin", "All Douyin",
        f"{total_dy} videos &bull; {fmt_num(dy_total_likes)} total likes &bull; Douyin API does not expose view counts",
        _douyin_tabs(data["douyin"], analysis["douyin_rankings"],
                     analysis["douyin_comments_rankings"], analysis["douyin_favorites_rankings"],
                     analysis["douyin_shares_rankings"], analysis["douyin_viral"],
                     members_all, DY_TIERS, "All Douyin"))

    sections_html += _build_section("solo-tiktok", "Solo TikTok",
        f"{len(solo.get('tiktok', []))} solo videos &bull; Single-member videos only",
        _ttyt_tabs(solo.get("tiktok", []), analysis["solo_tiktok_rankings"],
                   analysis["solo_tiktok_likes"], analysis["solo_tiktok_comments"],
                   analysis["solo_tiktok_shares"], analysis["solo_tiktok_viral"],
                   members_solo, "views_num", TT_TIERS, "Solo TikTok"))

    sections_html += _build_section("solo-youtube", "Solo YouTube",
        f"{len(solo.get('youtube', []))} solo videos &bull; Single-member videos only",
        _ttyt_tabs(solo.get("youtube", []), analysis["solo_youtube_rankings"],
                   analysis["solo_youtube_likes"], analysis["solo_youtube_comments"],
                   analysis["solo_youtube_shares"], analysis["solo_youtube_viral"],
                   members_solo, "views_num", YT_TIERS, "Solo YouTube"))

    sections_html += _build_section("solo-douyin", "Solo Douyin",
        f"{len(solo.get('douyin', []))} solo videos &bull; Single-member videos only",
        _douyin_tabs(solo.get("douyin", []), analysis["solo_douyin_rankings"],
                     analysis.get("solo_douyin_comments", {}), analysis.get("solo_douyin_favorites", {}),
                     analysis.get("solo_douyin_shares", {}), analysis["solo_douyin_viral"],
                     members_solo, DY_TIERS, "Solo Douyin"))

    # ── Build analysis HTML from markdown ──
    def _md_to_html(md_path):
        """Simple markdown to HTML converter (stdlib only)."""
        import re as _re
        if not md_path.exists():
            return "<p>Analysis file not found.</p>"
        text = md_path.read_text(encoding="utf-8")
        lines = text.split("\n")
        out = []
        in_table = False
        in_list = False
        for line in lines:
            stripped = line.strip()
            # Skip the title (first h1)
            if stripped.startswith("# ") and not any("<h" in o for o in out):
                continue
            # Horizontal rule
            if stripped == "---":
                if in_table:
                    out.append("</table>")
                    in_table = False
                if in_list:
                    out.append("</ul>")
                    in_list = False
                out.append("<hr>")
                continue
            # Headers
            if stripped.startswith("## "):
                if in_list:
                    out.append("</ul>")
                    in_list = False
                out.append(f'<h3>{stripped[3:]}</h3>')
                continue
            if stripped.startswith("### "):
                out.append(f'<h4>{stripped[4:]}</h4>')
                continue
            # Table
            if stripped.startswith("|"):
                cols = [c.strip() for c in stripped.split("|")[1:-1]]
                if all(set(c) <= set("- :") for c in cols):
                    continue  # separator row
                if not in_table:
                    out.append('<table class="data-table">')
                    tag = "th"
                    in_table = True
                else:
                    tag = "td"
                row = "<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cols) + "</tr>"
                out.append(row)
                continue
            else:
                if in_table:
                    out.append("</table>")
                    in_table = False
            # Bold
            formatted = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
            # List items
            if formatted.startswith("- ") or _re.match(r'^\d+\.\s', formatted):
                if not in_list:
                    out.append("<ul>")
                    in_list = True
                item = _re.sub(r'^-\s|^\d+\.\s', '', formatted)
                out.append(f"<li>{item}</li>")
                continue
            else:
                if in_list and stripped:
                    out.append("</ul>")
                    in_list = False
            # Paragraph
            if stripped:
                out.append(f"<p>{formatted}</p>")
        if in_table:
            out.append("</table>")
        if in_list:
            out.append("</ul>")
        return "\n".join(out)

    analysis_md = BASE_DIR / "IVE_ANALYSIS.md"
    toxic_md = BASE_DIR / "IVE_ANALYSIS_TOXIC.md"
    analysis_html = '<div class="tab-nav">\n'
    analysis_html += '<button class="tab-btn active" data-target="analysis-normal">Analysis</button>\n'
    analysis_html += '<button class="tab-btn" data-target="analysis-toxic">Toxic Version</button>\n'
    analysis_html += '</div>\n'
    analysis_html += f'<div class="tab-panel active" id="analysis-normal">\n{_md_to_html(analysis_md)}\n</div>\n'
    analysis_html += f'<div class="tab-panel" id="analysis-toxic">\n{_md_to_html(toxic_md)}\n</div>\n'

    # Section nav labels
    nav_items = [
        ("all-tiktok", "All TikTok"),
        ("all-youtube", "All YouTube"),
        ("all-douyin", "All Douyin"),
        ("solo-tiktok", "Solo TikTok"),
        ("solo-youtube", "Solo YouTube"),
        ("solo-douyin", "Solo Douyin"),
        ("analysis", "Analysis"),
    ]
    nav_html = '<nav class="section-nav">\n'
    for sid, label in nav_items:
        nav_html += f'<a class="section-nav-btn" href="#{sid}">{label}</a>\n'
    nav_html += '</nav>\n'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IVE Cross-Platform Analysis</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; line-height: 1.6; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
header {{ text-align: center; padding: 40px 0 20px; }}
header h1 {{ font-size: 2.5em; background: linear-gradient(135deg, #FF6B9D, #C084FC, #60A5FA); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
header .subtitle {{ color: #94a3b8; font-size: 1.1em; margin-top: 8px; }}
.stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 30px 0; }}
.stat-card {{ background: #1e293b; border-radius: 12px; padding: 20px; text-align: center; border: 1px solid #334155; }}
.stat-card .value {{ font-size: 2em; font-weight: 700; color: #f1f5f9; }}
.stat-card .label {{ color: #94a3b8; font-size: 0.9em; margin-top: 4px; }}
.section-nav {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; margin: 24px 0; position: sticky; top: 0; z-index: 100; background: #0f172a; padding: 12px 0; border-bottom: 1px solid #334155; }}
.section-nav-btn {{ padding: 8px 18px; border-radius: 8px; background: #1e293b; color: #94a3b8; text-decoration: none; font-weight: 600; font-size: 0.95em; border: 1px solid #334155; transition: all 0.2s; }}
.section-nav-btn:hover {{ background: #334155; color: #f1f5f9; }}
.section {{ background: #1e293b; border-radius: 12px; padding: 24px; margin: 24px 0; border: 1px solid #334155; scroll-margin-top: 70px; }}
.section h2 {{ font-size: 1.5em; margin-bottom: 16px; color: #f1f5f9; border-bottom: 2px solid #334155; padding-bottom: 8px; }}
.tab-nav {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; border-bottom: 2px solid #334155; padding-bottom: 0; }}
.tab-btn {{ padding: 8px 16px; border: none; background: transparent; color: #94a3b8; cursor: pointer; font-size: 0.9em; font-weight: 500; border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.2s; }}
.tab-btn:hover {{ color: #e2e8f0; background: #33415522; }}
.tab-btn.active {{ color: #60a5fa; border-bottom-color: #60a5fa; font-weight: 600; }}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}
.data-table {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.9em; }}
.data-table th {{ background: #334155; color: #e2e8f0; padding: 10px 12px; text-align: left; position: sticky; top: 0; cursor: pointer; user-select: none; white-space: nowrap; }}
.data-table th:hover {{ background: #475569; }}
.data-table th .sort-arrow {{ font-size: 0.7em; margin-left: 4px; opacity: 0.4; }}
.data-table th.sorted-asc .sort-arrow {{ opacity: 1; }}
.data-table th.sorted-desc .sort-arrow {{ opacity: 1; }}
.data-table td {{ padding: 8px 12px; border-bottom: 1px solid #334155; }}
.data-table tr:hover {{ background: #1e293b88; }}
.data-table .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.data-table a {{ color: #60a5fa; text-decoration: none; }}
.data-table a:hover {{ text-decoration: underline; }}
.member-tag {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; font-weight: 600; border: 1px solid; }}
.note {{ color: #94a3b8; font-style: italic; font-size: 0.9em; margin: 8px 0; }}
.table-scroll {{ overflow-x: auto; }}
.chart-wrap {{ background: #0f172a; border-radius: 8px; padding: 16px; }}
.chart-wrap canvas {{ max-height: 350px; }}
.analysis-content {{ max-width: 900px; margin: 0 auto; }}
.analysis-content h3 {{ color: #FF6B9D; font-size: 1.4em; margin: 32px 0 12px; padding-top: 16px; border-top: 1px solid #334155; }}
.analysis-content h4 {{ color: #C084FC; font-size: 1.1em; margin: 20px 0 8px; }}
.analysis-content p {{ color: #cbd5e1; margin: 10px 0; line-height: 1.8; }}
.analysis-content ul {{ color: #cbd5e1; margin: 10px 0 10px 24px; }}
.analysis-content li {{ margin: 6px 0; line-height: 1.7; }}
.analysis-content hr {{ border: none; border-top: 1px solid #334155; margin: 32px 0; }}
.analysis-content strong {{ color: #f1f5f9; }}
.analysis-content table {{ margin: 16px 0; }}
@media (max-width: 768px) {{ .tab-btn {{ padding: 6px 10px; font-size: 0.8em; }} }}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>IVE Cross-Platform Analysis</h1>
  <p class="subtitle">Member Popularity across TikTok, YouTube Shorts &amp; Douyin</p>
  <p class="subtitle">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
</header>

<div class="stats-grid">
  <div class="stat-card"><div class="value">{total_all:,}</div><div class="label">Total Videos</div></div>
  <div class="stat-card"><div class="value">{total_tt:,}</div><div class="label">TikTok Videos</div></div>
  <div class="stat-card"><div class="value">{total_yt:,}</div><div class="label">YouTube Shorts</div></div>
  <div class="stat-card"><div class="value">{total_dy:,}</div><div class="label">Douyin Videos</div></div>
  <div class="stat-card"><div class="value">{fmt_num(tt_total_views)}</div><div class="label">TikTok Total Views</div></div>
  <div class="stat-card"><div class="value">{fmt_num(yt_total_views)}</div><div class="label">YouTube Total Views</div></div>
  <div class="stat-card"><div class="value">{fmt_num(dy_total_likes)}</div><div class="label">Douyin Total Likes</div></div>
  <div class="stat-card"><div class="value">{tt_date_range}</div><div class="label">TikTok Date Range</div></div>
  <div class="stat-card"><div class="value">N/A</div><div class="label">YouTube Date Range</div></div>
  <div class="stat-card"><div class="value">{dy_date_range}</div><div class="label">Douyin Date Range</div></div>
</div>

{nav_html}

{sections_html}

<div class="section" id="analysis">
<h2>Analysis</h2>
<div class="analysis-content">
{analysis_html}
</div>
</div>

</div>

<script>
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#334155';

const memberOrder = {{"GAEUL": 1, "YUJIN": 2, "REI": 3, "WONYOUNG": 4, "LIZ": 5, "LEESEO": 6, "GROUP/UNKNOWN": 7}};

// Column index → data key mapping for distribution tables
const colKeys = [null, 'count', 'total', 'mean', 'median', 'stdev', 'min', 'max', 'p25', 'p75', 'p90', 'p99', 'top5_avg'];
const colLabels = [null, 'Videos', 'Total', 'Average', 'Median', 'StdDev', 'Min', 'Max', 'P25', 'P75', 'P90', 'P99', 'Top5 Avg'];

// Store chart instances keyed by canvas ID
const charts = {{}};

function fmtVal(v) {{
  if (v >= 1e9) return (v/1e9).toFixed(1) + 'B';
  if (v >= 1e6) return (v/1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v/1e3).toFixed(1) + 'K';
  return v.toFixed(0);
}}

// ── Sort table rows ──
function sortTableRows(table, colIdx, descending) {{
  const sortRow = table.querySelector('thead tr:last-child') || table.querySelector('thead tr');
  if (!sortRow) return;
  const ths = sortRow.querySelectorAll('th[data-sort]');
  const tbody = table.querySelector('tbody');
  if (!tbody) return;
  const th = ths[colIdx];
  if (!th) return;
  ths.forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
  th.classList.add(descending ? 'sorted-desc' : 'sorted-asc');
  const sortType = th.getAttribute('data-sort');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {{
    const cA = a.children[colIdx], cB = b.children[colIdx];
    if (!cA || !cB) return 0;
    let vA, vB;
    if (sortType === 'member') {{
      const nA = (cA.getAttribute('data-sort-value') || cA.textContent).trim();
      const nB = (cB.getAttribute('data-sort-value') || cB.textContent).trim();
      vA = memberOrder[nA] !== undefined ? memberOrder[nA] : 99;
      vB = memberOrder[nB] !== undefined ? memberOrder[nB] : 99;
    }} else {{
      vA = parseFloat(cA.getAttribute('data-sort-value') || cA.textContent.replace(/[^0-9.\\-]/g, '')) || 0;
      vB = parseFloat(cB.getAttribute('data-sort-value') || cB.textContent.replace(/[^0-9.\\-]/g, '')) || 0;
    }}
    return descending ? vB - vA : vA - vB;
  }});
  rows.forEach(r => tbody.appendChild(r));
}}

// ── Sync chart with table sort ──
function syncChart(table, colIdx, descending) {{
  const panel = table.closest('.tab-panel');
  if (!panel) return;
  const canvas = panel.querySelector('.metric-chart');
  if (!canvas || !canvas.dataset.chart) return;
  const chart = charts[canvas.id];
  if (!chart) return;

  const items = JSON.parse(canvas.dataset.chart);
  const key = colKeys[colIdx] || 'mean';
  const label = colLabels[colIdx] || 'Average';

  // Sort items by the selected column key
  const sorted = [...items].sort((a, b) => descending ? b[key] - a[key] : a[key] - b[key]);

  chart.data.labels = sorted.map(d => d.m);
  chart.data.datasets[0].data = sorted.map(d => d[key]);
  chart.data.datasets[0].label = label;
  chart.data.datasets[0].backgroundColor = sorted.map(d => d.c + '88');
  chart.data.datasets[0].borderColor = sorted.map(d => d.c);
  chart.update();
}}

// ── Create chart instance ──
function initChart(canvas) {{
  if (charts[canvas.id]) return;
  const items = JSON.parse(canvas.dataset.chart);
  // Default: sort by mean descending
  const sorted = [...items].sort((a, b) => b.mean - a.mean);
  const chart = new Chart(canvas, {{
    type: 'bar',
    data: {{
      labels: sorted.map(d => d.m),
      datasets: [{{
        label: 'Average',
        data: sorted.map(d => d.mean),
        backgroundColor: sorted.map(d => d.c + '88'),
        borderColor: sorted.map(d => d.c),
        borderWidth: 2,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ display: false }},
        tooltip: {{ callbacks: {{ label: ctx => fmtVal(ctx.raw) }} }}
      }},
      scales: {{ y: {{ beginAtZero: true, ticks: {{ callback: v => fmtVal(v) }} }} }}
    }}
  }});
  charts[canvas.id] = chart;
}}

// ── Distribution curve chart ──
const distCharts = {{}};

function initDistChart(canvas) {{
  if (distCharts[canvas.id]) return;
  const hist = JSON.parse(canvas.dataset.hist);
  const datasets = hist.members.map(m => ({{
    label: m.m,
    data: m.pcts,
    borderColor: m.c,
    backgroundColor: m.c + '22',
    borderWidth: 2,
    tension: 0.4,
    fill: true,
    pointRadius: 2,
  }}));
  distCharts[canvas.id] = new Chart(canvas, {{
    type: 'line',
    data: {{ labels: hist.bins, datasets }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'top', labels: {{ boxWidth: 12, padding: 10 }} }},
        tooltip: {{ callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.raw + '%' }} }}
      }},
      scales: {{
        x: {{ title: {{ display: true, text: canvas.closest('.chart-wrap').querySelector('h4')?.textContent.split(' Distribution')[0] || '' }} }},
        y: {{ beginAtZero: true, title: {{ display: true, text: '% of videos' }}, ticks: {{ callback: v => v + '%' }} }}
      }},
      interaction: {{ mode: 'index', intersect: false }}
    }}
  }});
}}

// ── Percentile curve chart ──
const pctlCharts = {{}};

function initPctlChart(canvas) {{
  if (pctlCharts[canvas.id]) return;
  const pctl = JSON.parse(canvas.dataset.pctl);
  const datasets = pctl.members.map(m => ({{
    label: m.m,
    data: m.vals,
    borderColor: m.c,
    backgroundColor: m.c + '22',
    borderWidth: 2,
    tension: 0.3,
    fill: false,
    pointRadius: 4,
    pointHoverRadius: 6,
  }}));
  pctlCharts[canvas.id] = new Chart(canvas, {{
    type: 'line',
    data: {{ labels: pctl.labels, datasets }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'top', labels: {{ boxWidth: 12, padding: 10 }} }},
        tooltip: {{ callbacks: {{ label: ctx => ctx.dataset.label + ': ' + fmtVal(ctx.raw) }} }}
      }},
      scales: {{
        x: {{ title: {{ display: true, text: 'Percentile' }} }},
        y: {{ type: 'logarithmic', title: {{ display: true, text: 'Value (log scale)' }}, ticks: {{ callback: v => fmtVal(v) }} }}
      }},
      interaction: {{ mode: 'index', intersect: false }}
    }}
  }});
}}

// ── Full sort: sort table + sync chart ──
function sortAndSync(table, colIdx, descending) {{
  sortTableRows(table, colIdx, descending);
  syncChart(table, colIdx, descending);
}}

// ── Attach click handlers to all sortable tables ──
document.querySelectorAll('table.sortable').forEach(table => {{
  const sortRow = table.querySelector('thead tr:last-child') || table.querySelector('thead tr');
  if (!sortRow) return;
  const ths = sortRow.querySelectorAll('th[data-sort]');
  ths.forEach((th, colIdx) => {{
    let ascending = true;
    th.addEventListener('click', () => {{
      sortAndSync(table, colIdx, !ascending);
      ascending = !ascending;
    }});
  }});
}});

// ── Init: default sort by Average (col 3) desc + init charts for active panels ──
function initPanel(panel) {{
  panel.querySelectorAll('.metric-chart').forEach(initChart);
  panel.querySelectorAll('.dist-chart').forEach(initDistChart);
  panel.querySelectorAll('.pctl-chart').forEach(initPctlChart);
  panel.querySelectorAll('table.sortable').forEach(t => {{
    if (!t.querySelector('th.sorted-asc, th.sorted-desc')) {{
      sortAndSync(t, 3, true);
    }}
  }});
}}
document.querySelectorAll('.tab-panel.active').forEach(initPanel);

// ── Tab switching ──
document.querySelectorAll('.tab-btn').forEach(btn => {{
  btn.addEventListener('click', () => {{
    const target = btn.getAttribute('data-target');
    const section = btn.closest('.section');
    section.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    section.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    const panel = document.getElementById(target);
    if (panel) {{
      panel.classList.add('active');
      initPanel(panel);
    }}
  }});
}});

// ── Highlight active section nav ──
const observer = new IntersectionObserver(entries => {{
  entries.forEach(e => {{
    if (e.isIntersecting) {{
      const id = e.target.id;
      document.querySelectorAll('.section-nav-btn').forEach(a => {{
        a.style.background = a.getAttribute('href') === '#' + id ? '#334155' : '';
        a.style.color = a.getAttribute('href') === '#' + id ? '#f1f5f9' : '';
      }});
    }}
  }});
}}, {{ threshold: 0.3 }});
document.querySelectorAll('.section[id]').forEach(s => observer.observe(s));
</script>
</body>
</html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved HTML report: {path}")


# ─── Main ───────────────────────────────────────────────────────────────────


def main():
    data = load_data()

    analysis = {}

    # Member rankings
    analysis["tiktok_rankings"] = compute_member_stats(data["tiktok"], "views_num")
    analysis["youtube_rankings"] = compute_member_stats(data["youtube"], "views_num")
    analysis["douyin_rankings"] = compute_member_stats(data["douyin"], "likes")

    # Engagement rankings by likes/comments/shares (all platforms)
    analysis["tiktok_likes_rankings"] = compute_member_stats(data["tiktok"], "likes")
    analysis["tiktok_comments_rankings"] = compute_member_stats(data["tiktok"], "comments")
    analysis["tiktok_shares_rankings"] = compute_member_stats(data["tiktok"], "shares")
    analysis["youtube_likes_rankings"] = compute_member_stats(data["youtube"], "likes")
    analysis["youtube_comments_rankings"] = compute_member_stats(data["youtube"], "comments")
    analysis["youtube_shares_rankings"] = compute_member_stats(data["youtube"], "shares")
    analysis["douyin_comments_rankings"] = compute_member_stats(data["douyin"], "comments")
    analysis["douyin_favorites_rankings"] = compute_member_stats(data["douyin"], "favorites")
    analysis["douyin_shares_rankings"] = compute_member_stats(data["douyin"], "shares")

    # Viral analysis
    analysis["tiktok_viral"] = compute_viral_analysis(data["tiktok"], "views_num", [5_000_000, 10_000_000, 20_000_000])
    analysis["youtube_viral"] = compute_viral_analysis(data["youtube"], "views_num", [2_000_000, 5_000_000, 10_000_000])
    analysis["douyin_viral"] = compute_viral_analysis(data["douyin"], "likes", [200_000, 500_000, 1_000_000])

    # Time trends
    analysis["tiktok_trends"] = compute_time_trends(data["tiktok"], "views_num")
    analysis["douyin_trends"] = compute_time_trends(data["douyin"], "likes")

    # ── Single-member video analysis ──
    # Filter to videos with exactly 1 identified member (not GROUP/UNKNOWN)
    solo = {}
    for platform_key in ["tiktok", "youtube", "douyin"]:
        solo[platform_key] = [
            v for v in data[platform_key]
            if len(v.get("members", [])) == 1 and v["members"][0] != "GROUP/UNKNOWN"
        ]
    analysis["solo_tiktok_rankings"] = compute_member_stats(solo["tiktok"], "views_num")
    analysis["solo_tiktok_likes"] = compute_member_stats(solo["tiktok"], "likes")
    analysis["solo_tiktok_comments"] = compute_member_stats(solo["tiktok"], "comments")
    analysis["solo_tiktok_shares"] = compute_member_stats(solo["tiktok"], "shares")
    analysis["solo_youtube_rankings"] = compute_member_stats(solo["youtube"], "views_num")
    analysis["solo_youtube_likes"] = compute_member_stats(solo["youtube"], "likes")
    analysis["solo_youtube_comments"] = compute_member_stats(solo["youtube"], "comments")
    analysis["solo_youtube_shares"] = compute_member_stats(solo["youtube"], "shares")
    analysis["solo_douyin_rankings"] = compute_member_stats(solo["douyin"], "likes")
    analysis["solo_douyin_comments"] = compute_member_stats(solo["douyin"], "comments")
    analysis["solo_douyin_favorites"] = compute_member_stats(solo["douyin"], "favorites")
    analysis["solo_douyin_shares"] = compute_member_stats(solo["douyin"], "shares")
    # Solo viral analysis
    analysis["solo_tiktok_viral"] = compute_viral_analysis(solo["tiktok"], "views_num", [5_000_000, 10_000_000, 20_000_000])
    analysis["solo_youtube_viral"] = compute_viral_analysis(solo["youtube"], "views_num", [2_000_000, 5_000_000, 10_000_000])
    analysis["solo_douyin_viral"] = compute_viral_analysis(solo["douyin"], "likes", [200_000, 500_000, 1_000_000])
    analysis["solo_counts"] = {p: len(solo[p]) for p in solo}
    data["solo"] = solo

    # Output
    print_terminal_summary(analysis)
    save_json(analysis, BASE_DIR / "ive_analysis.json")
    save_csvs(analysis, data, BASE_DIR)
    generate_html(analysis, data, BASE_DIR / "ive_report.html")

    print("\nDone! Open ive_report.html in a browser to see the interactive report.")


if __name__ == "__main__":
    main()
