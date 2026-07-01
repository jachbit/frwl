"""
F.O.C.U.S.© — Research Watchlist LITE  (Jachbit 2026)
Port 8115

Lite version — yFinance only, no Schwab/Tastytrade API required.
Safe to share on GitHub — zero credentials needed.

Data: yFinance (prices + fundamentals)
Prices refreshed every 60s | Fundamentals every 5 min
"""

import os, json, math, time, threading, pathlib, io, csv
from datetime import datetime
from flask import Flask, jsonify, Response, request, send_file

HERE = pathlib.Path(__file__).resolve().parent

app  = Flask(__name__)
PORT = int(os.getenv("PORT", 8115))

# ── Theme keyword auto-detection ───────────────────────────────────────────────
THEME_RULES = [
    (["artificial intelligence", " ai ", "machine learning", "deep learning",
      "generative", "large language", "chatgpt", "openai"],            "AI"),
    (["semiconductor", "chip", "fab ", "foundry", "wafer", "lithography",
      "nvidia", "amd", "intel", "tsmc", "arm "],                       "Chip"),
    (["cloud computing", "cloud service", "hyperscaler", "data center",
      "infrastructure as a service", "platform as a service"],         "AI Cloud"),
    (["bitcoin", "crypto", "blockchain", "digital asset", "mining",
      "hash", "btc", "ethereum"],                                       "BTC Mining"),
    (["hpc", "high performance computing", "gpu computing",
      "accelerat"],                                                     "HPC"),
    (["space", "aerospace", "satellite", "rocket", "launch vehicle",
      "orbital", "spacex", "planet labs", "rocket lab"],                "Space"),
    (["solar", "wind energy", "renewable", "clean energy",
      "nuclear", "hydrogen", "fuel cell"],                              "Clean Energy"),
    (["oil", "gas", "petroleum", "upstream", "downstream",
      "lng", "pipeline", "oilfield", " energy "],                      "Energy"),
    (["biotech", "biopharmaceutical", "genomic", "gene therapy",
      "crispr", "mrna"],                                                "Biotech"),
    (["pharmaceutical", "drug", "clinical", "oncology",
      "therapeutics"],                                                  "Pharma"),
    (["fintech", "payments", "digital payment", "neobank",
      "insurtech", "wealthtech"],                                       "Fintech"),
    (["cybersecurity", "cyber security", "endpoint", "zero trust",
      "siem", "firewall"],                                              "Cyber"),
    (["robotic", "automation", "autonomous vehicle", "self-driving",
      "drone", "uav"],                                                  "Robotics"),
    (["electric vehicle", "ev ", "battery", "lithium", "tesla"],       "EV"),
]

def _auto_theme(company, industry, sector, summary=""):
    haystack = f" {company} {industry} {sector} {summary} ".lower()
    return [theme for keywords, theme in THEME_RULES if any(kw in haystack for kw in keywords)]

def _theme_list(val):
    if isinstance(val, list):
        return [t.strip() for t in val if t and t.strip()]
    if isinstance(val, str) and val.strip():
        return [t.strip() for t in val.split(",") if t.strip()]
    return []

# ── State ──────────────────────────────────────────────────────────────────────
WATCHLIST_FILE = HERE / "watchlist.json"
_lock          = threading.Lock()
_watchlist     = {}
_last_refresh  = None
_source        = "yFinance"
PRICE_SEC      = 60    # yFinance price refresh (every 60s)
FUND_SEC       = 600   # fundamentals every 10 min (kinder to shared-IP rate limits)
TICKER_DELAY   = 1.2   # seconds between individual ticker fetches in fundamentals loop
BATCH_DELAY    = 3.0   # seconds between 50-symbol price batches
RL_SLEEP       = 45    # seconds to pause when a rate-limit error is hit

class RateLimitError(Exception):
    pass

def _sanitize(obj):
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    return obj

def _load_watchlist():
    global _watchlist
    if WATCHLIST_FILE.exists():
        try:
            _watchlist = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
        except Exception:
            _watchlist = {}
    else:
        _watchlist = {}

def _save_watchlist():
    WATCHLIST_FILE.write_text(json.dumps(_watchlist, indent=2), encoding="utf-8")

def _fmt_mktcap(v):
    if not v: return None
    if v >= 1e12: return f"${v/1e12:.2f}T"
    if v >= 1e9:  return f"${v/1e9:.2f}B"
    if v >= 1e6:  return f"${v/1e6:.2f}M"
    return f"${v:,.0f}"

def _safe(info, key, decimals=2):
    v = info.get(key)
    if v is None or v == 0: return None
    try: return round(float(v), decimals)
    except Exception: return None

# ── yFinance fetch ─────────────────────────────────────────────────────────────
def _fetch_ticker_data(symbol, preserve_cat=None):
    import yfinance as yf
    try:
        t    = yf.Ticker(symbol)
        info = t.info
        price = (info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose"))
        prev  = (info.get("previousClose") or info.get("regularMarketPreviousClose"))
        chg_pct = 0.0
        if price and prev and prev != 0:
            chg_pct = round((float(price) - float(prev)) / float(prev) * 100, 2)
        gm = info.get("grossMargins")
        if gm is not None:
            try: gm = round(float(gm) * 100, 1)
            except Exception: gm = None
        hist  = t.history(period="10d")
        trend = []
        if not hist.empty:
            trend = [round(float(p), 4) for p in hist["Close"].tail(7).tolist() if not math.isnan(float(p))]
        company  = info.get("longName") or info.get("shortName") or symbol.upper()
        industry = info.get("industry", "")
        sector   = info.get("sector",   "")
        cat = preserve_cat or sector or industry or "—"
        summary    = info.get("longBusinessSummary", "")
        auto_theme = _auto_theme(company, industry, sector, summary)
        return {
            "symbol":       symbol.upper(),
            "company":      company,
            "cat":          cat,
            "theme":        auto_theme,
            "live_price":   round(float(price), 2) if price else None,
            "fair_value":   _safe(info, "targetMeanPrice"),
            "mkt_cap":      _fmt_mktcap(info.get("marketCap")),
            "mkt_cap_raw":  info.get("marketCap") or 0,
            "week52_low":   _safe(info, "fiftyTwoWeekLow"),
            "week52_high":  _safe(info, "fiftyTwoWeekHigh"),
            "pe":           _safe(info, "trailingPE"),
            "pb":           _safe(info, "priceToBook"),
            "ps":           _safe(info, "priceToSalesTrailing12Months"),
            "peg":          _safe(info, "pegRatio"),
            "gross_margin": gm,
            "chg_pct":      chg_pct,
            "trend":        trend,
            "last_updated": datetime.now().isoformat(),
        }
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err or "429" in err:
            raise RateLimitError()
        print(f"  [yFinance] Error fetching {symbol}: {e}")
        return None

# ── Price refresh (yFinance, every 60s) ───────────────────────────────────────
def _yf_prices(symbols):
    import yfinance as yf
    result = {}
    try:
        batches = [symbols[i:i+50] for i in range(0, len(symbols), 50)]
        for idx, batch in enumerate(batches):
            if idx > 0:
                time.sleep(BATCH_DELAY)
            try:
                tickers = yf.Tickers(" ".join(batch))
            except Exception as e:
                print(f"  [yFinance price] batch error: {e}")
                continue
            for sym in batch:
                try:
                    fi    = tickers.tickers.get(sym, yf.Ticker(sym)).fast_info
                    price = getattr(fi, "last_price", None)
                    prev  = getattr(fi, "previous_close", None)
                    if not price: continue
                    chg_pct = round((float(price)-float(prev))/float(prev)*100, 2) if prev else 0.0
                    result[sym] = {"price": round(float(price), 2), "chg_pct": chg_pct}
                except Exception:
                    pass
    except Exception as e:
        print(f"  [yFinance price] error: {e}")
    return result

def _refresh_prices():
    global _last_refresh, _source
    with _lock:
        symbols = list(_watchlist.keys())
    if not symbols: return
    prices = _yf_prices(symbols)
    _source = f"yFinance({len(prices)})"
    with _lock:
        for sym, p in prices.items():
            if sym in _watchlist:
                _watchlist[sym]["live_price"] = p["price"]
                _watchlist[sym]["chg_pct"]    = p["chg_pct"]
        _last_refresh = datetime.now()
        _save_watchlist()
    print(f"  [Price] {len(prices)} symbols  [{_source}]  {_last_refresh:%H:%M:%S}")

def _refresh_fundamentals():
    global _last_refresh
    with _lock:
        symbols = list(_watchlist.keys())
    if not symbols: return
    print(f"  [Fundamentals] Updating {len(symbols)} symbols …")
    updated = {}
    for sym in symbols:
        with _lock:
            cat   = _watchlist.get(sym, {}).get("cat")
            theme = _watchlist.get(sym, {}).get("theme", "")
            notes = _watchlist.get(sym, {}).get("notes", "")
            group = _watchlist.get(sym, {}).get("group", "General")
            alarm = _watchlist.get(sym, {}).get("alarm")
        try:
            data = _fetch_ticker_data(sym, preserve_cat=cat)
        except RateLimitError:
            print(f"  [yFinance] Rate limited on {sym} — waiting {RL_SLEEP}s …")
            time.sleep(RL_SLEEP)
            try:
                data = _fetch_ticker_data(sym, preserve_cat=cat)
            except RateLimitError:
                print(f"  [yFinance] Still rate limited on {sym} — skipping this cycle")
                continue
        if data:
            existing = _theme_list(theme)
            auto     = _theme_list(data.get("theme"))
            if existing:
                merged = existing + [t for t in auto if t not in existing]
                data["theme"] = merged
            data["notes"] = notes
            data["group"] = group
            data["alarm"] = alarm
            updated[sym]  = data
        time.sleep(TICKER_DELAY)
    with _lock:
        for sym, data in updated.items():
            if sym in _watchlist:
                _watchlist[sym] = data
        _save_watchlist()
        _last_refresh = datetime.now()
    print(f"  [Fundamentals] Done — {len(updated)} symbols  {_last_refresh:%H:%M:%S}")

def _price_loop():
    import traceback
    while True:
        with _lock:
            empty = len(_watchlist) == 0
        if not empty:
            try: _refresh_prices()
            except Exception as e:
                print(f"  [Price loop] ERROR: {e}")
                traceback.print_exc()
        time.sleep(PRICE_SEC)

def _fundamentals_loop():
    import traceback
    time.sleep(5)
    while True:
        with _lock:
            empty = len(_watchlist) == 0
        if not empty:
            try: _refresh_fundamentals()
            except Exception as e:
                print(f"  [Fundamentals loop] ERROR: {e}")
                traceback.print_exc()
        time.sleep(FUND_SEC if not empty else 120)

# ── Flask routes ───────────────────────────────────────────────────────────────
@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    return r

@app.route("/")
def index():
    tmpl = HERE / "templates" / "index.html"
    html = tmpl.read_text(encoding="utf-8") if tmpl.exists() else "<h1>templates/index.html missing</h1>"
    return Response(html, mimetype="text/html")

@app.route("/ping")
def ping():
    """Health check endpoint — used by UptimeRobot to keep the free instance awake."""
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

@app.route("/api/watchlist")
def api_watchlist():
    with _lock:
        data = _sanitize(list(_watchlist.values()))
    return jsonify({
        "watchlist":    data,
        "count":        len(data),
        "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
        "source":       _source,
        "schwab":       False,
    })

@app.route("/api/lookup/<symbol>")
def api_lookup(symbol):
    sym = symbol.upper().strip()
    try:
        data = _fetch_ticker_data(sym)
    except RateLimitError:
        time.sleep(8)
        try:
            data = _fetch_ticker_data(sym)
        except RateLimitError:
            return jsonify({"error": "Yahoo Finance rate limit — please wait 30 seconds and try again."}), 429
    if not data:
        return jsonify({"error": f"Could not find ticker: {sym} — check the symbol and try again."}), 404
    return jsonify(data)

@app.route("/api/add", methods=["POST", "OPTIONS"])
def api_add():
    if request.method == "OPTIONS": return jsonify({}), 200
    body = request.get_json(force=True) or {}
    sym  = (body.get("symbol") or "").upper().strip()
    if not sym: return jsonify({"error": "Symbol is required"}), 400
    with _lock:
        if sym in _watchlist:
            return jsonify({"error": f"⚠ {sym} is already in your watchlist!", "duplicate": True, "data": _watchlist[sym]}), 409
    try:
        data = _fetch_ticker_data(sym)
    except RateLimitError:
        time.sleep(8)
        try:
            data = _fetch_ticker_data(sym)
        except RateLimitError:
            return jsonify({"error": "Yahoo Finance rate limit — please wait 30 seconds and try again."}), 429
    if not data:
        return jsonify({"error": f"Could not find ticker: {sym} — check the symbol and try again."}), 404
    if body.get("cat"): data["cat"] = body["cat"].strip()
    if body.get("theme") is not None:
        t = body["theme"]
        data["theme"] = t if isinstance(t, list) else [x.strip() for x in t.split(",") if x.strip()]
    data["notes"] = (body.get("notes") or "").strip()
    themes = data.get("theme") or []
    data["group"] = (body.get("group") or (themes[0] if themes else "General")).strip()
    with _lock:
        _watchlist[sym] = data
        _save_watchlist()
    return jsonify({"success": True, "data": data})

@app.route("/api/remove/<symbol>", methods=["DELETE", "OPTIONS"])
def api_remove(symbol):
    if request.method == "OPTIONS": return jsonify({}), 200
    sym = symbol.upper().strip()
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        del _watchlist[sym]
        _save_watchlist()
    return jsonify({"success": True, "symbol": sym})

@app.route("/api/refresh/<symbol>", methods=["POST", "OPTIONS"])
def api_refresh_one(symbol):
    if request.method == "OPTIONS": return jsonify({}), 200
    sym = symbol.upper().strip()
    with _lock:
        cat   = _watchlist.get(sym, {}).get("cat")
        theme = _watchlist.get(sym, {}).get("theme", "")
        alarm = _watchlist.get(sym, {}).get("alarm")
    data = _fetch_ticker_data(sym, preserve_cat=cat)
    if not data: return jsonify({"error": f"Could not refresh {sym}"}), 404
    existing = _theme_list(theme)
    auto     = _theme_list(data.get("theme"))
    if existing:
        merged = existing + [t for t in auto if t not in existing]
        data["theme"] = merged
    data["alarm"] = alarm
    with _lock:
        if sym in _watchlist:
            _watchlist[sym] = data
            _save_watchlist()
    return jsonify({"success": True, "data": data})

@app.route("/api/update_note", methods=["POST", "OPTIONS"])
def api_update_note():
    if request.method == "OPTIONS": return jsonify({}), 200
    body  = request.get_json(force=True) or {}
    sym   = (body.get("symbol") or "").upper().strip()
    notes = (body.get("notes")  or "").strip()
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        _watchlist[sym]["notes"] = notes
        _save_watchlist()
    return jsonify({"success": True})

@app.route("/api/set_alarm", methods=["POST", "OPTIONS"])
def api_set_alarm():
    if request.method == "OPTIONS": return jsonify({}), 200
    body      = request.get_json(force=True) or {}
    sym       = (body.get("symbol") or "").upper().strip()
    price     = body.get("price")
    direction = (body.get("direction") or "below").strip()
    enabled   = bool(body.get("enabled", True))
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        if price is None:
            _watchlist[sym]["alarm"] = None
        else:
            try: price = round(float(price), 2)
            except Exception: return jsonify({"error": "Invalid price"}), 400
            _watchlist[sym]["alarm"] = {"price": price, "direction": direction, "enabled": enabled}
        _save_watchlist()
    return jsonify({"success": True, "alarm": _watchlist[sym]["alarm"]})

@app.route("/api/update_group", methods=["POST", "OPTIONS"])
def api_update_group():
    if request.method == "OPTIONS": return jsonify({}), 200
    body  = request.get_json(force=True) or {}
    sym   = (body.get("symbol") or "").upper().strip()
    group = (body.get("group")  or "General").strip()
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        _watchlist[sym]["group"] = group
        _save_watchlist()
    return jsonify({"success": True})

@app.route("/api/batch_update_group", methods=["POST", "OPTIONS"])
def api_batch_update_group():
    if request.method == "OPTIONS": return jsonify({}), 200
    body    = request.get_json(force=True) or {}
    group   = (body.get("group") or "General").strip()
    symbols = [s.upper().strip() for s in (body.get("symbols") or [])]
    if not symbols: return jsonify({"error": "No symbols provided"}), 400
    updated = 0
    with _lock:
        for sym in symbols:
            if sym in _watchlist:
                _watchlist[sym]["group"] = group
                updated += 1
        if updated: _save_watchlist()
    return jsonify({"success": True, "updated": updated})

@app.route("/api/rename_group", methods=["POST", "OPTIONS"])
def api_rename_group():
    if request.method == "OPTIONS": return jsonify({}), 200
    body     = request.get_json(force=True) or {}
    from_grp = (body.get("from") or "").strip()
    to_grp   = (body.get("to")   or "").strip()
    if not from_grp or not to_grp: return jsonify({"error": "from and to are required"}), 400
    updated = 0
    with _lock:
        for sym, data in _watchlist.items():
            if (data.get("group") or "General") == from_grp:
                data["group"] = to_grp
                updated += 1
        if updated: _save_watchlist()
    return jsonify({"success": True, "updated": updated})

@app.route("/api/update_theme", methods=["POST", "OPTIONS"])
def api_update_theme():
    if request.method == "OPTIONS": return jsonify({}), 200
    body   = request.get_json(force=True) or {}
    sym    = (body.get("symbol") or "").upper().strip()
    themes = _theme_list(body.get("theme", []))
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        _watchlist[sym]["theme"] = themes
        _save_watchlist()
    return jsonify({"success": True})

@app.route("/api/update_cat", methods=["POST", "OPTIONS"])
def api_update_cat():
    if request.method == "OPTIONS": return jsonify({}), 200
    body = request.get_json(force=True) or {}
    sym  = (body.get("symbol") or "").upper().strip()
    cat  = (body.get("cat") or "").strip()
    with _lock:
        if sym not in _watchlist: return jsonify({"error": f"{sym} not found"}), 404
        _watchlist[sym]["cat"] = cat
        _save_watchlist()
    return jsonify({"success": True})

@app.route("/api/export/csv")
def api_export_csv():
    with _lock:
        rows = list(_watchlist.values())
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["#","Group","Symbol","Company","Sector","Theme","Mkt Cap","52W Low",
                 "Live Price","Fair Value","52W High","P/E","P/B","P/S","PEG",
                 "Gross Margin","% Chg","Notes","Last Updated"])
    for i, r in enumerate(rows, 1):
        gm  = f"{r['gross_margin']}%" if r.get("gross_margin") is not None else ""
        chg = r.get("chg_pct", 0) or 0
        w.writerow([
            i, r.get("group","General"), r.get("symbol",""), r.get("company",""), r.get("cat",""),
            ", ".join(r.get("theme") or []) if isinstance(r.get("theme"), list) else (r.get("theme","") or ""),
            r.get("mkt_cap",""), r.get("week52_low",""),
            r.get("live_price",""), r.get("fair_value",""), r.get("week52_high",""),
            r.get("pe",""), r.get("pb",""), r.get("ps",""), r.get("peg",""),
            gm, f"{chg:+.2f}%", r.get("notes",""), r.get("last_updated",""),
        ])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        out.getvalue().encode("utf-8"),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=FOCUS_WATCHLIST_LITE_{ts}.csv"},
    )

@app.route("/api/import/csv", methods=["POST", "OPTIONS"])
def api_import_csv():
    if request.method == "OPTIONS": return jsonify({}), 200
    file = request.files.get("file")
    if not file: return jsonify({"error": "No file uploaded"}), 400
    try:
        text   = file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        added = skipped = 0
        with _lock:
            for row in reader:
                sym = (row.get("Symbol") or "").strip().upper()
                if not sym: continue
                if sym in _watchlist:
                    skipped += 1
                    continue
                theme_raw = (row.get("Theme") or "").strip()
                theme = [t.strip() for t in theme_raw.split(",") if t.strip()]
                _watchlist[sym] = {
                    "symbol": sym, "company": (row.get("Company") or "").strip() or sym,
                    "cat": (row.get("Sector") or "").strip() or "—",
                    "group": (row.get("Group") or "General").strip() or "General",
                    "theme": theme, "notes": (row.get("Notes") or "").strip(),
                    "live_price": None, "fair_value": None,
                    "mkt_cap": (row.get("Mkt Cap") or "").strip() or None,
                    "mkt_cap_raw": 0, "week52_low": None, "week52_high": None,
                    "pe": None, "pb": None, "ps": None, "peg": None,
                    "gross_margin": None, "chg_pct": 0, "trend": [],
                    "last_updated": datetime.now().isoformat(),
                }
                added += 1
            if added: _save_watchlist()
        return jsonify({"success": True, "added": added, "skipped": skipped})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Boot ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print()
    print("=" * 56)
    print("  F.O.C.U.S.© — Research Watchlist LITE  (Jachbit 2026)")
    print(f"  Port: {PORT}  |  Data: yFinance only")
    print("=" * 56)
    _load_watchlist()
    print(f"  Loaded {len(_watchlist)} tickers from watchlist.json")
    for sym, d in _watchlist.items():
        existing = _theme_list(d.get("theme"))
        if not existing:
            existing = _auto_theme(d.get("company",""), "", d.get("cat",""))
        d["theme"] = existing
        if not d.get("group"):
            d["group"] = existing[0] if existing else "General"
    _save_watchlist()
    threading.Thread(target=_price_loop,        daemon=True).start()
    threading.Thread(target=_fundamentals_loop, daemon=True).start()
    print(f"  Prices every {PRICE_SEC}s  |  Fundamentals every {FUND_SEC}s  |  {TICKER_DELAY}s/ticker delay")
    print(f"  Open: http://localhost:{PORT}")
    print()
    import webbrowser, threading as _th
    _th.Timer(5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)
