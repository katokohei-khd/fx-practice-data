"""FX練習チャート用のデータを作る。

1分足から15分足を作り、銘柄ごとに次のファイルを data/ に書き出す。
データ元：
  過去分  … HistData（為替・金・原油・米国指数。1年分を1ファイルで配布）、Binance（BTC。15分足を月ごとに配布）
            US30 だけはまとめて配布している所がないため Dukascopy の日ごとのファイル
  直近の日 … Dukascopy（BTC は Binance）の日ごとのファイル
Dukascopy はクラウドのサーバーからの取得をかなり制限するため、日ごとのファイルを大量に取るのは US30 と直近分だけにしている。
  data/{銘柄}/{YYYY}.bin     … 終わった年（1年分）
  data/{銘柄}/{YYYY-MM}.bin  … 今年の月ごと
  data/index.json            … ファイル一覧と各銘柄のデータ期間
  data/rate.json             … 円換算用のドル円日足終値

.bin は gzip 圧縮した Int32（リトルエンディアン）の並び：
  [1(形式), 本数n, 先頭時刻(UTC分), 先頭始値,
   時刻差(15分単位)×n, 始値-前の終値×n, 高値-max(始,終)×n, min(始,終)-安値×n, 終値-始値×n]

使い方:
  python build.py hist 銘柄                     HistData から過去分（2015年〜）を作り、そのあとの日を Dukascopy で埋める
  python build.py btc                          Binance から BTC の過去分を作り、そのあとの日を埋める
  python build.py months 銘柄 YYYY-MM YYYY-MM   Dukascopy で指定した月を作り直す（US30 の過去分用）
  python build.py fill 銘柄                     最後のデータの日〜前日を日ごとに取って足す（直近10日の欠けた日も取り直す）
  python build.py recent                       全銘柄で fill を行う（毎日の更新）
  python build.py finalize                     終わった年をまとめ、index.json と rate.json を作る
"""
import calendar
import concurrent.futures as cf
import http.cookiejar
import io
import re
import urllib.parse
import zipfile
import datetime as dt
import gzip
import json
import lzma
import os
import random
import struct
import sys
import time
import urllib.error
import urllib.request

import numpy as np
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# 銘柄: (Dukascopyの名前, 価格の割り算, 小数桁, データ開始年)
SYMBOLS = {
    "USDJPY": ("USDJPY", 1, 3, 2015), "EURUSD": ("EURUSD", 1, 5, 2015), "EURJPY": ("EURJPY", 1, 3, 2015),
    "GBPJPY": ("GBPJPY", 1, 3, 2015), "GBPUSD": ("GBPUSD", 1, 5, 2015), "AUDJPY": ("AUDJPY", 1, 3, 2015),
    "AUDUSD": ("AUDUSD", 1, 5, 2015), "USDCHF": ("USDCHF", 1, 5, 2015), "USDCAD": ("USDCAD", 1, 5, 2015),
    "XAUUSD": ("XAUUSD", 10, 2, 2015), "USOIL": ("LIGHTCMDUSD", 1, 3, 2015), "US30": ("USA30IDXUSD", 100, 1, 2015),
    "USTECH": ("USATECHIDXUSD", 100, 1, 2015), "US500": ("USA500IDXUSD", 100, 1, 2015), "BTCUSD": ("BTCUSD", 1, 1, 2017),
}
# HistData での名前（US30 と BTC は無い）
HISTDATA = {s: s.lower() for s in ["USDJPY", "EURUSD", "EURJPY", "GBPJPY", "GBPUSD", "AUDJPY", "AUDUSD", "USDCHF", "USDCAD", "XAUUSD"]}
HISTDATA.update({"USOIL": "wtiusd", "USTECH": "nsxusd", "US500": "spxusd"})
START_YEAR = 2015
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
DATA = os.environ.get("DATA_DIR", "data")
CACHE = os.environ.get("RAW_CACHE")  # 指定すると取得したファイルを保存して再利用する
CONC = int(os.environ.get("CONC", "4"))
URL = "https://datafeed.dukascopy.com/datafeed/"


def today_utc():
    return dt.datetime.now(dt.timezone.utc).date()


def fetch(path, use_cache=True):
    """1ファイル取得。無い日は b""。混雑や通信エラーは間隔を空けて取り直す"""
    if CACHE and use_cache:
        fn = os.path.join(CACHE, path.replace("/", "_"))
        if os.path.exists(fn):
            return open(fn, "rb").read()
    for k in range(14):
        try:
            req = urllib.request.Request(URL + path, headers={"User-Agent": "Mozilla/5.0"})
            data = urllib.request.urlopen(req, timeout=60).read()
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                data = b""
                break
            err = e
        except Exception as e:  # 通信エラー
            err = e
        time.sleep(min(120, 2 ** k) * (0.5 + random.random()))
    else:
        raise RuntimeError(f"取得失敗 {path}: {err}")
    if CACHE:
        os.makedirs(CACHE, exist_ok=True)
        open(os.path.join(CACHE, path.replace("/", "_")), "wb").write(data)
    return data


FAILED = []


def day_bars(sym, day):
    """1日分の1分足 → [(UTC分, 始, 高, 安, 終)]。出来高0（休場中）の足は捨てる。
    取得や展開に失敗した日は取り直し、それでもだめなら記録して空で続ける（1日の失敗で全体を止めない）"""
    duka, div, _, _ = SYMBOLS[sym]
    path = f"{duka}/{day.year}/{day.month - 1:02d}/{day.day:02d}/BID_candles_min_1.bi5"
    for k in range(3):
        try:
            raw = fetch(path, use_cache=(k == 0))
            if not raw:
                return []
            d = lzma.decompress(raw, format=lzma.FORMAT_ALONE)
            break
        except Exception as e:
            err = e
            time.sleep(5 * (k + 1))
    else:
        print(f"::warning::{sym} {day} を取得できませんでした: {err}", flush=True)
        FAILED.append((sym, day))
        return []
    base = int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp()) // 60
    out = []
    for i in range(0, len(d) - 23, 24):
        s, o, c, lo, hi, vol = struct.unpack(">5if", d[i:i + 24])
        if vol <= 0:
            continue
        r = lambda v: int(round(v / div))
        out.append((base + s // 60, r(o), r(hi), r(lo), r(c)))
    return out


def histdata_rows(sym, year, month=None):
    """HistData の1分足（1年分、または今年の1か月分）→ [(UTC分, 始, 高, 安, 終)]。
    配布されていない期間は None。HistData の時刻はニューヨーク時間（夏時間あり。冬はUTC-5、夏はUTC-4）。
    ※ HistData の説明には「夏時間なし」とあるが、Dukascopy・OANDA と突き合わせると夏は1時間ずれるため、夏時間ありとして扱う"""
    pair, dig = HISTDATA[sym], SYMBOLS[sym][2]
    page = f"https://www.histdata.com/download-free-forex-historical-data/?/ascii/1-minute-bar-quotes/{pair}/{year}"
    if month:
        page += f"/{month}"
    for k in range(6):
        try:
            op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            html = op.open(urllib.request.Request(page, headers=BROWSER), timeout=60).read().decode("utf-8", "ignore")
            tk = re.search(r'id="tk" value="([^"]+)"', html)
            if not tk:
                return None
            form = {"tk": tk.group(1), "date": str(year), "datemonth": f"{year}{month:02d}" if month else str(year),
                    "platform": "ASCII", "timeframe": "M1", "fxpair": pair.upper()}
            req = urllib.request.Request("https://www.histdata.com/get.php", data=urllib.parse.urlencode(form).encode(),
                                         headers=dict(BROWSER, Referer=page, Origin="https://www.histdata.com"))
            blob = op.open(req, timeout=180).read()
            if not blob.startswith(b"PK"):
                return None
            zf = zipfile.ZipFile(io.BytesIO(blob))
            text = zf.read(next(n for n in zf.namelist() if n.endswith(".csv"))).decode()
            break
        except Exception as e:
            err = e
            time.sleep(5 * (k + 1))
    else:
        raise RuntimeError(f"HistData 取得失敗 {sym} {year} {month}: {err}")
    out, p, offset = [], 10 ** dig, {}
    for line in text.splitlines():
        ts, o, h, l, c = line.split(";")[:5]
        y, mo, d, hh, mm = int(ts[:4]), int(ts[4:6]), int(ts[6:8]), int(ts[9:11]), int(ts[11:13])
        key = (y, mo, d, hh)
        if key not in offset:  # ニューヨーク時間とUTCの差（分）を1時間ごとに求める
            offset[key] = -int(dt.datetime(y, mo, d, hh, tzinfo=NY).utcoffset().total_seconds()) // 60
        t = calendar.timegm((y, mo, d, hh, mm, 0)) // 60 + offset[key]
        out.append((t, round(float(o) * p), round(float(h) * p), round(float(l) * p), round(float(c) * p)))
    return out


def binance_rows(path):
    """Binance の BTCUSDT 15分足（月ごと・日ごとのzip）→ [(UTC分, 始, 高, 安, 終)]。無ければ None"""
    for k in range(6):
        try:
            blob = urllib.request.urlopen(urllib.request.Request("https://data.binance.vision/data/spot/" + path, headers=BROWSER), timeout=60).read()
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            err = e
        except Exception as e:
            err = e
        time.sleep(3 * (k + 1))
    else:
        raise RuntimeError(f"Binance 取得失敗 {path}: {err}")
    zf = zipfile.ZipFile(io.BytesIO(blob))
    out = []
    for line in zf.read(zf.namelist()[0]).decode().splitlines():
        f = line.split(",")
        if not f[0].isdigit():
            continue
        ts = int(f[0])
        t = ts // 60_000_000 if ts > 10 ** 14 else ts // 60_000  # 2025年以降はマイクロ秒
        out.append((t, *(round(float(x) * 10) for x in f[1:5])))
    return out


def to_m15(rows):
    bars = {}
    for t, o, h, l, c in sorted(rows):
        k = t // 15 * 15
        b = bars.get(k)
        if b is None:
            bars[k] = [o, h, l, c]
        else:
            b[1] = max(b[1], h); b[2] = min(b[2], l); b[3] = c
    return [(k, *v) for k, v in sorted(bars.items())]


def encode(bars):
    a = np.array(bars, dtype=np.int64).reshape(-1, 5)
    t, o, h, l, c = a.T
    pc = np.concatenate([[o[0]], c[:-1]])
    dtv = np.diff(np.concatenate([[t[0]], t])) // 15
    cols = [dtv, o - pc, h - np.maximum(o, c), np.minimum(o, c) - l, c - o]
    head = np.array([1, len(a), t[0], o[0]], dtype=np.int64)
    arr = np.concatenate([head] + cols).astype("<i4")
    return gzip.compress(arr.tobytes(), 9, mtime=0)


def decode(blob):
    a = np.frombuffer(gzip.decompress(blob), dtype="<i4").astype(np.int64)
    _, n, t0, o0 = a[:4]
    dtv, do, dh, dl, dc = a[4:].reshape(5, n)
    t = t0 + np.cumsum(dtv) * 15
    o = np.empty(n, np.int64); c = np.empty(n, np.int64)
    prev = o0
    for i in range(n):  # 始値は前の終値からの差
        o[i] = prev + do[i]; c[i] = o[i] + dc[i]; prev = c[i]
    h = np.maximum(o, c) + dh
    l = np.minimum(o, c) - dl
    return [tuple(int(x) for x in r) for r in zip(t, o, h, l, c)]


def write(path, bars):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if bars:
        open(path, "wb").write(encode(bars))
    elif os.path.exists(path):
        os.remove(path)


def build_month(sym, y, m, pool):
    first = dt.date(y, m, 1)
    last = min(dt.date(y + m // 12, m % 12 + 1, 1), today_utc())  # 今日の分はまだ確定していないので含めない
    days = [first + dt.timedelta(i) for i in range((last - first).days)]
    if sym != "BTCUSD":
        days = [d for d in days if d.weekday() != 5]  # 土曜は休場
    rows = [r for day in pool.map(lambda d: day_bars(sym, d), days) for r in day]
    # 失敗した日はもう一度だけ1件ずつ取り直す
    retry = [d for s, d in FAILED if s == sym and d in days]
    if retry:
        time.sleep(30)
        for d in retry:
            FAILED.remove((sym, d))
            rows += day_bars(sym, d)
    bars = to_m15(rows)
    write(os.path.join(DATA, sym, f"{y}-{m:02d}.bin"), bars)
    print(f"{sym} {y}-{m:02d}: {len(bars)}本", flush=True)


def months_between(a, b):
    y, m = map(int, a.split("-")); y2, m2 = map(int, b.split("-"))
    while (y, m) <= (y2, m2):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def cmd_months(sym, a, b):
    with cf.ThreadPoolExecutor(CONC) as pool:
        for y, m in months_between(a, b):
            build_month(sym, y, m, pool)
    if FAILED:
        print(f"::warning::取得できなかった日 {len(FAILED)}件: {FAILED[:20]}", flush=True)


def path_of(sym, t):
    """その時刻のバーが入るファイル（今年は月ごと、前年以前は年ごと。年ファイルが無ければ月ごと）"""
    d = dt.datetime.fromtimestamp(t * 60, dt.timezone.utc)
    if d.year < today_utc().year and os.path.exists(os.path.join(DATA, sym, f"{d.year}.bin")):
        return os.path.join(DATA, sym, f"{d.year}.bin")
    return os.path.join(DATA, sym, f"{d.year}-{d.month:02d}.bin")


def merge_days(sym, day_rows):
    """日ごとに取ったデータを、その日の分だけ差し替えてファイルに書き込む"""
    by_file = {}
    for day, rows in day_rows.items():
        lo = calendar.timegm(day.timetuple()) // 60
        by_file.setdefault(path_of(sym, lo), []).append((lo, lo + 1440, to_m15(rows)))
    for path, parts in by_file.items():
        bars = {b[0]: b for b in decode(open(path, "rb").read())} if os.path.exists(path) else {}
        for lo, hi, new in parts:
            bars = {k: v for k, v in bars.items() if not lo <= k < hi}
            bars.update({b[0]: b for b in new})
        write(path, [bars[k] for k in sorted(bars)])


def all_bars(sym):
    d = os.path.join(DATA, sym)
    if not os.path.isdir(d):
        return []
    return sorted(b for n in sorted(os.listdir(d)) if n.endswith(".bin") for b in decode(open(os.path.join(d, n), "rb").read()))


def cmd_fill(sym, pool=None):
    """最後のデータの日〜前日を日ごとに取って足す。直近10日で中身が空の日（取りこぼし）も取り直す"""
    bars = all_bars(sym)
    yesterday = today_utc() - dt.timedelta(1)
    last = dt.datetime.fromtimestamp(bars[-1][0] * 60, dt.timezone.utc).date() if bars else yesterday - dt.timedelta(30)
    recent = bars[-1][0] - 15 * 1440 if bars else 0
    have = {dt.datetime.fromtimestamp(b[0] * 60, dt.timezone.utc).date() for b in bars if b[0] >= recent}
    start = max(min(last, yesterday - dt.timedelta(9)), yesterday - dt.timedelta(14))
    days = [start + dt.timedelta(i) for i in range((yesterday - start).days + 1)]
    days = [d for d in days if d >= last or d not in have]
    if sym != "BTCUSD":
        days = [d for d in days if d.weekday() != 5]  # 土曜は休場
    if not days:
        return
    if sym == "BTCUSD":
        rows = {d: binance_rows(f"daily/klines/BTCUSDT/15m/BTCUSDT-15m-{d.isoformat()}.zip") or [] for d in days}
    else:
        own = pool is None
        pool = pool or cf.ThreadPoolExecutor(2)
        rows = dict(zip(days, pool.map(lambda d: day_bars(sym, d), days)))
        if own:
            pool.shutdown()
    rows = {d: r for d, r in rows.items() if r}  # 取れなかった日は既存のデータを残す
    if rows:
        merge_days(sym, rows)
    print(f"{sym}: {len(rows)}/{len(days)}日を追加 {min(rows) if rows else ''}〜{max(rows) if rows else ''}", flush=True)


def write_years(sym, rows_by_year):
    """年ごとの1分足 → 前年以前は1年分のファイル、今年は月ごとのファイル"""
    for y, rows in rows_by_year.items():
        bars = to_m15(rows)
        if y < today_utc().year:
            write(os.path.join(DATA, sym, f"{y}.bin"), bars)
        else:
            for m in range(1, 13):
                lo = calendar.timegm((y, m, 1, 0, 0, 0)) // 60
                hi = calendar.timegm((y + m // 12, m % 12 + 1, 1, 0, 0, 0)) // 60
                part = [b for b in bars if lo <= b[0] < hi]
                if part:
                    write(os.path.join(DATA, sym, f"{y}-{m:02d}.bin"), part)
        print(f"{sym} {y}: {len(bars)}本", flush=True)


def cmd_hist(sym):
    this = today_utc()
    for y in range(START_YEAR, this.year + 1):
        if y < this.year:
            rows = histdata_rows(sym, y)
            if rows is None:
                print(f"::warning::HistData に {sym} {y} がありません", flush=True)
                continue
        else:  # 今年は月ごとに配布される（終わった月だけ）
            rows = []
            for m in range(1, this.month):
                r = histdata_rows(sym, y, m)
                if r is None:
                    print(f"::notice::HistData {sym} {y}-{m:02d} は未配布（Dukascopy で埋めます）", flush=True)
                    break
                rows += r
        write_years(sym, {y: rows})
    cmd_fill(sym)


def cmd_btc():
    this = today_utc()
    by_year = {}
    y, m = 2017, 8  # Binance の BTCUSDT は2017年8月から
    while (y, m) < (this.year, this.month):
        r = binance_rows(f"monthly/klines/BTCUSDT/15m/BTCUSDT-15m-{y}-{m:02d}.zip")
        if r:
            by_year.setdefault(y, []).extend(r)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    write_years("BTCUSD", by_year)
    cmd_fill("BTCUSD")


def month_counts(sym):
    """月ごとの本数 {(年, 月): 本数}"""
    c = {}
    for b in all_bars(sym):
        d = dt.datetime.fromtimestamp(b[0] * 60, dt.timezone.utc)
        c[(d.year, d.month)] = c.get((d.year, d.month), 0) + 1
    return c


def gap_months(sym):
    """データが無い・明らかに少ない月（ふつうの月の6割未満）。今月と、データ開始前は除く"""
    c = month_counts(sym)
    if not c:
        return []
    typical = sorted(c.values())[len(c) // 2]
    this = today_utc()
    start = (START_YEAR, 1) if sym != "BTCUSD" else (2017, 9)  # Binance の BTC は2017年8月中旬から
    out = []
    for y, m in months_between(f"{start[0]}-{start[1]:02d}", f"{this.year}-{this.month:02d}"):
        if (y, m) == (this.year, this.month):
            continue
        if c.get((y, m), 0) < typical * 0.6 and not (m == 1 and c.get((y, m), 0) > typical * 0.4):
            out.append((y, m))
    return out


def cmd_gaps(sym, shard, shards):
    """欠けている月を Dukascopy で作り直す（shards 個に分けて並行するときは shard 番目の分だけ）。
    時間がかかるので BUDGET 秒（既定50分）で打ち切り、残りは次回に回す"""
    budget = time.time() + int(os.environ.get("BUDGET", "3000"))
    todo = [ym for i, ym in enumerate(gap_months(sym)) if i % int(shards) == int(shard)]
    print(f"::notice::{sym} 欠けている月 {len(todo)}件（この分担）: {todo[:12]}", flush=True)
    with cf.ThreadPoolExecutor(CONC) as pool:
        for y, m in todo:
            if time.time() > budget:
                print(f"::notice::{sym} 時間切れのため残りは次回", flush=True)
                break
            build_month(sym, y, m, pool)


def cmd_recent():
    with cf.ThreadPoolExecutor(2) as pool:
        for sym in SYMBOLS:
            try:
                cmd_fill(sym, pool)
            except Exception as e:  # 1銘柄の失敗で全体を止めない（次の日にまた取り直す）
                print(f"::warning::{sym} の更新に失敗: {e}", flush=True)


def month_start(name):
    """"YYYY-MM" → その月の1日0時（UTC分）"""
    return int(dt.datetime(int(name[:4]), int(name[5:7]), 1, tzinfo=dt.timezone.utc).timestamp()) // 60


def cmd_finalize():
    this_year = today_utc().year
    index = {"updated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "symbols": {}}
    for sym in SYMBOLS:
        d = os.path.join(DATA, sym)
        if not os.path.isdir(d):
            continue
        names = sorted(os.listdir(d))
        # 終わった年は月ファイルを1年分にまとめる。すでに1年分のファイルがあれば、それに月の分を上書きで足す
        # （1月の1〜3日は前年12月を作り直すので、12月だけで1年分を上書きしないように）
        for y in sorted({n[:4] for n in names if len(n) == 11}):
            if int(y) < this_year:
                months = [n for n in names if n.startswith(y + "-")]
                yfile = os.path.join(d, f"{y}.bin")
                bars = {b[0]: b for b in decode(open(yfile, "rb").read())} if os.path.exists(yfile) else {}
                for n in months:
                    lo = month_start(n)
                    hi = month_start(f"{int(n[:4]) + int(n[5:7]) // 12}-{int(n[5:7]) % 12 + 1:02d}")
                    bars = {k: v for k, v in bars.items() if not lo <= k < hi}  # その月の古い分は捨てて作り直した分に替える
                    bars.update({b[0]: b for b in decode(open(os.path.join(d, n), "rb").read())})
                write(yfile, [bars[k] for k in sorted(bars)])
                for n in months:
                    os.remove(os.path.join(d, n))
        names = sorted(os.listdir(d))
        files = [n[:-4] for n in names if n.endswith(".bin")]
        first = decode(open(os.path.join(d, names[0]), "rb").read())[0][0]
        last = decode(open(os.path.join(d, names[-1]), "rb").read())[-1][0]
        index["symbols"][sym] = {"files": files, "first": first, "last": last}
    # 円換算用：ドル円の日足終値（日本時間の朝7時区切り）
    rate = {}
    d = os.path.join(DATA, "USDJPY")
    for n in sorted(os.listdir(d)):
        for t, o, h, l, c in decode(open(os.path.join(d, n), "rb").read()):
            rate[(t + 120) // 1440] = (t, c)
    json.dump(sorted(rate.values()), open(os.path.join(DATA, "rate.json"), "w"), separators=(",", ":"))
    json.dump(index, open(os.path.join(DATA, "index.json"), "w"), ensure_ascii=False, indent=1)
    print("index:", {k: (len(v["files"]), v["last"]) for k, v in index["symbols"].items()})


def main():
    c = sys.argv[1]
    if c == "months":
        cmd_months(*sys.argv[2:5])
    elif c == "hist":
        cmd_hist(sys.argv[2])
    elif c == "btc":
        cmd_btc()
    elif c == "gaps":
        cmd_gaps(*sys.argv[2:5])
    elif c == "fill":
        cmd_fill(sys.argv[2])
    elif c == "recent":
        cmd_recent()
    elif c == "finalize":
        cmd_finalize()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        tb = traceback.format_exc().strip().splitlines()
        print(f"::error::{type(e).__name__}: {e} | " + " | ".join(tb[-6:]), flush=True)
        raise
