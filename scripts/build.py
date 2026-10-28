"""FX練習チャート用のデータを作る。

Dukascopy の1分足から15分足を作り、銘柄ごとに次のファイルを data/ に書き出す。
  data/{銘柄}/{YYYY}.bin     … 終わった年（1年分）
  data/{銘柄}/{YYYY-MM}.bin  … 今年の月ごと
  data/index.json            … ファイル一覧と各銘柄のデータ期間
  data/rate.json             … 円換算用のドル円日足終値

.bin は gzip 圧縮した Int32（リトルエンディアン）の並び：
  [1(形式), 本数n, 先頭時刻(UTC分), 先頭始値,
   時刻差(15分単位)×n, 始値-前の終値×n, 高値-max(始,終)×n, min(始,終)-安値×n, 終値-始値×n]

使い方:
  python build.py months 銘柄 YYYY-MM YYYY-MM   指定した月を作り直す
  python build.py recent                       全銘柄の直近（前日を含む月と、月初なら前月）を作り直す
  python build.py finalize                     終わった年をまとめ、index.json と rate.json を作る
"""
import concurrent.futures as cf
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

# 銘柄: (Dukascopyの名前, 価格の割り算, 小数桁, データ開始年)
SYMBOLS = {
    "USDJPY": ("USDJPY", 1, 3, 2015), "EURUSD": ("EURUSD", 1, 5, 2015), "EURJPY": ("EURJPY", 1, 3, 2015),
    "GBPJPY": ("GBPJPY", 1, 3, 2015), "GBPUSD": ("GBPUSD", 1, 5, 2015), "AUDJPY": ("AUDJPY", 1, 3, 2015),
    "AUDUSD": ("AUDUSD", 1, 5, 2015), "USDCHF": ("USDCHF", 1, 5, 2015), "USDCAD": ("USDCAD", 1, 5, 2015),
    "XAUUSD": ("XAUUSD", 10, 2, 2015), "USOIL": ("LIGHTCMDUSD", 1, 3, 2015), "US30": ("USA30IDXUSD", 100, 1, 2015),
    "USTECH": ("USATECHIDXUSD", 100, 1, 2015), "US500": ("USA500IDXUSD", 100, 1, 2015), "BTCUSD": ("BTCUSD", 1, 1, 2017),
}
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


def cmd_recent():
    y = today_utc() - dt.timedelta(1)
    targets = {(y.year, y.month)}
    if today_utc().day <= 3:  # 月が変わった直後は前月も最終日まで作り直す
        p = y.replace(day=1) - dt.timedelta(1)
        targets.add((p.year, p.month))
    with cf.ThreadPoolExecutor(CONC) as pool:
        for sym in SYMBOLS:
            for yy, mm in sorted(targets):
                build_month(sym, yy, mm, pool)


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
