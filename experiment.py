"""
StreamingCube Simulation - PySpark
===================================
Simulasi konsep StreamingCube (Shaikh & Kitagawa, 2020) menggunakan PySpark.
Mensimulasikan: Stream Processing + OLAP Aggregation (roll-up, drill-down,
slice) + Perbandingan Materialized vs Non-Materialized vertex.

Referensi: Shaikh, S. A., & Kitagawa, H. (2020).
StreamingCube: Seamless Integration of Stream Processing and OLAP Analysis.
IEEE Access, 8, 104632–104649.
"""

import pyspark
import time
import random
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType
)

# ─────────────────────────────────────────────
# 1. INISIALISASI SPARK
# ─────────────────────────────────────────────

spark = (SparkSession.builder
         .appName("StreamingCube_Simulation")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.memory", "2g")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
print("=" * 60)
print("  StreamingCube Simulation - PySpark")
print("=" * 60)

# ─────────────────────────────────────────────
# 2. GENERATE SYNTHETIC DATA STREAM
# ─────────────────────────────────────────────
# Skema mengacu pada jurnal (Sec. IX-A):
# SALES(ProductID, SupplierID, CustomerID, StoreID, Timestamp, SalesAmount)

PRODUCTS   = ["P1", "P2", "P3", "P4", "P5"]
SUPPLIERS  = ["S1", "S2", "S3"]
CUSTOMERS  = ["C1", "C2", "C3", "C4"]
STORES     = ["ST1", "ST2", "ST3"]

def generate_stream(n: int, seed: int = 42):
    """Generate n tuples sintetis."""
    random.seed(seed)
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    records = []
    for i in range(n):
        records.append({
            "ProductID":  random.choice(PRODUCTS),
            "SupplierID": random.choice(SUPPLIERS),
            "CustomerID": random.choice(CUSTOMERS),
            "StoreID":    random.choice(STORES),
            "Timestamp":  base_time + timedelta(seconds=i * 2),
            "SalesAmount": round(random.uniform(10.0, 500.0), 2),
        })
    return records

SCHEMA = StructType([
    StructField("ProductID",   StringType(),    False),
    StructField("SupplierID",  StringType(),    False),
    StructField("CustomerID",  StringType(),    False),
    StructField("StoreID",     StringType(),    False),
    StructField("Timestamp",   TimestampType(), False),
    StructField("SalesAmount", FloatType(),     False),
])

# ─────────────────────────────────────────────
# 3. UTILITAS
# ─────────────────────────────────────────────

results = {}   # menyimpan semua hasil eksperimen

def measure(label: str, fn):
    """Jalankan fn(), ukur waktu, simpan hasil."""
    t0 = time.perf_counter()
    out = fn()
    elapsed = round(time.perf_counter() - t0, 4)
    results[label] = {"time_s": elapsed, "rows": out}
    print(f"  [{label}] rows={out:>6,}  time={elapsed:.4f}s")
    return out

# ─────────────────────────────────────────────
# 4. EKSPERIMEN UTAMA
# ─────────────────────────────────────────────

STREAM_SIZES   = [1_000, 5_000, 10_000, 20_000]
WINDOW_SIZE_N  = 500        # tuple-based window (n rows)

print("\n[1] THROUGHPUT vs UKURAN STREAM")
print("-" * 60)
print(f"  Window size (n) = {WINDOW_SIZE_N} rows")
print(f"  Agregasi : SUM(SalesAmount) GROUP BY ProductID, StoreID\n")

for n in STREAM_SIZES:
    data = generate_stream(n)
    df   = spark.createDataFrame(data, schema=SCHEMA)

    # Simulasi windowing: ambil n baris terakhir (tuple-based window)
    windowed = df.orderBy(F.col("Timestamp").desc()).limit(WINDOW_SIZE_N)

    def run_agg(wdf=windowed):
        return wdf.groupBy("ProductID", "StoreID") \
                  .agg(F.sum("SalesAmount").alias("TotalSales")) \
                  .count()

    t0 = time.perf_counter()
    row_count = run_agg()
    elapsed   = round(time.perf_counter() - t0, 4)
    tput      = round(min(n, WINDOW_SIZE_N) / elapsed, 2) if elapsed > 0 else 0

    results[f"throughput_n{n}"] = {
        "stream_size": n,
        "window_rows": min(n, WINDOW_SIZE_N),
        "agg_groups":  row_count,
        "time_s":      elapsed,
        "throughput_tuples_per_s": tput,
    }
    print(f"  n={n:>6,} | window={min(n,WINDOW_SIZE_N):>5} "
          f"| groups={row_count:>3} | {elapsed:.4f}s | {tput:>8.1f} tuples/s")


# ─────────────────────────────────────────────
# 5. OLAP OPERATIONS (konsep cubify)
# ─────────────────────────────────────────────

print("\n[2] OLAP QUERYING COST - Materialized vs Non-Materialized Vertex")
print("-" * 60)

N_MAIN = 10_000
data   = generate_stream(N_MAIN)
df     = spark.createDataFrame(data, schema=SCHEMA).cache()

# Materialized vertex: {ProductID, StoreID} — finest grain yang di-cache
print("  Pre-materializing vertex {ProductID, StoreID} ...")
t_mat_start = time.perf_counter()
mat_vertex = (df.groupBy("ProductID", "StoreID")
                .agg(F.sum("SalesAmount").alias("TotalSales"),
                     F.count("*").alias("TxCount"))
                .cache())
mat_vertex.count()   # trigger materialization
t_mat = round(time.perf_counter() - t_mat_start, 4)
print(f"  Materialization selesai dalam {t_mat:.4f}s\n")

# --- ROLL-UP ---
print("  ROLL-UP: {ProductID, StoreID} → {ProductID}")

def rollup_materialized():
    return (mat_vertex.groupBy("ProductID")
                      .agg(F.sum("TotalSales").alias("TotalSales"))
                      .count())

def rollup_non_materialized():
    return (df.groupBy("ProductID")
              .agg(F.sum("SalesAmount").alias("TotalSales"))
              .count())

t0 = time.perf_counter()
r1 = rollup_materialized()
t_rollup_mat = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
r2 = rollup_non_materialized()
t_rollup_non = round(time.perf_counter() - t0, 4)

print(f"    Materialized   : {r1} groups | {t_rollup_mat:.4f}s")
print(f"    Non-Materialized: {r2} groups | {t_rollup_non:.4f}s")
speedup_rollup = round(t_rollup_non / t_rollup_mat, 2) if t_rollup_mat > 0 else 0
print(f"    Speedup        : {speedup_rollup}x\n")

# --- DRILL-DOWN ---
print("  DRILL-DOWN: {ProductID} → {ProductID, CustomerID}")

def drilldown_materialized():
    # dari vertex {ProductID,StoreID} tidak bisa langsung drill-down ke CustomerID
    # → harus dari raw (simulasi nearest vertex)
    return (df.groupBy("ProductID", "CustomerID")
              .agg(F.sum("SalesAmount").alias("TotalSales"))
              .count())

def drilldown_non_materialized():
    return (df.groupBy("ProductID", "CustomerID")
              .agg(F.sum("SalesAmount").alias("TotalSales"))
              .count())

t0 = time.perf_counter()
d1 = drilldown_materialized()
t_dd_mat = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
d2 = drilldown_non_materialized()
t_dd_non = round(time.perf_counter() - t0, 4)

print(f"    Via raw (nearest vertex): {d1} groups | {t_dd_mat:.4f}s")
print(f"    Non-Materialized        : {d2} groups | {t_dd_non:.4f}s")

# --- SLICE ---
print("\n  SLICE: StoreID = 'ST1'")

def slice_materialized():
    return (mat_vertex.filter(F.col("StoreID") == "ST1")
                      .agg(F.sum("TotalSales"))
                      .count())

def slice_non_materialized():
    return (df.filter(F.col("StoreID") == "ST1")
              .agg(F.sum("SalesAmount"))
              .count())

t0 = time.perf_counter()
s1 = slice_materialized()
t_slice_mat = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
s2 = slice_non_materialized()
t_slice_non = round(time.perf_counter() - t0, 4)

print(f"    Materialized   : {t_slice_mat:.4f}s")
print(f"    Non-Materialized: {t_slice_non:.4f}s")
speedup_slice = round(t_slice_non / t_slice_mat, 2) if t_slice_mat > 0 else 0
print(f"    Speedup        : {speedup_slice}x")

results["olap_ops"] = {
    "materialization_time_s": t_mat,
    "rollup":   {"mat_s": t_rollup_mat, "non_mat_s": t_rollup_non, "speedup": speedup_rollup},
    "drilldown":{"mat_s": t_dd_mat,     "non_mat_s": t_dd_non},
    "slice":    {"mat_s": t_slice_mat,  "non_mat_s": t_slice_non,  "speedup": speedup_slice},
}

# ─────────────────────────────────────────────
# 6. C-WINDOW SIZE EFFECT
# ─────────────────────────────────────────────

print("\n[3] PENGARUH C-WINDOW SIZE terhadap Throughput")
print("-" * 60)

data_large = generate_stream(20_000)
df_large   = spark.createDataFrame(data_large, schema=SCHEMA)

c_windows = [30, 60, 90, 120]   # detik — sesuai jurnal Sec. IX

cwin_results = []
for cw in c_windows:
    cutoff = datetime(2024, 1, 1, 0, 0, 0) + timedelta(seconds=cw)
    t0 = time.perf_counter()
    count = (df_large
             .filter(F.col("Timestamp") <= F.lit(cutoff))
             .groupBy("ProductID", "StoreID")
             .agg(F.sum("SalesAmount").alias("TotalSales"))
             .count())
    elapsed = round(time.perf_counter() - t0, 4)
    tput    = round(count / elapsed, 2) if elapsed > 0 else 0
    cwin_results.append({"c_window_s": cw, "groups": count,
                          "time_s": elapsed, "throughput": tput})
    print(f"  c-window={cw:>3}s | groups={count:>3} | "
          f"{elapsed:.4f}s | {tput:>7.1f} tuples/s")

results["c_window"] = cwin_results

# ─────────────────────────────────────────────
# 7. SIMPAN HASIL
# ─────────────────────────────────────────────

out_path = "/home/claude/streamingcube_sim/results.json"
with open(out_path, "w") as f:
    json.dump(results, f, indent=2, default=str)

print("\n" + "=" * 60)
print(f"  Semua hasil disimpan ke: {out_path}")
print("=" * 60)

spark.stop()
