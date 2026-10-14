# StreamingCube Simulation — PySpark

Simulasi konsep **StreamingCube** menggunakan Apache PySpark sebagai bagian dari tugas Eksplorasi Mandiri mata kuliah **Mahadata (IF25-40402)**, Institut Teknologi Sumatera (ITERA).

## Referensi Jurnal

> Shaikh, S. A., & Kitagawa, H. (2020).  
> **StreamingCube: Seamless Integration of Stream Processing and OLAP Analysis.**  
> *IEEE Access*, 8, 104632–104649.  
> DOI: [10.1109/ACCESS.2020.2999572](https://doi.org/10.1109/ACCESS.2020.2999572)

## Deskripsi

Jurnal asli mengusulkan framework **StreamingCube** (ditulis dalam C++) yang menyatukan Stream Processing Engine (SPE) dan OLAP dalam satu platform menggunakan operator *cubify*. Eksperimen ini **mensimulasikan konsep utama** jurnal tersebut menggunakan PySpark (Python), karena kode asli tidak tersedia dalam bahasa Python.

### Konsep yang disimulasikan:
| Konsep Jurnal | Implementasi di Sini |
|---|---|
| Tuple-based window | `df.orderBy(Timestamp).limit(n)` |
| Cubify operator | `groupBy + agg (SUM, COUNT)` |
| Materialized vertex | DataFrame yang di-`.cache()` |
| OLAP Roll-up | Aggregasi ke dimensi lebih kasar |
| OLAP Drill-down | Aggregasi ke dimensi lebih detail |
| OLAP Slice | Filter satu dimensi |
| C-window (time-based) | Filter berdasarkan rentang Timestamp |

## Struktur Eksperimen

### Eksperimen 1 — Throughput vs Ukuran Stream
Mengukur throughput (tuples/detik) untuk berbagai ukuran stream (1.000–20.000 tuple) dengan window size tetap 500 baris.

### Eksperimen 2 — OLAP Querying Cost
Membandingkan waktu query antara **materialized vertex** (data ter-cache) vs **non-materialized vertex** (query langsung ke raw data) untuk operasi:
- Roll-up: `{ProductID, StoreID}` → `{ProductID}`
- Drill-down: `{ProductID}` → `{ProductID, CustomerID}`
- Slice: filter `StoreID = 'ST1'`

### Eksperimen 3 — Pengaruh C-Window Size
Mengukur throughput dengan variasi ukuran c-window (30, 60, 90, 120 detik), sesuai dengan Sec. IX-A jurnal.

## Dataset

Data sintetis di-generate secara programatik dengan skema:

```
SALES(ProductID, SupplierID, CustomerID, StoreID, Timestamp, SalesAmount)
```

- 5 produk, 3 supplier, 4 customer, 3 store
- SalesAmount: random float 10.0–500.0
- Timestamp: interval 2 detik per tuple

Pendekatan ini mengacu pada dataset sintetis yang digunakan jurnal (Sec. IX-A-3).

## Cara Menjalankan

### Prasyarat
```bash
pip install pyspark
```

### Jalankan eksperimen
```bash
python experiment.py
```

Output hasil disimpan otomatis ke `results.json`.

## Hasil Eksperimen

### Throughput vs Ukuran Stream

| Stream Size | Window (n) | Waktu (s) | Throughput (tuples/s) |
|---:|---:|---:|---:|
| 1.000 | 500 | 6.8520 | 73.0 |
| 5.000 | 500 | 0.6827 | 732.4 |
| 10.000 | 500 | 0.5397 | 926.4 |
| 20.000 | 500 | 0.6204 | 805.9 |

### OLAP Querying Cost (n = 10.000)

| Operasi | Materialized (s) | Non-Materialized (s) |
|---|---:|---:|
| Roll-up | 1.2303 | 0.6192 |
| Drill-down | 0.9430 | 0.5048 |
| Slice | 0.9078 | 0.3817 |

### C-Window Size Effect

| C-Window (s) | Groups | Waktu (s) | Throughput |
|---:|---:|---:|---:|
| 30 | 10 | 0.9682 | 10.3 |
| 60 | 13 | 0.7194 | 18.1 |
| 90 | 14 | 0.7012 | 20.0 |
| 120 | 15 | 0.7394 | 20.3 |

## Analisis Singkat

**Throughput**: Stabil di kisaran 700–900 tuples/s setelah warm-up awal Spark (run pertama lebih lambat karena JVM cold-start).

**Materialized vs Non-Materialized**: Pada eksperimen ini, non-materialized terlihat lebih cepat — berbeda dengan hasil jurnal. Hal ini disebabkan overhead `.cache()` PySpark di mode lokal dengan dataset kecil. Pada dataset besar dan cluster nyata, materialized vertex akan lebih unggul seperti yang ditunjukkan jurnal.

**C-Window**: Semakin besar window, semakin banyak tuple yang diproses, sehingga throughput meningkat secara wajar.

## Penggunaan AI

Eksperimen ini dibuat dengan bantuan **Claude (Anthropic)** untuk penulisan kode awal dan struktur laporan. Analisis, interpretasi hasil, dan kontekstualisasi terhadap jurnal dilakukan secara mandiri.

## Lisensi

MIT License — bebas digunakan untuk keperluan akademik.
