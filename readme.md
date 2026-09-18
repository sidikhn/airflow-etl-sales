# Apache Airflow ETL Pipeline Penjualan Retail

Proyek ini merupakan implementasi pipeline ETL sederhana menggunakan Apache Airflow dan Docker. Pipeline memproses dataset penjualan retail dalam format CSV melalui empat tahap utama:

```text
Extract -> Validate -> Transform -> Load
```

Pipeline dilengkapi validasi kualitas data, penanganan nilai kosong, penghapusan duplikat, dua agregasi, visualisasi hasil, dan penyimpanan log terpisah untuk setiap DAG run.

## Identitas Proyek

- **Nama:** Hikmah Nursidik
- **NIM:** 25573877PPA07227
- **DAG ID:** `etl_sederhana_25573877PPA07227`
- **Mata Kuliah:** Data Warehouse and Business Intelligence
- **Dataset:** Penjualan retail sintetis

## Fitur Utama

- Empat task Airflow dengan dependency berurutan.
- Validasi header dan kolom wajib.
- Pemeriksaan nilai kosong atau null.
- Pemeriksaan record duplikat identik.
- Pemeriksaan `sale_id` ganda.
- Validasi tipe data numerik.
- Validasi tanggal dengan format `YYYY-MM-DD`.
- Penanganan aman untuk string kosong seperti `int("")`.
- Pengisian kota kosong menjadi `Unknown`.
- Pengisian nama produk kosong menjadi `Unknown Product`.
- Perhitungan ulang `total_amount` jika kosong atau tidak konsisten.
- Agregasi penjualan berdasarkan tanggal.
- Agregasi penjualan berdasarkan kota.
- Visualisasi tren penjualan harian.
- Visualisasi total penjualan berdasarkan kota.
- Folder log terpisah untuk setiap trigger DAG.
- Aman dijalankan ulang dengan `max_active_runs=1`.

## Struktur Repository

Struktur repository proyek ini adalah sebagai berikut:

```text
airflow_tugas_25573877PPA07227/
|-- .dist/
|-- dags/
|   `-- etl_sederhana_25573877PPA07227.py
|
|-- data/
|   |-- raw/
|   |   `-- sales.csv
|   |
|   |-- staging/
|   |   |-- sales_staging.csv
|   |   |-- sales_daily_transformed.csv
|   |   `-- sales_by_city_transformed.csv
|   |
|   |-- output/
|   |   |-- sales_daily.csv
|   |   |-- sales_by_city.csv
|   |   `-- visualizations/
|   |       |-- sales_daily_trend.png
|   |       `-- sales_by_city.png
|   |
|   `-- logs/
|       `-- <logical_date>__<run_id>/
|           |-- extract.log
|           |-- validate.log
|           |-- transform.log
|           `-- load.log
|
|-- docs/
|   `-- laporan dan source dokumentasi
|
|-- evidence/
|   `-- screenshot bukti eksekusi
|
|-- .gitignore
|-- docker-compose.yaml
|-- Dockerfile
|-- generate_data.py
`-- README.md
```

> Folder `staging`, `output`, `visualizations`, dan `logs` dibuat atau diperbarui ketika pipeline dijalankan.

## Dataset

Dataset dibuat secara sintetis menggunakan `generate_data.py`. Penggunaan `random.seed(42)` membuat data dapat direproduksi.

File sumber:

```text
data/raw/sales.csv
```

Dataset dasar berisi 300 transaksi selama tahun 2025. Nilai kosong dan 15 baris duplikat sengaja ditambahkan agar data menyerupai data mentah yang belum sempurna dan membutuhkan proses ETL.

### Struktur Kolom

| Kolom          | Tipe yang Diharapkan | Keterangan                                   |
| -------------- | -------------------- | -------------------------------------------- |
| `sale_id`      | Integer              | Identitas transaksi                          |
| `sale_date`    | Date                 | Tanggal transaksi dengan format `YYYY-MM-DD` |
| `product_id`   | String               | Identitas produk                             |
| `product_name` | String               | Nama produk                                  |
| `quantity`     | Integer              | Jumlah unit terjual                          |
| `unit_price`   | Numeric              | Harga setiap unit                            |
| `total_amount` | Numeric              | Total nilai transaksi                        |
| `city`         | String               | Kota transaksi                               |

## Arsitektur Pipeline

Dependency task pada DAG:

```text
extract_data
    -> validate_data
    -> transform_data
    -> load_data
```

Secara konseptual, alur data adalah:

```text
data/raw/sales.csv
        |
        v
     EXTRACT
        |
        v
data/staging/sales_staging.csv
        |
        v
    VALIDATE
        |
        v
    TRANSFORM
        |
        +-------------------------------+
        |                               |
        v                               v
Agregasi Harian                  Agregasi Kota
        |                               |
        +---------------+---------------+
                        |
                        v
                       LOAD
                        |
        +---------------+----------------+
        |               |                |
        v               v                v
  Output CSV      Visualisasi PNG   Log per Trigger
```

## Penjelasan Task

### 1. Extract

Task: `extract_data`

Proses yang dilakukan:

1. Membuat file log untuk proses Extract.
2. Memastikan `data/raw/sales.csv` tersedia.
3. Membuat folder staging jika belum tersedia.
4. Menyalin file sumber menjadi `data/staging/sales_staging.csv`.
5. Menghitung jumlah record tanpa menghitung header.
6. Menulis informasi proses ke `extract.log`.

### 2. Validate

Task: `validate_data`

Pemeriksaan yang dilakukan:

- File staging tersedia.
- CSV memiliki header.
- Semua kolom wajib tersedia.
- CSV memiliki record data.
- Jumlah nilai kosong pada setiap kolom.
- Jumlah record duplikat identik.
- Jumlah `sale_id` ganda.
- Validitas nilai numerik.
- Validitas tanggal dengan format `YYYY-MM-DD`.

Task Validate melaporkan masalah kualitas data melalui log. Nilai kosong dan duplikat tidak langsung menghentikan pipeline karena pembersihan dilakukan pada task Transform.

### 3. Transform

Task: `transform_data`

Aturan transformasi:

- Record duplikat identik hanya diproses satu kali.
- `sale_id` ganda hanya diproses satu kali.
- Record dengan `sale_id` kosong atau tidak valid dilewati.
- Record dengan `sale_date` kosong atau tidak valid dilewati.
- Record dengan `product_id` kosong dilewati.
- Record dengan `quantity` kosong, tidak valid, nol, atau negatif dilewati.
- Record dengan `unit_price` kosong, tidak valid, nol, atau negatif dilewati.
- `product_name` kosong diisi `Unknown Product`.
- `city` kosong diisi `Unknown`.
- `total_amount` kosong atau tidak konsisten dihitung ulang.

Rumus perhitungan ulang:

```text
total_amount = quantity * unit_price
```

Task Transform menghasilkan dua agregasi dan dua visualisasi.

### 4. Load

Task: `load_data`

Proses yang dilakukan:

1. Memastikan dua file hasil transformasi tersedia.
2. Membuat folder output jika belum tersedia.
3. Menyalin hasil agregasi harian ke folder output.
4. Menyalin hasil agregasi kota ke folder output.
5. Menghitung jumlah baris setiap output.
6. Memastikan dua file visualisasi tersedia.
7. Menulis informasi hasil ke `load.log`.

## Hasil Agregasi

### Agregasi Harian

File:

```text
data/output/sales_daily.csv
```

Kolom output:

| Kolom                | Keterangan                                   |
| -------------------- | -------------------------------------------- |
| `sale_date`          | Tanggal transaksi                            |
| `total_transactions` | Jumlah transaksi valid pada tanggal tersebut |
| `total_quantity`     | Total unit terjual pada tanggal tersebut     |
| `total_sales`        | Total nilai penjualan pada tanggal tersebut  |

Data diurutkan berdasarkan tanggal secara menaik.

### Agregasi Berdasarkan Kota

File:

```text
data/output/sales_by_city.csv
```

Kolom output:

| Kolom                | Keterangan                              |
| -------------------- | --------------------------------------- |
| `city`               | Nama kota atau `Unknown`                |
| `total_transactions` | Jumlah transaksi valid di kota tersebut |
| `total_quantity`     | Total unit terjual di kota tersebut     |
| `total_sales`        | Total nilai penjualan di kota tersebut  |

Data diurutkan berdasarkan `total_sales` dari nilai terbesar.

## Visualisasi

Pipeline menghasilkan dua visualisasi menggunakan Matplotlib.

### Tren Penjualan Harian

File:

```text
data/output/visualizations/sales_daily_trend.png
```

![Tren penjualan harian](data/output/visualizations/sales_daily_trend.png)

Grafik garis menampilkan perubahan total penjualan harian berdasarkan data yang sudah dibersihkan.

### Penjualan Berdasarkan Kota

File:

```text
data/output/visualizations/sales_by_city.png
```

![Penjualan berdasarkan kota](data/output/visualizations/sales_by_city.png)

Grafik batang membandingkan total penjualan antar-kota dan diurutkan dari total penjualan terbesar.

> Jika gambar belum tampil di GitHub, jalankan DAG terlebih dahulu lalu commit dan push file PNG pada folder `data/output/visualizations`.

## Log Per Trigger DAG

Selain log standar yang ditampilkan pada Airflow UI, pipeline menyimpan log tambahan pada:

```text
data/logs/
```

Setiap trigger DAG membuat satu folder baru berdasarkan `logical_date` dan `run_id`.

Contoh:

```text
data/logs/
|-- 20260917T153243__manual__2026-09-17T15_32_43_00_00/
|   |-- extract.log
|   |-- validate.log
|   |-- transform.log
|   `-- load.log
|
`-- 20260918T010000__scheduled__2026-09-18T01_00_00_00_00/
    |-- extract.log
    |-- validate.log
    |-- transform.log
    `-- load.log
```

Keterangan:

- `extract.log` mencatat proses ekstraksi.
- `validate.log` mencatat hasil pemeriksaan kualitas data.
- `transform.log` mencatat pembersihan, deduplikasi, agregasi, dan visualisasi.
- `load.log` mencatat file yang berhasil dimuat ke output.

Semua task pada satu DAG run menggunakan folder log yang sama. Trigger berikutnya menghasilkan folder baru.

## Persyaratan

Pastikan perangkat berikut tersedia:

- Docker
- Docker Compose
- Git
- Browser untuk membuka Airflow UI

Dependency Python tambahan di dalam image Airflow:

```text
matplotlib==3.9.2
```

## Dockerfile

```dockerfile
FROM apache/airflow:2.10.5-python3.12

USER airflow

RUN pip install --no-cache-dir matplotlib==3.9.2
```

## Konfigurasi Docker Compose

File proyek menggunakan nama:

```text
docker-compose.yaml
```

Contoh konfigurasi:

```yaml
services:
  airflow:
    build: .
    image: tugas-airflow-etl:local

    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'False'
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'True'

    ports:
      - '8080:8080'

    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
```

Volume `./data:/opt/airflow/data` membuat data, output, visualisasi, dan log tetap tersedia pada host setelah container dihentikan.

## Cara Menjalankan Proyek

### 1. Clone Repository

```bash
git clone https://github.com/sidikhn/airflow-etl-sales.git
cd airflow-etl-sales
```

### 2. Membuat Dataset

Jika file `data/raw/sales.csv` belum tersedia, jalankan:

```bash
python generate_data.py
```

### 3. Build dan Menjalankan Airflow

```bash
docker compose down
docker compose up --build
```

Opsi `--build` diperlukan ketika Dockerfile atau dependency berubah.

### 4. Memeriksa Import DAG

```bash
docker compose exec airflow airflow dags list-import-errors
```

Jika tidak ada import error, DAG siap dijalankan.

### 5. Membuka Airflow UI

Buka:

```text
http://localhost:8080
```

Gunakan kredensial lokal yang dikonfigurasi pada `docker-compose.yaml`.

### 6. Menjalankan DAG

1. Cari DAG `etl_sederhana_25573877PPA07227`.
2. Aktifkan DAG jika masih paused.
3. Pilih **Trigger DAG**.
4. Tunggu sampai seluruh task berstatus success.
5. Periksa folder `data/output`.
6. Periksa folder baru di dalam `data/logs`.

## Cara Memeriksa Hasil

### PowerShell

```powershell
Get-ChildItem data\output -Recurse
Get-Content data\output\sales_daily.csv -TotalCount 10
Get-Content data\output\sales_by_city.csv
Get-ChildItem data\logs -Recurse
```

### Git Bash, Linux, atau macOS

```bash
find data/output -maxdepth 2 -type f
head data/output/sales_daily.csv
cat data/output/sales_by_city.csv
find data/logs -maxdepth 2 -type f
```

## Dokumentasi

Folder `docs` digunakan untuk menyimpan laporan dan source dokumentasi.

Contoh isi:

```text
docs/
|-- main.tex
|-- contents/
|-- images/
`-- laporan.pdf
```

Dokumentasi laporan mencakup:

- Sampul
- Daftar isi
- BAB I Pendahuluan
- BAB II Deskripsi Dataset
- BAB III Perancangan Pipeline
- BAB IV Implementasi Airflow
- BAB V Hasil Eksekusi
- BAB VI Agregasi dan Visualisasi
- BAB VII Sistem Logging
- BAB VIII Kendala dan Penyelesaian
- BAB IX Kesimpulan
- BAB X Referensi
- Lampiran

## Evidence

Folder `evidence` digunakan untuk menyimpan screenshot asli hasil eksekusi, misalnya:

```text
evidence/
|-- airflow-dag-list.png
|-- airflow-graph-view.png
|-- airflow-run-success.png
|-- validate-log.png
|-- transform-log.png
|-- output-csv.png
`-- per-trigger-logs.png
```

Screenshot sebaiknya berasal dari lingkungan eksekusi sendiri dan tidak menampilkan informasi sensitif.

## Menjalankan Ulang Setelah Perubahan

Jika hanya mengubah file DAG:

```bash
docker compose restart airflow
```

Jika mengubah Dockerfile atau dependency:

```bash
docker compose down
docker compose up --build
```

Kemudian periksa kembali import DAG:

```bash
docker compose exec airflow airflow dags list-import-errors
```

## Troubleshooting

### DAG Tidak Muncul

Periksa import error:

```bash
docker compose exec airflow airflow dags list-import-errors
```

Pastikan file DAG berada pada:

```text
dags/etl_sederhana_25573877PPA07227.py
```

### Error `No module named matplotlib`

Pastikan Dockerfile memasang Matplotlib, kemudian lakukan build ulang:

```bash
docker compose down
docker compose up --build
```

### Error `invalid literal for int()`

Error terjadi jika string kosong langsung diberikan kepada `int()`. DAG menggunakan `safe_int()` dan `safe_float()` untuk menangani nilai kosong atau tidak valid sebelum konversi.

### File Sumber Tidak Ditemukan

Pastikan file tersedia pada host:

```text
data/raw/sales.csv
```

Di dalam container, file harus tersedia pada:

```text
/opt/airflow/data/raw/sales.csv
```

### Folder Log Tidak Muncul

Pastikan task sudah dijalankan dan volume data tersedia:

```yaml
- ./data:/opt/airflow/data
```

Folder log dibuat ketika setiap task memanggil fungsi konfigurasi logging.

### Visualisasi Tidak Terbentuk

Periksa `transform.log` pada folder DAG run terkait. Pastikan Matplotlib terpasang, task Transform berhasil, dan folder output dapat ditulis.

## Upload dan Update Repository

Untuk commit pertama:

```powershell
git init
git branch -M main
git add .
git commit -m "Initial commit: Airflow retail sales ETL"
git remote add origin https://github.com/sidikhn/airflow-etl-sales.git
git push -u origin main
```

Untuk perubahan selanjutnya:

```powershell
git status
git add .
git commit -m "Update ETL project documentation"
git push
```

## Catatan Keamanan

- Jangan commit file `.env`.
- Jangan commit password, token, atau API key.
- Jangan commit database runtime Airflow.
- Jangan memasukkan token GitHub ke dalam URL remote.
- Periksa screenshot sebelum diunggah.

## Lisensi

Proyek ini dibuat untuk keperluan pembelajaran. Tambahkan file `LICENSE` jika repository akan dibagikan atau digunakan ulang dengan ketentuan lisensi tertentu.

## Penulis

**Hikmah Nursidik**  
NIM: `25573877PPA07227`
