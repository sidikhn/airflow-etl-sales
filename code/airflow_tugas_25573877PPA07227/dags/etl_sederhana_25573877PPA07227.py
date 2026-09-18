"""
DAG ETL sederhana untuk dataset penjualan retail.

Pipeline:
    Extract -> Validate -> Transform -> Load

Input:
    data/raw/sales.csv

Staging:
    data/staging/sales_staging.csv
    data/staging/sales_daily_transformed.csv
    data/staging/sales_by_city_transformed.csv

Output:
    data/output/sales_daily.csv
    data/output/sales_by_city.csv
    data/output/visualizations/sales_daily_trend.png
    data/output/visualizations/sales_by_city.png

Log per trigger:
    data/logs/<logical_date>__<run_id>/extract.log
    data/logs/<logical_date>__<run_id>/validate.log
    data/logs/<logical_date>__<run_id>/transform.log
    data/logs/<logical_date>__<run_id>/load.log
"""

from datetime import datetime
from pathlib import Path
import csv
import logging
import shutil
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.python import PythonOperator


# 1. KONFIGURASI

DAG_ID = "etl_sederhana_25573877PPA07227"

BASE_DIR = Path(__file__).resolve().parents[1]

# File sumber yang akan diproses oleh pipeline.
RAW_FILE = BASE_DIR / "data" / "raw" / "sales.csv"

# Folder dan file staging.
STAGING_DIR = BASE_DIR / "data" / "staging"
STAGING_FILE = STAGING_DIR / "sales_staging.csv"

# File sementara hasil transformasi.
DAILY_TRANSFORMED_FILE = (
    STAGING_DIR / "sales_daily_transformed.csv"
)

CITY_TRANSFORMED_FILE = (
    STAGING_DIR / "sales_by_city_transformed.csv"
)

# Folder dan file output akhir.
OUTPUT_DIR = BASE_DIR / "data" / "output"

DAILY_OUTPUT_FILE = (
    OUTPUT_DIR / "sales_daily.csv"
)

CITY_OUTPUT_FILE = (
    OUTPUT_DIR / "sales_by_city.csv"
)

# Folder untuk menyimpan visualisasi hasil agregasi.
VISUALIZATION_DIR = (
    OUTPUT_DIR / "visualizations"
)

DAILY_VISUALIZATION_FILE = (
    VISUALIZATION_DIR / "sales_daily_trend.png"
)

CITY_VISUALIZATION_FILE = (
    VISUALIZATION_DIR / "sales_by_city.png"
)

# Folder utama log khusus proyek.
# Setiap trigger DAG akan membuat subfolder baru di dalam folder ini.
LOG_DIR = BASE_DIR / "data" / "logs"

# Semua kolom yang wajib tersedia pada CSV sumber.
REQUIRED_COLUMNS = [
    "sale_id",
    "sale_date",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "total_amount",
    "city",
]

# Kolom yang dianggap penting pada proses validasi nilai kosong.
IMPORTANT_COLUMNS = [
    "sale_id",
    "sale_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
]

# 2. FUNGSI UNTUK LOG PER TRIGGER
def configure_process_log(process_name, context):
    """
    Membuat satu folder log untuk setiap trigger atau DAG run.

    Semua task dalam DAG run yang sama menggunakan kombinasi
    logical_date dan run_id yang sama. Oleh karena itu, keempat
    file log akan tersimpan pada folder trigger yang sama.

    Contoh folder:
        data/logs/
        20260917T153243__manual__2026-09-17T15_32_43_00_00/

    Isi folder:
        extract.log
        validate.log
        transform.log
        load.log
    """
    run_id = str(
        context.get("run_id", "unknown_run")
    )

    logical_date = context.get("logical_date")

    if logical_date is not None:
        trigger_time = logical_date.strftime(
            "%Y%m%dT%H%M%S"
        )
    else:

        trigger_time = datetime.now().strftime(
            "%Y%m%dT%H%M%S"
        )

    safe_run_id = "".join(
        character
        if character.isalnum() or character in "-_"
        else "_"
        for character in run_id
    )

    run_log_dir = (
        LOG_DIR
        / f"{trigger_time}__{safe_run_id}"
    )

    run_log_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_file = (
        run_log_dir
        / f"{process_name}.log"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in list(root_logger.handlers):
        if getattr(
            handler,
            "etl_process_handler",
            False,
        ):
            root_logger.removeHandler(handler)
            handler.close()

    file_handler = logging.FileHandler(
        log_file,
        mode="a",
        encoding="utf-8",
    )

    file_handler.etl_process_handler = True
    file_handler.setLevel(logging.INFO)

    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root_logger.addHandler(file_handler)

    logging.info("========================================")
    logging.info("DAG run_id: %s", run_id)
    logging.info("Logical date: %s", logical_date)
    logging.info("Process log file: %s", log_file)
    logging.info("========================================")

    return log_file


# 3. PEMBERSIHAN DAN VALIDASI DATA
def clean_text(value, default=None):
    """
    Membersihkan nilai teks.

    Nilai None, string kosong, atau string yang hanya berisi
    spasi akan dikembalikan sebagai nilai default.
    """

    if value is None:
        return default

    cleaned_value = str(value).strip()

    if cleaned_value == "":
        return default

    return cleaned_value


def safe_int(value, default=None):
    """
    Mengubah nilai menjadi integer secara aman.

    Contoh:
        "5"   -> 5
        "5.0" -> 5
        ""    -> default
        None  -> default
        "abc" -> default

    Fungsi ini mencegah error seperti:
        ValueError: invalid literal for int() with base 10: ''
    """

    cleaned_value = clean_text(value)

    if cleaned_value is None:
        return default

    try:
        return int(cleaned_value)

    except (ValueError, TypeError):
        try:
            numeric_value = float(cleaned_value)

            if numeric_value.is_integer():
                return int(numeric_value)

            return default

        except (ValueError, TypeError):
            return default


def safe_float(value, default=None):
    """
    Mengubah nilai menjadi float secara aman.

    Nilai kosong atau bukan angka akan dikembalikan sebagai default,
    sehingga satu record yang rusak tidak langsung menggagalkan task.
    """

    cleaned_value = clean_text(value)

    if cleaned_value is None:
        return default

    try:
        return float(cleaned_value)

    except (ValueError, TypeError):
        return default


def parse_date(value):
    """
    Memvalidasi dan menormalkan tanggal dengan format YYYY-MM-DD.

    Jika valid, hasil dikembalikan dalam format ISO YYYY-MM-DD.
    Jika kosong atau tidak valid, fungsi mengembalikan None.
    """

    cleaned_value = clean_text(value)

    if cleaned_value is None:
        return None

    try:
        parsed_date = datetime.strptime(
            cleaned_value,
            "%Y-%m-%d",
        )

        return parsed_date.date().isoformat()

    except ValueError:
        return None


def normalize_row(row):
    """
    Mengubah seluruh isi record menjadi tuple yang konsisten.

    Tuple ini digunakan untuk mendeteksi record yang benar-benar
    identik berdasarkan seluruh kolom dataset.
    """

    return tuple(
        clean_text(
            row.get(column),
            default="",
        )
        for column in REQUIRED_COLUMNS
    )


def count_csv_rows(file_path):
    """
    Menghitung jumlah record CSV tanpa menghitung header.
    """

    with file_path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:
        reader = csv.reader(file)
        row_count = sum(1 for _ in reader) - 1

    return max(row_count, 0)


# 4. VISUALISASI
def create_visualizations(daily_sales, city_sales):
    """
    Membuat dua visualisasi dari hasil agregasi yang sudah bersih.

    Visualisasi pertama:
        Grafik garis tren total penjualan harian.

    Visualisasi kedua:
        Grafik batang total penjualan berdasarkan kota.

    File PNG disimpan pada data/output/visualizations.
    """

    logging.info("========================================")
    logging.info("Memulai pembuatan VISUALISASI")
    logging.info("========================================")

    VISUALIZATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Visualisasi 1: Tren total penjualan harian
    daily_items = sorted(
        daily_sales.items()
    )

    daily_dates = [
        datetime.strptime(
            sale_date,
            "%Y-%m-%d",
        )
        for sale_date, _ in daily_items
    ]

    daily_totals = [
        data["total_sales"]
        for _, data in daily_items
    ]

    figure, axis = plt.subplots(
        figsize=(12, 6)
    )

    axis.plot(
        daily_dates,
        daily_totals,
        color="#2563EB",
        linewidth=1.6,
    )

    axis.set_title(
        "Tren Total Penjualan Harian"
    )
    axis.set_xlabel("Tanggal")
    axis.set_ylabel("Total Penjualan")
    axis.grid(True, alpha=0.25)

    figure.autofmt_xdate()
    figure.tight_layout()

    figure.savefig(
        DAILY_VISUALIZATION_FILE,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    logging.info(
        "Visualisasi harian disimpan: %s",
        DAILY_VISUALIZATION_FILE,
    )

    # Visualisasi 2: Total penjualan berdasarkan kota
    city_items = sorted(
        city_sales.items(),
        key=lambda item: item[1]["total_sales"],
        reverse=True,
    )

    city_names = [
        city
        for city, _ in city_items
    ]

    city_totals = [
        data["total_sales"]
        for _, data in city_items
    ]

    figure, axis = plt.subplots(
        figsize=(10, 6)
    )

    bars = axis.bar(
        city_names,
        city_totals,
        color="#16A34A",
    )

    axis.set_title(
        "Total Penjualan Berdasarkan Kota"
    )
    axis.set_xlabel("Kota")
    axis.set_ylabel("Total Penjualan")
    axis.tick_params(
        axis="x",
        rotation=25,
    )
    axis.grid(
        axis="y",
        alpha=0.25,
    )

    for bar, value in zip(
        bars,
        city_totals,
    ):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    figure.tight_layout()

    figure.savefig(
        CITY_VISUALIZATION_FILE,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    logging.info(
        "Visualisasi kota disimpan: %s",
        CITY_VISUALIZATION_FILE,
    )
    logging.info("Pembuatan VISUALISASI selesai.")


# 5. TASK 1 - EXTRACT
def extract_data(**context):
    """
    Membaca file CSV dari folder raw dan menyalinnya ke staging.

    Proses ini tidak mengubah isi data. File staging selalu ditimpa
    agar task aman dijalankan ulang.
    """

    configure_process_log(
        "extract",
        context,
    )

    logging.info("========================================")
    logging.info("Memulai proses EXTRACT")
    logging.info("========================================")

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {RAW_FILE}"
        )

    logging.info(
        "File sumber ditemukan: %s",
        RAW_FILE,
    )

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.copy2(
        RAW_FILE,
        STAGING_FILE,
    )

    row_count = count_csv_rows(
        STAGING_FILE
    )

    logging.info("Data berhasil diekstrak.")
    logging.info(
        "File staging: %s",
        STAGING_FILE,
    )
    logging.info(
        "Jumlah record: %s",
        row_count,
    )
    logging.info("EXTRACT selesai.")


# 6. TASK 2 - VALIDATE
def validate_data(**context):
    """
    Melakukan pemeriksaan kualitas data.

    Pemeriksaan yang dilakukan:
        1. File staging tersedia.
        2. Header CSV tersedia.
        3. Semua kolom wajib tersedia.
        4. File memiliki record data.
        5. Menghitung nilai null.
        6. Menghitung record duplikat identik.
        7. Menghitung sale_id ganda.
        8. Memeriksa tipe data numerik.
        9. Memeriksa format tanggal.

    Task ini melaporkan masalah kualitas data melalui log.
    Pembersihan data dilakukan pada task transform_data.
    """

    configure_process_log(
        "validate",
        context,
    )

    logging.info("========================================")
    logging.info("Memulai proses VALIDATE")
    logging.info("========================================")

    if not STAGING_FILE.exists():
        raise FileNotFoundError(
            f"File staging tidak ditemukan: {STAGING_FILE}"
        )

    with STAGING_FILE.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(
                "File CSV tidak memiliki header."
            )

        fieldnames = [
            field.strip()
            for field in reader.fieldnames
            if field is not None
        ]

        logging.info(
            "Kolom CSV: %s",
            fieldnames,
        )

        missing_columns = [
            column
            for column in REQUIRED_COLUMNS
            if column not in fieldnames
        ]

        if missing_columns:
            raise ValueError(
                "Kolom wajib tidak ditemukan: "
                f"{missing_columns}"
            )

        rows = list(reader)

    if not rows:
        raise ValueError(
            "File staging tidak memiliki record data."
        )

    logging.info(
        "Jumlah record yang divalidasi: %s",
        len(rows),
    )

    # Pemeriksaan NULL
    null_counts = {
        column: 0
        for column in REQUIRED_COLUMNS
    }

    for row in rows:
        for column in REQUIRED_COLUMNS:
            if clean_text(row.get(column)) is None:
                null_counts[column] += 1

    total_null = sum(
        null_counts.values()
    )

    logging.info("Hasil pemeriksaan NULL:")

    for column, count in null_counts.items():
        logging.info(
            "NULL - %s: %s",
            column,
            count,
        )

        if count > 0:
            logging.warning(
                "Ditemukan %s nilai NULL pada kolom %s.",
                count,
                column,
            )

    # Pemeriksaan DUPLIKAT IDENTIK
    seen_rows = set()
    duplicate_count = 0

    for row in rows:
        row_tuple = normalize_row(row)

        if row_tuple in seen_rows:
            duplicate_count += 1
        else:
            seen_rows.add(row_tuple)

    logging.info(
        "Duplicate records identik: %s",
        duplicate_count,
    )

    if duplicate_count > 0:
        logging.warning(
            "Ditemukan %s record duplikat identik.",
            duplicate_count,
        )

    # Pemeriksaan SALE ID GANDA
    seen_sale_ids = set()
    duplicate_sale_id_count = 0

    for row in rows:
        sale_id = clean_text(
            row.get("sale_id")
        )

        if sale_id is None:
            continue

        if sale_id in seen_sale_ids:
            duplicate_sale_id_count += 1
        else:
            seen_sale_ids.add(sale_id)

    logging.info(
        "Sale ID ganda: %s",
        duplicate_sale_id_count,
    )

    if duplicate_sale_id_count > 0:
        logging.warning(
            "Ditemukan %s sale_id ganda.",
            duplicate_sale_id_count,
        )

    # Pemeriksaan TIPE DATA DAN FORMAT TANGGAL
    invalid_sale_id_count = 0
    invalid_quantity_count = 0
    invalid_unit_price_count = 0
    invalid_total_amount_count = 0
    invalid_date_count = 0

    for row in rows:
        sale_id_text = clean_text(
            row.get("sale_id")
        )
        quantity_text = clean_text(
            row.get("quantity")
        )
        unit_price_text = clean_text(
            row.get("unit_price")
        )
        total_amount_text = clean_text(
            row.get("total_amount")
        )
        sale_date_text = clean_text(
            row.get("sale_date")
        )

        if (
            sale_id_text is not None
            and safe_int(sale_id_text) is None
        ):
            invalid_sale_id_count += 1

        if (
            quantity_text is not None
            and safe_int(quantity_text) is None
        ):
            invalid_quantity_count += 1

        if (
            unit_price_text is not None
            and safe_float(unit_price_text) is None
        ):
            invalid_unit_price_count += 1

        if (
            total_amount_text is not None
            and safe_float(total_amount_text) is None
        ):
            invalid_total_amount_count += 1

        if (
            sale_date_text is not None
            and parse_date(sale_date_text) is None
        ):
            invalid_date_count += 1

 
    logging.info("----------------------------------------")
    logging.info("Ringkasan VALIDASI")
    logging.info(
        "Jumlah record          : %s",
        len(rows),
    )
    logging.info(
        "Total NULL             : %s",
        total_null,
    )
    logging.info(
        "Duplikat identik       : %s",
        duplicate_count,
    )
    logging.info(
        "Sale ID ganda          : %s",
        duplicate_sale_id_count,
    )
    logging.info(
        "Sale ID tidak valid    : %s",
        invalid_sale_id_count,
    )
    logging.info(
        "Quantity tidak valid   : %s",
        invalid_quantity_count,
    )
    logging.info(
        "Unit price tidak valid : %s",
        invalid_unit_price_count,
    )
    logging.info(
        "Total tidak valid      : %s",
        invalid_total_amount_count,
    )
    logging.info(
        "Tanggal tidak valid    : %s",
        invalid_date_count,
    )
    logging.info("----------------------------------------")

    if total_null > 0 or duplicate_count > 0:
        logging.warning(
            "Masalah kualitas data ditemukan. Pipeline dilanjutkan "
            "ke task TRANSFORM untuk melakukan pembersihan."
        )

    logging.info("VALIDATE selesai.")


# 7. TASK 3 - TRANSFORM
def transform_data(**context):
    """
    Membersihkan data dan membuat dua agregasi.

    Agregasi pertama:
        Penjualan berdasarkan tanggal.

    Agregasi kedua:
        Penjualan berdasarkan kota.

    Aturan pembersihan:
        1. Record identik hanya diproses satu kali.
        2. sale_id ganda hanya diproses satu kali.
        3. Record dengan field kritis tidak valid dilewati.
        4. Kota kosong diisi dengan "Unknown".
        5. total_amount kosong atau tidak konsisten dihitung ulang.
        6. Hasil agregasi ditulis ke file staging.
        7. Dua visualisasi PNG dibuat dari hasil agregasi.
    """

    configure_process_log(
        "transform",
        context,
    )

    logging.info("========================================")
    logging.info("Memulai proses TRANSFORM")
    logging.info("========================================")

    if not STAGING_FILE.exists():
        raise FileNotFoundError(
            f"File staging tidak ditemukan: {STAGING_FILE}"
        )

    daily_sales = {}

    city_sales = {}

    seen_rows = set()
    seen_sale_ids = set()

    input_count = 0
    valid_count = 0
    duplicate_count = 0
    duplicate_sale_id_count = 0
    skipped_count = 0
    recalculated_total_count = 0
    unknown_city_count = 0

    with STAGING_FILE.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(
                "File staging tidak memiliki header."
            )

        missing_columns = [
            column
            for column in REQUIRED_COLUMNS
            if column not in reader.fieldnames
        ]

        if missing_columns:
            raise ValueError(
                "Kolom wajib tidak ditemukan: "
                f"{missing_columns}"
            )

        for row_number, row in enumerate(
            reader,
            start=2,
        ):
            input_count += 1

            # Menghapus record yang benar-benar identik
            row_tuple = normalize_row(row)

            if row_tuple in seen_rows:
                duplicate_count += 1

                logging.warning(
                    "Baris %s dilewati karena merupakan "
                    "record duplikat identik.",
                    row_number,
                )

                continue

            seen_rows.add(row_tuple)

            # Membersihkan dan mengonversi nilai
            sale_id = safe_int(
                row.get("sale_id")
            )

            sale_date = parse_date(
                row.get("sale_date")
            )

            product_id = clean_text(
                row.get("product_id")
            )

            product_name = clean_text(
                row.get("product_name"),
                default="Unknown Product",
            )

            quantity = safe_int(
                row.get("quantity")
            )

            unit_price = safe_float(
                row.get("unit_price")
            )

            total_amount = safe_float(
                row.get("total_amount")
            )

            city = clean_text(
                row.get("city"),
                default="Unknown",
            )

            # Memeriksa field yang wajib valid
            invalid_reasons = []

            if sale_id is None:
                invalid_reasons.append(
                    "sale_id kosong atau tidak valid"
                )
            elif sale_id <= 0:
                invalid_reasons.append(
                    "sale_id harus lebih besar dari nol"
                )

            if sale_date is None:
                invalid_reasons.append(
                    "sale_date kosong atau tidak valid"
                )

            if product_id is None:
                invalid_reasons.append(
                    "product_id kosong"
                )

            if quantity is None:
                invalid_reasons.append(
                    "quantity kosong atau tidak valid"
                )
            elif quantity <= 0:
                invalid_reasons.append(
                    "quantity harus lebih besar dari nol"
                )

            if unit_price is None:
                invalid_reasons.append(
                    "unit_price kosong atau tidak valid"
                )
            elif unit_price <= 0:
                invalid_reasons.append(
                    "unit_price harus lebih besar dari nol"
                )

            if invalid_reasons:
                skipped_count += 1

                logging.warning(
                    "Baris %s dilewati: %s",
                    row_number,
                    "; ".join(invalid_reasons),
                )

                continue

            # Menghapus sale_id ganda
            if sale_id in seen_sale_ids:
                duplicate_sale_id_count += 1

                logging.warning(
                    "Baris %s dilewati karena sale_id %s "
                    "sudah pernah diproses.",
                    row_number,
                    sale_id,
                )

                continue

            seen_sale_ids.add(sale_id)

            # Menghitung ulang total_amount jika diperlukan
            expected_total = quantity * unit_price

            total_is_invalid = (
                total_amount is None
                or total_amount < 0
                or abs(
                    total_amount - expected_total
                ) > 0.01
            )

            if total_is_invalid:
                total_amount = expected_total
                recalculated_total_count += 1

                logging.warning(
                    "Baris %s memiliki total_amount kosong "
                    "atau tidak konsisten. Nilai dihitung "
                    "ulang menjadi %.2f.",
                    row_number,
                    total_amount,
                )

            if city == "Unknown":
                unknown_city_count += 1

            # AGREGASI 1: BERDASARKAN TANGGAL
            if sale_date not in daily_sales:
                daily_sales[sale_date] = {
                    "total_transactions": 0,
                    "total_quantity": 0,
                    "total_sales": 0.0,
                }

            daily_sales[sale_date][
                "total_transactions"
            ] += 1

            daily_sales[sale_date][
                "total_quantity"
            ] += quantity

            daily_sales[sale_date][
                "total_sales"
            ] += total_amount

            # AGREGASI 2: BERDASARKAN KOTA
            if city not in city_sales:
                city_sales[city] = {
                    "total_transactions": 0,
                    "total_quantity": 0,
                    "total_sales": 0.0,
                }

            city_sales[city][
                "total_transactions"
            ] += 1

            city_sales[city][
                "total_quantity"
            ] += quantity

            city_sales[city][
                "total_sales"
            ] += total_amount

            valid_count += 1

            logging.debug(
                "Record valid diproses: sale_id=%s, "
                "product=%s, date=%s, city=%s",
                sale_id,
                product_name,
                sale_date,
                city,
            )

    if valid_count == 0:
        raise ValueError(
            "Tidak ada record valid yang dapat ditransformasi."
        )

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Menulis agregasi berdasarkan tanggal
    daily_fieldnames = [
        "sale_date",
        "total_transactions",
        "total_quantity",
        "total_sales",
    ]

    with DAILY_TRANSFORMED_FILE.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=daily_fieldnames,
        )

        writer.writeheader()

        for sale_date in sorted(daily_sales):
            data = daily_sales[sale_date]

            writer.writerow({
                "sale_date": sale_date,
                "total_transactions": (
                    data["total_transactions"]
                ),
                "total_quantity": (
                    data["total_quantity"]
                ),
                "total_sales": round(
                    data["total_sales"],
                    2,
                ),
            })

    # Menulis agregasi berdasarkan kota
    city_fieldnames = [
        "city",
        "total_transactions",
        "total_quantity",
        "total_sales",
    ]

    with CITY_TRANSFORMED_FILE.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=city_fieldnames,
        )

        writer.writeheader()

        sorted_cities = sorted(
            city_sales.items(),
            key=lambda item: item[1]["total_sales"],
            reverse=True,
        )

        for city, data in sorted_cities:
            writer.writerow({
                "city": city,
                "total_transactions": (
                    data["total_transactions"]
                ),
                "total_quantity": (
                    data["total_quantity"]
                ),
                "total_sales": round(
                    data["total_sales"],
                    2,
                ),
            })

    create_visualizations(
        daily_sales,
        city_sales,
    )


    logging.info("----------------------------------------")
    logging.info("Ringkasan TRANSFORM")
    logging.info(
        "Record input                : %s",
        input_count,
    )
    logging.info(
        "Record valid                : %s",
        valid_count,
    )
    logging.info(
        "Duplikat identik dihapus    : %s",
        duplicate_count,
    )
    logging.info(
        "Sale ID ganda dihapus       : %s",
        duplicate_sale_id_count,
    )
    logging.info(
        "Record tidak valid          : %s",
        skipped_count,
    )
    logging.info(
        "Total amount dihitung ulang : %s",
        recalculated_total_count,
    )
    logging.info(
        "Kota diisi Unknown          : %s",
        unknown_city_count,
    )
    logging.info(
        "Jumlah tanggal agregasi     : %s",
        len(daily_sales),
    )
    logging.info(
        "Jumlah kota agregasi        : %s",
        len(city_sales),
    )
    logging.info("----------------------------------------")

    logging.info(
        "Agregasi harian disimpan: %s",
        DAILY_TRANSFORMED_FILE,
    )
    logging.info(
        "Agregasi kota disimpan: %s",
        CITY_TRANSFORMED_FILE,
    )
    logging.info("TRANSFORM selesai.")


# 8. TASK 4 - LOAD

def load_data(**context):
    """
    Memuat dua hasil transformasi ke folder output.

    Output:
        data/output/sales_daily.csv
        data/output/sales_by_city.csv

    Visualisasi telah disimpan langsung oleh task transform_data ke:
        data/output/visualizations/
    """

    configure_process_log(
        "load",
        context,
    )

    logging.info("========================================")
    logging.info("Memulai proses LOAD")
    logging.info("========================================")

    transformed_files = [
        {
            "source": DAILY_TRANSFORMED_FILE,
            "destination": DAILY_OUTPUT_FILE,
            "description": "agregasi harian",
        },
        {
            "source": CITY_TRANSFORMED_FILE,
            "destination": CITY_OUTPUT_FILE,
            "description": "agregasi berdasarkan kota",
        },
    ]

    # Memastikan semua hasil transformasi tersedia sebelum
    # menyalin file apa pun ke folder output.
    for transformed_file in transformed_files:
        source_file = transformed_file["source"]

        if not source_file.exists():
            raise FileNotFoundError(
                "File hasil transformasi tidak ditemukan: "
                f"{source_file}"
            )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    for transformed_file in transformed_files:
        source_file = transformed_file["source"]
        destination_file = transformed_file["destination"]
        description = transformed_file["description"]

        shutil.copy2(
            source_file,
            destination_file,
        )

        row_count = count_csv_rows(
            destination_file
        )

        logging.info(
            "File %s berhasil dimuat.",
            description,
        )
        logging.info(
            "Lokasi output: %s",
            destination_file,
        )
        logging.info(
            "Jumlah baris: %s",
            row_count,
        )

    visualization_files = [
        DAILY_VISUALIZATION_FILE,
        CITY_VISUALIZATION_FILE,
    ]

    for visualization_file in visualization_files:
        if not visualization_file.exists():
            raise FileNotFoundError(
                "File visualisasi tidak ditemukan: "
                f"{visualization_file}"
            )

        logging.info(
            "Visualisasi tersedia: %s",
            visualization_file,
        )

    logging.info("----------------------------------------")
    logging.info("Output yang berhasil dibuat:")
    logging.info("1. %s", DAILY_OUTPUT_FILE)
    logging.info("2. %s", CITY_OUTPUT_FILE)
    logging.info("3. %s", DAILY_VISUALIZATION_FILE)
    logging.info("4. %s", CITY_VISUALIZATION_FILE)
    logging.info("----------------------------------------")
    logging.info("LOAD selesai.")


# 9. DEFINISI DAG

with DAG(
    dag_id=DAG_ID,
    description=(
        "ETL data penjualan dengan validasi, "
        "dua agregasi, visualisasi, dan log per trigger"
    ),

    schedule_interval="@daily",

    start_date=datetime(2025, 1, 1),
    catchup=False,

    max_active_runs=1,

    tags=[
        "etl",
        "sales",
        "validation",
        "visualization",
    ],
) as dag:

    # Task 1 - Extract
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # Task 2 - Validate
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # Task 3 - Transform
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Task 4 - Load
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Urutan dependency wajib pipeline ETL.
    extract_task >> validate_task >> transform_task >> load_task
