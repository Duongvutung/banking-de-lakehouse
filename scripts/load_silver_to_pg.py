import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Tải biến môi trường từ .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "bank_admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bank_secure_password")
DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "core_banking_dwh")

# Kết nối tới PostgreSQL thông qua SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "lake" / "bronze"

def load_silver_layer():
    """
    Mục đích: Tự động tạo schema 'silver' (nếu chưa có), sau đó đọc dữ liệu 
    từ tầng Bronze, chuẩn hóa schema và nạp vào PostgreSQL Silver Layer.
    """
    # 1. Tự động tạo schema 'silver' trong PostgreSQL trước khi nạp bảng
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
    print("[INFO] Đã đảm bảo schema 'silver' tồn tại trong database.")

    if not BRONZE_DIR.exists():
        print(f"[LỖI] Không tìm thấy thư mục Bronze: {BRONZE_DIR}")
        return

    csv_files = sorted(list(BRONZE_DIR.glob("*.csv")))
    if not csv_files:
        print(f"[CẢNH BÁO] Không có file CSV nào trong tầng Bronze.")
        return

    print(f"[INFO] Bắt đầu nạp dữ liệu từ Bronze lên PostgreSQL Silver Layer...")

    for file_path in csv_files:
        table_name = file_path.stem  # Tên bảng lấy theo tên file
        print(f"\n -> Đang xử lý bảng silver.{table_name}...")

        # Đọc dữ liệu từ file CSV chuẩn ở Bronze
        df = pd.read_csv(file_path, sep=';', low_memory=False)

        # Chuẩn hóa tên cột (viết thường, thay khoảng trắng bằng dấu gạch dưới)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Ghi vào PostgreSQL (schema='silver')
        df.to_sql(
            name=table_name,
            con=engine,
            schema='silver',
            if_exists='replace',
            index=False,
            chunksize=10000
        )
        print(f" -> Đã nạp thành công {len(df):,} dòng vào bảng silver.{table_name}")

    print("\n[THÀNH CÔNG] Hoàn tất nạp toàn bộ dữ liệu vào tầng Silver PostgreSQL!")

if __name__ == "__main__":
    load_silver_layer()