import os
import hashlib
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# Định nghĩa đường dẫn các tầng Data Lake local
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "lake" / "bronze"

# Salt bảo mật dùng cho PII Masking
SALT = "SECURE_BANKING_SALT_2026"

def hash_pii(val: str) -> str:
    """
    Mục đích: Mã hóa một chiều (Hashing) thông tin định danh cá nhân (PII) 
    như mã khách hàng hoặc mã thẻ bằng thuật toán SHA-256 kết hợp Salt.
    Bản chất: Đảm bảo tuân thủ tiêu chuẩn bảo mật dữ liệu tài chính (GDPR/PCI-DSS),
    không thể dịch ngược lại thông tin gốc từ chuỗi mã hóa.
    """
    if pd.isna(val) or str(val).strip() == "":
        return ""
    salted_str = f"{str(val).strip()}_{SALT}"
    return hashlib.sha256(salted_str.encode("utf-8")).hexdigest()

def process_and_store_bronze(file_path: Path):
    """
    Mục đích: Đọc file CSV thô, thực hiện chuẩn hóa và áp dụng quy tắc bảo mật 
    để lưu vào tầng Bronze (Raw/Landing Zone của Data Lake).
    """
    file_name = file_path.name
    print(f"\n[BRONZE] Đang xử lý file thô: {file_name}...")

    # Đọc file CSV với định dạng phân cách cột bằng dấu ';' của bộ Berka
    df = pd.read_csv(file_path, sep=';', low_memory=False)
    total_rows = len(df)

    # Thực hiện PII Masking cho các bảng chứa dữ liệu nhạy cảm
    if file_name == "client.csv" and "client_id" in df.columns:
        print(" -> Áp dụng PII Masking (SHA-256) cho trường client_id...")
        df["client_id_hashed"] = df["client_id"].apply(hash_pii)

    elif file_name == "card.csv" and "card_id" in df.columns:
        print(" -> Áp dụng PII Masking (SHA-256) cho trường card_id...")
        df["card_id_hashed"] = df["card_id"].apply(hash_pii)

    # Đảm bảo thư mục lưu trữ Bronze tồn tại
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Lưu file xuống tầng Bronze dưới dạng CSV sạch (hoặc Parquet)
    output_path = BRONZE_DIR / file_name
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    
    print(f" -> Đã lưu thành công vào tầng Bronze: {output_path} ({total_rows:,} dòng)")

def main():
    if not RAW_DIR.exists():
        print(f"[LỖI] Không tìm thấy thư mục chứa dữ liệu thô: {RAW_DIR}")
        return

    csv_files = sorted(list(RAW_DIR.glob("*.csv")))
    if not csv_files:
        print(f"[CẢNH BÁO] Chưa có file .csv nào trong thư mục {RAW_DIR}")
        return

    print(f"[INFO] Phát hiện {len(csv_files)} file CSV cần đưa vào Data Lake (Bronze Layer).")
    for file_path in csv_files:
        process_and_store_bronze(file_path)

    print("\n[THÀNH CÔNG] Hoàn tất toàn bộ luồng Ingestion vào tầng Bronze Data Lake!")

if __name__ == "__main__":
    main()