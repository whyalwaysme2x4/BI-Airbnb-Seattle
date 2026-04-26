# Hướng dẫn load dữ liệu Airbnb Seattle vào PostgreSQL

Tài liệu này hướng dẫn thiết lập PostgreSQL Data Warehouse cho project BI Airbnb Seattle theo quy trình:

```text
Raw CSV -> ETL Python -> PostgreSQL Data Warehouse -> Power BI
```

## 1. Cài PostgreSQL

Tải và cài PostgreSQL tại:

```text
https://www.postgresql.org/download/
```

Khi cài đặt, ghi nhớ các thông tin:

- Host: thường là `localhost`
- Port: mặc định `5432`
- User: thường là `postgres`
- Password: mật khẩu đặt trong lúc cài PostgreSQL

Có thể cài thêm pgAdmin để quản lý database bằng giao diện.

## 2. Tạo database

Mở pgAdmin hoặc `psql`, sau đó tạo database:

```sql
CREATE DATABASE airbnb_bi;
```

Nếu dùng `psql`:

```bash
psql -U postgres
```

Sau đó chạy:

```sql
CREATE DATABASE airbnb_bi;
\l
```

## 3. Cài thư viện Python

Từ thư mục gốc project:

```bash
python -m pip install -r requirements.txt
```

Các thư viện dùng cho bước load PostgreSQL:

- `sqlalchemy`: tạo kết nối PostgreSQL.
- `psycopg2-binary`: driver PostgreSQL cho Python.
- `python-dotenv`: đọc cấu hình database từ file `.env`.
- `pandas`: dùng trong bước ETL tạo processed CSV.

## 4. Cấu hình file .env

Copy file mẫu:

```bash
copy .env.example .env
```

Trên macOS/Linux:

```bash
cp .env.example .env
```

Mở file `.env` và chỉnh thông tin kết nối:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=airbnb_bi
DB_USER=postgres
DB_PASSWORD=your_password
```

Trong đó:

- `DB_HOST`: máy chủ PostgreSQL.
- `DB_PORT`: cổng PostgreSQL, thường là `5432`.
- `DB_NAME`: tên database, trong project này là `airbnb_bi`.
- `DB_USER`: user PostgreSQL.
- `DB_PASSWORD`: mật khẩu PostgreSQL.

File `.env` chứa mật khẩu nên đã được bỏ qua trong `.gitignore`.

## 5. Chạy ETL tạo dữ liệu processed

Trước khi load vào PostgreSQL, cần tạo 5 file CSV trong `data/processed`:

```bash
python etl/etl_airbnb.py
```

Kết quả cần có:

- `data/processed/dim_date.csv`
- `data/processed/dim_listing.csv`
- `data/processed/dim_location.csv`
- `data/processed/dim_room_type.csv`
- `data/processed/fact_availability.csv`

## 6. Load dữ liệu vào PostgreSQL

Chạy:

```bash
python etl/load_to_postgres.py
```

Script sẽ thực hiện:

1. Đọc cấu hình database từ `.env`.
2. Kết nối PostgreSQL.
3. Chạy `sql/schema.sql` để tạo lại schema `airbnb_seattle` và 5 bảng.
4. Load CSV theo đúng thứ tự:
   - `dim_date`
   - `dim_room_type`
   - `dim_location`
   - `dim_listing`
   - `fact_availability`
5. In số dòng đã load cho từng bảng.

## 7. Kiểm tra bảng bằng SQL

Kết nối vào database:

```bash
psql -U postgres -d airbnb_bi
```

Kiểm tra schema:

```sql
SET search_path TO airbnb_seattle;
\dt
```

Kiểm tra số dòng:

```sql
SELECT 'dim_date' AS table_name, COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_room_type', COUNT(*) FROM dim_room_type
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_listing', COUNT(*) FROM dim_listing
UNION ALL
SELECT 'fact_availability', COUNT(*) FROM fact_availability;
```

Kiểm tra khoảng ngày:

```sql
SELECT MIN(date) AS min_date, MAX(date) AS max_date
FROM airbnb_seattle.dim_date;
```

Kiểm tra logic flag:

```sql
SELECT COUNT(*) AS invalid_flag_rows
FROM airbnb_seattle.fact_availability
WHERE available_flag + booked_flag <> 1;
```

Kiểm tra price bị thiếu:

```sql
SELECT COUNT(*) AS missing_price_rows
FROM airbnb_seattle.fact_availability
WHERE price IS NULL;
```

## 8. Kết nối Power BI với PostgreSQL

Trong Power BI Desktop:

1. Chọn **Home** > **Get data** > **PostgreSQL database**.
2. Nhập:
   - Server: `localhost:5432`
   - Database: `airbnb_bi`
3. Chọn chế độ:
   - **Import**: phù hợp cho bài tập lớn, dữ liệu được nạp vào Power BI.
   - **DirectQuery**: dùng khi muốn truy vấn trực tiếp database.
4. Chọn 5 bảng trong schema `airbnb_seattle`:
   - `dim_date`
   - `dim_room_type`
   - `dim_location`
   - `dim_listing`
   - `fact_availability`
5. Chọn **Load**.

Sau khi import, kiểm tra relationship trong Model view:

- `fact_availability.date_id` -> `dim_date.date_id`
- `fact_availability.listing_id` -> `dim_listing.listing_id`
- `dim_listing.location_id` -> `dim_location.location_id`
- `dim_listing.room_type_id` -> `dim_room_type.room_type_id`

Power BI thường tự nhận relationship nếu khóa rõ ràng, nhưng nên kiểm tra lại để đảm bảo model đúng star schema.
