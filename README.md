# BI phân tích dữ liệu Airbnb Seattle

Đây là project bài tập lớn môn Business Intelligence với đề tài phân tích dữ liệu Airbnb tại Seattle. Project xây dựng pipeline ETL bằng Python, tạo mô hình Data Warehouse dạng star schema và chuẩn bị dữ liệu đầu ra để trực quan hóa bằng Power BI.

## 1. Mục tiêu phân tích

Project tập trung trả lời các câu hỏi BI chính:

- Giá thuê thay đổi như thế nào theo khu vực, neighbourhood group và neighbourhood?
- Loại phòng nào phổ biến nhất tại Seattle?
- Tỷ lệ đặt phòng ước tính theo thời gian, khu vực và loại phòng là bao nhiêu?
- Doanh thu ước tính thay đổi như thế nào theo tháng, ngày trong tuần, loại phòng và khu vực?
- Những khu vực nào có giá thuê trung bình, số đêm không khả dụng và doanh thu ước tính cao?

## 2. Dataset sử dụng

Dataset: **Seattle Airbnb Open Data**

Nguồn Kaggle: https://www.kaggle.com/datasets/airbnb/seattle

Dataset gồm 3 file CSV chính:

- `data/raw/listings.csv`: thông tin listing, host, vị trí, loại phòng, giá niêm yết, số phòng, review score.
- `data/raw/calendar.csv`: dữ liệu theo từng listing và từng ngày, gồm ngày, trạng thái available và price.
- `data/raw/reviews.csv`: thông tin review của khách theo listing.

Theo mô tả Kaggle, dataset này mô tả hoạt động listing Airbnb tại Seattle, bao gồm listings, reviews và calendar availability.

Ghi chú: thư mục `data/raw` có thể chứa file CSV dung lượng lớn nên không được đưa lên Git. Người dùng cần tải dataset từ Kaggle và đặt 3 file `listings.csv`, `calendar.csv`, `reviews.csv` vào `data/raw` trước khi chạy ETL.

## 3. Cấu trúc thư mục project

```text
BI-Airbnb-Seattle/
├── data/
│   ├── raw/
│   │   ├── listings.csv
│   │   ├── calendar.csv
│   │   └── reviews.csv
│   └── processed/
│       ├── dim_date.csv
│       ├── dim_listing.csv
│       ├── dim_location.csv
│       ├── dim_room_type.csv
│       └── fact_availability.csv
├── etl/
│   └── etl_airbnb.py
├── sql/
│   └── schema.sql
├── requirements.txt
└── README.md
```

## 4. Cài đặt thư viện

```bash
python -m pip install -r requirements.txt
```

Thư viện chính:

- `pandas`: đọc CSV, làm sạch dữ liệu, xử lý bảng dimension/fact và xuất dữ liệu processed.

## 5. Cách chạy ETL

Chạy từ thư mục gốc của project:

```bash
python etl/etl_airbnb.py
```

Pipeline ETL thực hiện các bước:

- Đọc `listings.csv`, `calendar.csv`, `reviews.csv` từ `data/raw`.
- Làm sạch cột giá dạng `$1,234.00` thành số.
- Chuyển `calendar.date` và `reviews.date` sang kiểu ngày.
- Xử lý missing values cho các cột quan trọng.
- Tạo `available_flag`, `booked_flag`, `estimated_revenue`.
- Tạo các bảng dimension và fact.
- Xuất kết quả vào `data/processed`.
- In báo cáo kiểm tra dữ liệu cơ bản gồm số dòng, null quan trọng, min/max date và thống kê price.

## 6. Mô hình Data Warehouse

Project sử dụng mô hình **star schema** với một bảng fact trung tâm và bốn bảng dimension.

```text
                  dim_date
                     |
                     |
dim_room_type -- dim_listing -- dim_location
                     |
                     |
             fact_availability
```

Grain của bảng fact:

- Mỗi dòng trong `fact_availability` tương ứng với một listing trong một ngày cụ thể.
- Khóa chính logic: `listing_id` + `date_id`.

File tạo schema PostgreSQL:

```text
sql/schema.sql
```

## 7. Mô tả các bảng output

### dim_date.csv

Bảng dimension thời gian.

Cột chính:

- `date_id`: khóa ngày dạng `YYYYMMDD`.
- `date`: ngày thực tế.
- `year`, `quarter`, `month`, `month_name`, `day`.
- `day_of_week`, `day_name`, `week_of_year`.
- `is_weekend`: đánh dấu cuối tuần.

Mục đích phân tích:

- Phân tích giá, availability, booked nights và doanh thu ước tính theo ngày, tuần, tháng, quý, năm.

### dim_room_type.csv

Bảng dimension loại phòng.

Cột chính:

- `room_type_id`: khóa loại phòng.
- `room_type`: loại phòng, ví dụ `Entire home/apt`, `Private room`, `Shared room`.

Mục đích phân tích:

- So sánh số lượng listing, giá trung bình, tỷ lệ không khả dụng và doanh thu ước tính theo loại phòng.

### dim_location.csv

Bảng dimension khu vực.

Cột chính:

- `location_id`: khóa khu vực.
- `neighbourhood_cleansed`: khu vực chi tiết.
- `neighbourhood_group_cleansed`: nhóm khu vực.
- `city`, `state`, `zipcode`, `country`.
- `latitude`, `longitude`: tọa độ dùng cho bản đồ.

Mục đích phân tích:

- Phân tích giá thuê, listing count, tỷ lệ không khả dụng và doanh thu ước tính theo khu vực.

### dim_listing.csv

Bảng dimension listing.

Cột chính:

- `listing_id`: khóa listing từ Airbnb.
- `listing_name`, `host_id`, `host_name`.
- `property_type`, `room_type_id`, `location_id`.
- `accommodates`, `bathrooms`, `bedrooms`, `beds`, `bed_type`.
- `price`: giá niêm yết của listing.
- `minimum_nights`, `maximum_nights`.
- `number_of_reviews`, `actual_review_count`.
- `first_review_date`, `last_review_date`.
- `review_scores_rating`, `reviews_per_month`.

Mục đích phân tích:

- Phân tích đặc điểm listing, host, loại bất động sản, sức chứa, review và mối liên hệ với giá hoặc khả năng đặt phòng.

### fact_availability.csv

Bảng fact availability theo listing và ngày.

Cột chính:

- `listing_id`: khóa ngoại tới `dim_listing`.
- `date_id`: khóa ngoại tới `dim_date`.
- `available_flag`: bằng `1` nếu `available = "t"`, ngược lại bằng `0`.
- `booked_flag`: bằng `1` nếu `available = "f"`, ngược lại bằng `0`.
- `price`: giá theo ngày.
- `estimated_revenue`: doanh thu ước tính, tính bằng `booked_flag * price`.

Mục đích phân tích:

- Tính số ngày available, số ngày không khả dụng, tỷ lệ không khả dụng/ước tính đặt phòng và doanh thu ước tính.

## 8. Dashboard Power BI dự kiến

### Dashboard 1: Tổng quan thị trường Airbnb Seattle

Gợi ý visual:

- Card: tổng số listing.
- Card: giá trung bình.
- Card: occupancy rate ước tính.
- Card: estimated revenue.
- Line chart: estimated revenue theo tháng.
- Bar chart: số listing theo room type.

### Dashboard 2: Phân tích giá thuê theo khu vực

Gợi ý visual:

- Map: listing theo latitude/longitude.
- Bar chart: average price theo neighbourhood group.
- Matrix: neighbourhood group, neighbourhood, average price, total listings.
- Top N chart: khu vực có giá trung bình cao nhất.

### Dashboard 3: Phân tích loại phòng

Gợi ý visual:

- Donut chart: tỷ trọng listing theo room type.
- Column chart: average price theo room type.
- Column chart: booked nights ước tính theo room type.
- Scatter chart: price và review score theo room type.

### Dashboard 4: Availability và doanh thu ước tính

Gợi ý visual:

- Line chart: occupancy rate ước tính theo tháng.
- Heatmap: booked nights ước tính theo ngày trong tuần và tháng.
- Bar chart: estimated revenue theo neighbourhood group.
- Table: top listing theo estimated revenue.

## 9. DAX measure gợi ý

```DAX
Total Listings =
DISTINCTCOUNT(dim_listing[listing_id])

Total Available Nights =
SUM(fact_availability[available_flag])

Total Unavailable Nights =
SUM(fact_availability[booked_flag])

Estimated Occupancy Rate =
DIVIDE(
    [Total Unavailable Nights],
    COUNTROWS(fact_availability)
)

Average Daily Price =
AVERAGE(fact_availability[price])

Estimated Revenue =
SUM(fact_availability[estimated_revenue])

Revenue Per Listing =
DIVIDE(
    [Estimated Revenue],
    [Total Listings]
)

Average Listing Price =
AVERAGE(dim_listing[price])

Average Review Score =
AVERAGE(dim_listing[review_scores_rating])

Total Reviews =
SUM(dim_listing[actual_review_count])
```

Measure theo thời gian:

```DAX
Estimated Revenue MTD =
TOTALMTD(
    [Estimated Revenue],
    dim_date[date]
)

Estimated Revenue QTD =
TOTALQTD(
    [Estimated Revenue],
    dim_date[date]
)

Estimated Revenue YTD =
TOTALYTD(
    [Estimated Revenue],
    dim_date[date]
)
```

## 10. Ghi chú quan trọng về booked_flag

Trong dataset `calendar.csv`, cột `available` chỉ cho biết listing có khả dụng trong một ngày hay không.

Project này suy ra:

- `available_flag = 1` nếu `available = "t"`.
- `booked_flag = 1` nếu `available = "f"`.

Vì vậy, `booked_flag` không chắc chắn tuyệt đối là booking thật. Nó thể hiện trạng thái **không khả dụng** và được sử dụng như một chỉ báo **ước tính đặt phòng**. Một listing có thể không khả dụng vì đã được đặt, bị chủ nhà khóa lịch, bảo trì, thay đổi chính sách hoặc lý do khác.

Do đó, các chỉ số như `Estimated Occupancy Rate` và `Estimated Revenue` nên được hiểu là **ước tính dựa trên calendar availability**, không phải doanh thu hoặc booking thực tế được xác nhận từ Airbnb.

## 11. PostgreSQL

Tạo schema và bảng:

```bash
psql -d your_database -f sql/schema.sql
```

Thứ tự import CSV khuyến nghị:

1. `dim_date.csv`
2. `dim_room_type.csv`
3. `dim_location.csv`
4. `dim_listing.csv`
5. `fact_availability.csv`

Import dimension trước, fact sau để tránh lỗi foreign key.
