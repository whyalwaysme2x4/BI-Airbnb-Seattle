# Hướng dẫn import dữ liệu Airbnb Seattle vào Power BI

File này hướng dẫn import dữ liệu đã xử lý trong `data/processed` vào Power BI, thiết lập relationship và tạo dashboard phân tích.

## 1. Import 5 file CSV

Mở Power BI Desktop và thực hiện:

1. Chọn **Home** > **Get data** > **Text/CSV**.
2. Import lần lượt 5 file trong thư mục `data/processed`:
   - `dim_date.csv`
   - `dim_listing.csv`
   - `dim_location.csv`
   - `dim_room_type.csv`
   - `fact_availability.csv`
3. Kiểm tra kiểu dữ liệu trong Power Query:
   - `dim_date[date_id]`: Whole Number
   - `dim_date[date]`: Date
   - `fact_availability[date_id]`: Whole Number
   - `fact_availability[listing_id]`: Whole Number
   - `fact_availability[price]`: Decimal Number
   - `fact_availability[estimated_revenue]`: Decimal Number
   - `available_flag`, `booked_flag`: Whole Number
4. Chọn **Close & Apply** để nạp dữ liệu vào model.

## 2. Thiết lập relationship

Vào **Model view** và tạo các relationship sau:

| From table | From column | To table | To column | Cardinality | Cross filter direction |
|---|---|---|---|---|---|
| `fact_availability` | `date_id` | `dim_date` | `date_id` | Many-to-one | Single |
| `fact_availability` | `listing_id` | `dim_listing` | `listing_id` | Many-to-one | Single |
| `dim_listing` | `location_id` | `dim_location` | `location_id` | Many-to-one | Single |
| `dim_listing` | `room_type_id` | `dim_room_type` | `room_type_id` | Many-to-one | Single |

Mô hình sau khi thiết lập:

```text
dim_date                  dim_location
   |                          |
   |                          |
fact_availability -- dim_listing -- dim_room_type
```

Lưu ý:

- `fact_availability` là bảng fact trung tâm.
- Các bảng `dim_*` dùng để filter, group by và drill down.
- Nên dùng cross filter direction là `Single` để mô hình rõ ràng và tránh quan hệ lọc vòng.

## 3. Tạo DAX measures

Tạo một bảng measures riêng nếu muốn quản lý measure gọn hơn, hoặc tạo trực tiếp trong `fact_availability`.

### Listing Count

```DAX
Listing Count =
DISTINCTCOUNT(dim_listing[listing_id])
```

### Average Price

```DAX
Average Price =
AVERAGE(fact_availability[price])
```

### Booking Rate

`Booking Rate` ở đây là tỷ lệ ngày không khả dụng, được suy ra từ `booked_flag`.

```DAX
Booking Rate =
DIVIDE(
    SUM(fact_availability[booked_flag]),
    COUNTROWS(fact_availability)
)
```

### Availability Rate

```DAX
Availability Rate =
DIVIDE(
    SUM(fact_availability[available_flag]),
    COUNTROWS(fact_availability)
)
```

### Estimated Revenue

```DAX
Estimated Revenue =
SUM(fact_availability[estimated_revenue])
```

Gợi ý định dạng:

- `Booking Rate`: Percentage
- `Availability Rate`: Percentage
- `Average Price`: Currency hoặc Decimal Number
- `Estimated Revenue`: Currency
- `Listing Count`: Whole Number

## 4. Gợi ý 4 trang dashboard

## Trang 1: Overview

Mục tiêu: cung cấp cái nhìn tổng quan về thị trường Airbnb Seattle.

Biểu đồ gợi ý:

- Card: `Listing Count`
- Card: `Average Price`
- Card: `Booking Rate`
- Card: `Availability Rate`
- Card: `Estimated Revenue`
- Line chart:
  - Axis: `dim_date[date]` hoặc `dim_date[month_name]`
  - Values: `Estimated Revenue`
- Clustered column chart:
  - Axis: `dim_room_type[room_type]`
  - Values: `Listing Count`
- Slicer:
  - `dim_date[month_name]`
  - `dim_room_type[room_type]`
  - `dim_location[neighbourhood_group_cleansed]`

## Trang 2: Price by Area

Mục tiêu: phân tích giá thuê theo khu vực.

Biểu đồ gợi ý:

- Filled map hoặc map:
  - Latitude: `dim_location[latitude]`
  - Longitude: `dim_location[longitude]`
  - Size: `Listing Count`
  - Tooltips: `Average Price`, `Booking Rate`, `Estimated Revenue`
- Bar chart:
  - Axis: `dim_location[neighbourhood_group_cleansed]`
  - Values: `Average Price`
- Bar chart Top N:
  - Axis: `dim_location[neighbourhood_cleansed]`
  - Values: `Average Price`
  - Filter: Top 10 by `Average Price`
- Matrix:
  - Rows: `dim_location[neighbourhood_group_cleansed]`, `dim_location[neighbourhood_cleansed]`
  - Values: `Listing Count`, `Average Price`, `Booking Rate`, `Estimated Revenue`
- Slicer:
  - `dim_room_type[room_type]`
  - `dim_date[month_name]`

## Trang 3: Room Type Analysis

Mục tiêu: phân tích mức độ phổ biến, giá và hiệu quả của từng loại phòng.

Biểu đồ gợi ý:

- Donut chart:
  - Legend: `dim_room_type[room_type]`
  - Values: `Listing Count`
- Clustered column chart:
  - Axis: `dim_room_type[room_type]`
  - Values: `Average Price`
- Clustered column chart:
  - Axis: `dim_room_type[room_type]`
  - Values: `Booking Rate`
- Bar chart:
  - Axis: `dim_room_type[room_type]`
  - Values: `Estimated Revenue`
- Table:
  - Columns: `dim_room_type[room_type]`, `Listing Count`, `Average Price`, `Booking Rate`, `Availability Rate`, `Estimated Revenue`
- Slicer:
  - `dim_location[neighbourhood_group_cleansed]`

## Trang 4: Booking Rate Analysis

Mục tiêu: phân tích tỷ lệ không khả dụng/ước tính đặt phòng theo thời gian và khu vực.

Biểu đồ gợi ý:

- Line chart:
  - Axis: `dim_date[date]`
  - Values: `Booking Rate`
- Clustered column chart:
  - Axis: `dim_date[month_name]`
  - Values: `Booking Rate`, `Availability Rate`
- Matrix hoặc heatmap:
  - Rows: `dim_date[day_name]`
  - Columns: `dim_date[month_name]`
  - Values: `Booking Rate`
- Bar chart:
  - Axis: `dim_location[neighbourhood_group_cleansed]`
  - Values: `Booking Rate`
- Scatter chart:
  - X-axis: `Average Price`
  - Y-axis: `Booking Rate`
  - Size: `Estimated Revenue`
  - Legend: `dim_room_type[room_type]`
- Slicer:
  - `dim_room_type[room_type]`
  - `dim_location[neighbourhood_group_cleansed]`
  - `dim_date[month_name]`

## 5. Ghi chú về Booking Rate

Trong project này:

```text
booked_flag = 1 nếu available = "f"
booked_flag = 0 nếu available = "t"
```

Vì vậy, `Booking Rate` là tỷ lệ ngày **không khả dụng** trên lịch Airbnb. Chỉ số này được dùng như tỷ lệ đặt phòng ước tính, không chắc chắn tuyệt đối là booking thật vì listing có thể không khả dụng do chủ nhà khóa lịch, bảo trì hoặc các lý do khác.
