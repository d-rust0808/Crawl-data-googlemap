# 🍰 Google Maps Crawler - Tiệm Bánh TP.HCM

## 🚀 Cách chạy

### Chạy trực tiếp

```bash
python batch_crawler.py
```

### Chạy với Docker

```bash
# Build và chạy
docker-compose up --build

# Chạy trong background
docker-compose up -d
```

## 📋 Danh sách jobs

File `list_jobs.txt` chứa 94 jobs để crawl tất cả tiệm bánh TP.HCM:

- Từ khóa chính: bánh kem, bakery, cake shop...
- Các loại bánh cụ thể: bánh kem chocolate, bánh kem dâu...
- Theo quận: quận 1, quận 3, quận 5...
- Khu vực nổi tiếng: Phú Mỹ Hưng, Thảo Điền...

## 🗄️ Database

- **Host**: localhost:5432
- **Database**: google-map-data
- **User**: cdudu
- **Password**: cdudu.com

## 📊 Kết quả

- Tự động tránh trùng lặp theo số điện thoại
- Lưu vào PostgreSQL database
- Export ra file Excel
- Logs chi tiết trong thư mục `logs/`
