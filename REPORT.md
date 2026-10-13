# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Minh Dương` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602920` |
| GitHub | `minhduong814` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-09209` |

MSSV có chữ A nên lệnh `int(MSSV)` trong GUIDE không chạy trực tiếp. Bài tập chọn trip dùng phần số `2202602920 % 26 = 24`, tức `trip-09209` trong danh sách đã sắp xếp; MSSV chia split vẫn là `2A202602920`.

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` dạng ISO UTC, còn B dùng `timestamp_ms` dạng epoch mili giây; A có tốc độ `speed_mps` (m/s), B có `spd_kmh` (km/h).
Các cột hướng/GPS/độ cao đổi từ `heading_deg`, `lat`, `lon`, `alt_m` sang `hdg`, `latitude`, `longitude`, `altitude`; gia tốc `acc_x/acc_y` đổi thành `ax/ay`, góc lái `steer` thành `steering`.
Bàn đạp A (`gas`, `brake`) ở thang 0–1, còn B (`throttle_pct`, `brake_pct`) ở thang phần trăm; đồng hồ A không lệch, B nhanh hơn video 1.500 ms.
Bronze giữ riêng schema và các giá trị CSV dạng chữ để bảo toàn nguồn, cả ô trống và dữ liệu sai, phục vụ kiểm tra và xử lý lại khi đổi luật. Các cột metadata do pipeline thêm có kiểu riêng.
Kết quả thực tế: A 3.021 dòng, B 3.234 dòng, 26 trip và 2 video. Nạp lại được nhận diện bằng SHA-256 và không tăng số dòng; `bronze.raw_telemetry_a` chỉ có commit version 0.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi các tên cột của B về schema A, chia tốc độ km/h cho 3,6 và phần trăm bàn đạp cho 100; timestamp A được cast sang UTC, B trừ offset 1.500 ms rồi chuyển epoch ms sang timestamp.
Hai nguồn được UNION theo tên cột, phát hiện bản sao và phân loại tốc độ thiếu/ngoài khoảng 0–70 m/s. A còn 2.968 dòng sạch, B còn 3.181; cộng rejects vẫn đúng 3.021 và 3.234 dòng đầu vào.
Bắt đầu telemetry B là `2025-10-08 14:30:00.009+00:00`, chỉ sau video 9 ms, thay vì 1.509 ms nếu chưa hiệu chỉnh.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng rejects |
|---|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

106 dòng lỗi được giữ trong `silver.silver_telemetry_rejects` kèm lý do để đối chiếu nguồn và xử lý lại, không làm mất dấu lỗi bằng cách xoá. Bước 2 đạt 7/7 kiểm tra.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` có `severity: warn` vì khoảng mất tín hiệu cảm biến cần được báo nhưng vẫn có thể dùng những đoạn còn tốt; bước nối frame sẽ đánh dấu mất đồng bộ. `in_range` mặc định là `error` vì tốc độ vô lý đi vào dữ liệu huấn luyện sẽ làm sai nhãn.
Trong `break_gate.json`, nới luật làm sạch lên 1.000 m/s khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng và báo `PASS=5 WARN=1 ERROR=1 SKIP=0`.
Test vẫn giữ giới hạn 70 m/s, nên phát hiện được lỗi dù luật upstream bị nới. Cổng chất lượng kiểm tra đầu ra, không chỉ tin vào bước biến đổi.
Khôi phục luật gốc cho `PASS=6 WARN=1 ERROR=0 SKIP=0`: cảnh báo gap còn đó, lỗi tốc độ đã hết; DAG có thể đi tiếp khi không còn test error.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm chạy trên `trip-00003` (60 khung): toàn bảng có 1.246 dòng trước và sau hai lần chạy lại; dấu vân tay đều là `6c7480dfde7e`, `unchanged=true` trong `idempotency.json`.
Bảng thử dùng append hai lần có 120 dòng cho cùng 60 khung, tức nhân đôi. Pipeline dùng overwrite với predicate `trip_id` để thay đúng tập dòng của trip, giữ nguyên các trip khác.
Delta công bố thay đổi bằng một commit giao dịch: trước khi commit thành công, người đọc vẫn thấy phiên bản cũ, tránh trạng thái bảng bị mất trip nếu xoá trước rồi ghi thất bại.
Mã hiện tại partition vật lý theo `drive_id`, còn phạm vi ghi đè là predicate `trip_id`; 26 trip là 26 đơn vị xử lý logic. Tính giao dịch của bảng không đồng nghĩa toàn bộ thao tác ghi ảnh S3 cũng là một giao dịch.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Đen là độ sáng trung bình <12; tối là từ 12 đến <40. Khung đen bị loại khỏi Gold, khung tối chỉ gắn cờ và vẫn có thể được giữ nếu không vướng cờ loại khác. Tôi chọn cách này vì đường hầm/bóng râm vẫn là tình huống cần học.
A có 6 khung đen, 4 khung tối, 9 khung mờ; B có 4 khung đen, 61 khung tối và 0 khung mờ. Các cờ có thể chồng lấp nên không cộng trực tiếp để tính số khung bị loại.
Theo `threshold.json`, tăng ngưỡng từ 0 lên 6 làm số khung trùng tăng từ 61 lên 363 ở A, từ 8 lên 244 ở B. Tôi giữ ngưỡng 2 để bắt đứng hình/gần trùng mà tránh loại quá nhiều cảnh thay đổi thật.
Khi xe chạy chậm hoặc dừng, nhiều ảnh liên tiếp ít đổi nên dễ bị coi là trùng; khi xe chạy nhanh, ảnh đổi nhiều hơn. dHash còn chịu ảnh hưởng cảnh và chuyển động camera nên cần xem mẫu ảnh khi chỉnh ngưỡng.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
