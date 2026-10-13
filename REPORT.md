# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Minh Dương` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `202602920` |
| GitHub | `minhduong814` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-00003` |

MSSV chốt để nộp là `202602920`; `202602920 % 26 = 0` nên trip bài tập là `trip-00003`. Các bước đầu từng chạy với chuỗi `2A202602920`; bằng chứng lượt đầu được giữ riêng trong `report/evidence/initial_2A202602920/`, và bốn thí nghiệm đã chạy lại với MSSV chốt.

## Kết quả cuối

- `dataset_version` của bạn: `d154c90c8d3b`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `745 / 142 / 132` (tổng 1.019)
- `report/verify.txt`: đã tạo bằng `python3 scripts/check_submission.py --save-verify`, đạt `27/27 checks passed` sau lần chạy cuối từ lake trống.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_failure_log.png`, `airflow_failed_graph.png`, `airflow_recovered_graph.png` và `metabase_question.png`.

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

`leakage.json` dùng 1.019 khung hợp lệ thuộc 26 trip. Khi chia theo frame bằng hash, cả 26 trip bị chia qua nhiều split và cả 26 đều có ảnh trong train lẫn test.
Chia theo trip của Gold cho 0 trip bị chia cắt; mỗi drive có 9 trip train, 2 val, 2 test.
Ảnh liền nhau cùng chuyến có cảnh, góc máy và thời điểm rất giống nhau. Nếu xuất hiện ở cả train/test, mô hình có thể nhận lại cảnh đã học nên điểm test cao mà khả năng tổng quát sang chuyến mới chưa được đo đúng.
Thí nghiệm đếm rò rỉ, không huấn luyện mô hình, nên chưa định lượng mức tăng điểm kiểm thử.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Pipeline sắp xếp theo `frame_id`, băm SHA-256 chuỗi `frame_id|split`, rồi lấy 12 ký tự: version phản ánh danh sách ảnh và cách chia split, không đổi chỉ vì giờ chạy khác.
Với MSSV chốt `202602920`, lượt DAG chạy thêm và lượt phục hồi đều cho `d154c90c8d3b`, 745 train / 142 val / 132 test. Lần kiểm tra cuối từ lake trống và lần `export-dataset` kế tiếp đều giữ version này, 1.019 khung và cùng checksum manifest `9cb0eda512c3199f89341953d1ca735afb79c94b56e352e1bfe646dab44042f6` (`run_summary.json`, `export_repeat_final.json`).
MSSV được trộn vào thứ tự md5 của trip trong từng drive. Khi còn dùng chuỗi `2A202602920`, kết quả là `10cf9861186c`, 700/176/143; đổi sang MSSV chốt làm đổi trip thuộc từng split, vẫn giữ tổng 1.019 khung.
Băm không bao gồm toàn bộ byte ảnh hay mọi trường telemetry, nên không phải checksum của mọi nội dung dataset. MSSV khác có thể cho split khác nhưng không bảo đảm mọi MSSV đều khác kết quả.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest được lưu tại `lake/gold/datasets/d154c90c8d3b/manifest.json`; bản chụp cuối `report/evidence/manifest_final.json` ghi Bronze telemetry A/B và trips_raw version 0, video_files version 1; Silver frames version 25, telemetry/frames_flagged/frame_telemetry version 0. Bản sau phục hồi (`manifest_recovered.json`) có version bảng cao hơn do các lần chạy lại, nhưng cùng dataset_version.
Hai video đầu vào có SHA-256 A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.
Dataset loại ảnh đen, mờ, gần trùng hoặc mất đồng bộ, chia theo trip trong từng drive; manifest có danh sách ảnh Parquet và checksum manifest. Version bảng có thể tăng sau chạy lại, dù `dataset_version` không đổi.
Để tái tạo sau 3 tháng cần giữ đầu vào đúng checksum, phiên bản Delta cùng log/data còn tồn tại, mã nguồn và cấu hình ngưỡng/split, MSSV đầy đủ và môi trường chạy. Lineage hỗ trợ truy vết nhưng không tự lưu lại mọi phiên bản mã/cấu hình.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Run `lab_step5_failure_recovery` có đúng 1 task đỏ: `silver_frames`, map index 0, trip `trip-00003`; 7 task downstream là `upstream_failed`, 29 task khác đã thành công. Ảnh `airflow_failure_log.png` thấy dòng `simulated crash while extracting trip-00003`.
Clear riêng task đỏ khiến trip chạy lần thử 2 và thành công, nhưng Airflow 2.10.5 giữ 7 task downstream ở `upstream_failed`. Tôi tiếp tục clear riêng 7 task đó; không clear 29 task đã xanh. Đây là khác biệt thực tế so với hướng dẫn chỉ clear task đỏ.
Cuối cùng 37/37 task xanh: chỉ 1 task xử lý trip chạy lại; 7 task downstream thực thi lần đầu (try_number=1), các trip khác vẫn lần thử 1. Bằng chứng là `airflow_after_clear_trip.json`, hai file `airflow_clear_*.json` và `airflow_recovered.json`.
Sau phục hồi vẫn có 1.246 frame, riêng trip 60 frame, fingerprint `6c7480dfde7e`, và verify 27/27; không nhân đôi (`airflow_recovered_data.json`, `verify_after_recovery.txt`).
Trong thời gian chụp ảnh đã có một run khác với MSSV chốt `202602920`. Dataset sau phục hồi vẫn là `d154c90c8d3b`, cùng version của lượt dùng MSSV này; khác `10cf9861186c` của lượt đầu do đổi MSSV, không phải do retry. Graph sau phục hồi lưu ở `airflow_recovered_graph.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Thứ nhất, decode FFmpeg và upload ảnh: hiện mỗi trip tải lại video cả drive, Airflow LocalExecutor chỉ song song 2 task và `silver_frames` bị giới hạn 1 task/run. Với 500 xe/ngày, CPU, băng thông và thời gian chờ tăng mạnh; tôi tách worker xử lý video theo partition, cache/chia đoạn video và dùng cụm worker có khả năng tăng số lượng.
Thứ hai, DuckDB một file cục bộ cùng quá trình dbt trong một container: nhiều job ghi đồng thời dễ tranh chấp, RAM/CPU giới hạn. Tôi dùng engine phân tán phù hợp như Spark trên Delta và phối hợp các writer, giữ cổng kiểm tra chất lượng ở từng lớp.
Thứ ba, việc xuất nhiều JPEG, ghi lại toàn bảng/mart vào một Postgres và dashboard truy vấn trực tiếp sẽ tạo tải I/O lớn. Tôi giữ lake/object storage cho dữ liệu lớn, gom metadata theo partition, nạp warehouse tăng dần và tạo bảng tổng hợp phục vụ BI.
Các lựa chọn này nối với việc đo nút cổ chai, phân bố compute và chọn công cụ ELT ở slide 12, 22, 23; cần đo tải thực tế trước khi chọn cấu hình cụm.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: trip nào có tỷ lệ khung bị loại cao nhất, và tỷ lệ đó khác nhau thế nào giữa các trip?
Đã thêm câu hỏi “Tỷ lệ khung bị loại theo trip — Nguyễn Minh Dương” vào dashboard `http://localhost:3000/dashboard/2` (card 46).

```sql
select drive_id, trip_id, lighting, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 2) as dropped_pct,
       avg_speed_kmh, split
from marts.mart_trip_summary
order by dropped_pct desc, trip_id;
```

Kết quả: `trip-04622` của A bị loại 34/60 khung (56,67%), tiếp theo `trip-03904` 11/22 (50%) và `trip-02086` 28/60 (46,67%). Trip bài tập `trip-00003` bị loại 14/60 (23,33%).
Dùng `frames - frames_kept` đếm mỗi khung bị loại một lần, tránh cộng các cờ chồng lấp. Bảng này chỉ cho biết mức loại; muốn kết luận nguyên nhân cần xem thêm cờ và ảnh. Kết quả API được lưu trong `report/evidence/metabase_question.json`.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Tôi hiểu quy trình Bronze → Silver → Gold → warehouse, nhưng thao tác CLI khó hình dung và chưa đủ trực quan. Xem thư mục lake, Graph Airflow và dashboard giúp nối các lệnh với dữ liệu đang ở đâu.
Điều cần hiểu thêm là cách theo dõi một frame cụ thể từ video, qua cờ chất lượng, tới split và ảnh export.
Một lỗi có thể chưa bị bắt là tọa độ GPS nhảy bất thường dù tốc độ vẫn trong 0–70 m/s: các test hiện tại chưa kiểm tra sự nhất quán giữa quãng đường GPS và tốc độ theo thời gian.
