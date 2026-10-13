# Ảnh chụp cần nộp

Lưu ảnh thật từ trình duyệt vào `report/screenshots/` với các tên sau.

1. `airflow_graph.png`: http://localhost:8080 (admin / admin), DAG `day09_video_pipeline`, chọn run `lab_step5_baseline` thành công rồi mở Graph. Mở nhóm mapped task `silver_frames` để thấy 26 task; giữ hai cổng `dbt_test_silver` và `dbt_test_gold` trong ảnh.
2. `airflow_failure_log.png`: chọn run `lab_step5_failure_recovery`, mở `silver_frames` ở map index 0 (`trip-00003`), tab Logs. Ảnh phải thấy dòng `simulated crash while extracting trip-...` và trip của bạn. Chụp trước khi clear để thấy trạng thái failed. Nếu xem lại sau phục hồi, chọn Attempt 1 trong Logs để thấy lỗi cũ.
3. `airflow_recovered_graph.png`: Graph của chính run `lab_step5_failure_recovery` sau khi clear riêng task lỗi, tiếp tục clear bảy task `upstream_failed`, và DAG thành công.
4. `metabase_dashboard.png`: dashboard Day 9 ở http://localhost:3000, đăng nhập `lab@example.com` / `Day9-lab-pass!`. Ảnh có các biểu đồ. Câu hỏi SQL riêng ở cuối dashboard có thể chụp riêng thành `metabase_question.png`. MSSV cuối `202602920`: train 745, val 142, test 132.
5. `minio_lake.png`: http://localhost:9001 (minioadmin / minioadmin), Object Browser → bucket `lake`, ảnh thấy các thư mục bronze, silver, gold.

Không cần trigger/reset thêm khi chỉ chụp ảnh. Chọn đúng run trong Airflow để tránh nhầm giữa run đầu thành công và run sập/phục hồi.

## Lưu ý phục hồi trên Airflow 2.10.5

Run lỗi có một mapped task đỏ (`silver_frames`, map index 0) và bảy task downstream ở `upstream_failed`. Các trip khác vẫn chạy. Chỉ clear task đỏ sẽ thử lại đúng một trip. Nếu bảy task downstream không tự chạy lại, cần clear riêng các task `upstream_failed` để scheduler xét lại; không clear các task đã success. Báo cáo ghi hành vi thực đo và số lần chạy của từng task.

Ảnh `airflow_failed_graph.png` giữ lại Graph đỏ trước phục hồi để đối chiếu; đây không phải ảnh Graph sau phục hồi.

## Đối chiếu ảnh trước nộp

- [x] `airflow_graph.png`: Graph baseline xanh, 26 mapped task và hai cổng dbt.
- [x] `airflow_failure_log.png`: đúng run lỗi, map index 0, thấy `trip-00003`.
- [x] `airflow_failed_graph.png`: Graph đỏ trước phục hồi, 7 task upstream_failed.
- [x] `minio_lake.png`: thấy bronze/silver/gold.
- [x] `metabase_dashboard.png`: thấy các biểu đồ và split 745/142/132.
- [x] `airflow_recovered_graph.png`: Graph xanh của **run `lab_step5_failure_recovery`**, toàn bộ task success.
- [x] `metabase_question.png`: câu hỏi **“Tỷ lệ khung bị loại theo trip — Nguyễn Minh Dương”** trên dashboard, thấy bảng kết quả với `trip-04622` bị loại 34/60 khung (56,67%).

Đã kiểm tra nội dung đủ 7 ảnh trước nộp; không còn ảnh cần bổ sung.
