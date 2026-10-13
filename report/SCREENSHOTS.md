# Ảnh chụp cần nộp

Lưu ảnh thật từ trình duyệt vào `report/screenshots/` với các tên sau.

1. `airflow_graph.png`: http://localhost:8080 (admin / admin), DAG `day09_video_pipeline`, chọn run `lab_step5_baseline` thành công rồi mở Graph. Mở nhóm mapped task `silver_frames` để thấy 26 task; giữ hai cổng `dbt_test_silver` và `dbt_test_gold` trong ảnh.
2. `airflow_failure_log.png`: chọn run `lab_step5_failure_recovery`, mở `silver_frames` ở map index 0 (`trip-00003`), tab Logs. Ảnh phải thấy dòng `simulated crash while extracting trip-...` và trip của bạn. Chụp trước khi clear để thấy trạng thái failed. Nếu xem lại sau phục hồi, chọn Attempt 1 trong Logs để thấy lỗi cũ.
3. `airflow_recovered_graph.png`: Graph của chính run `lab_step5_failure_recovery` sau khi clear riêng task lỗi, tiếp tục clear bảy task `upstream_failed`, và DAG thành công.
4. `metabase_dashboard.png`: dashboard Day 9 ở http://localhost:3000, đăng nhập `lab@example.com` / `Day9-lab-pass!`. Ảnh có các biểu đồ và câu hỏi SQL riêng về tỷ lệ khung bị loại theo trip; có thể chụp toàn trang.
5. `minio_lake.png`: http://localhost:9001 (minioadmin / minioadmin), Object Browser → bucket `lake`, ảnh thấy các thư mục bronze, silver, gold.

Không cần trigger/reset thêm khi chỉ chụp ảnh. Chọn đúng run trong Airflow để tránh nhầm giữa run đầu thành công và run sập/phục hồi.

## Lưu ý phục hồi trên Airflow 2.10.5

Run lỗi có một mapped task đỏ (`silver_frames`, map index 0) và bảy task downstream ở `upstream_failed`. Các trip khác vẫn chạy. Chỉ clear task đỏ sẽ thử lại đúng một trip. Nếu bảy task downstream không tự chạy lại, cần clear riêng các task `upstream_failed` để scheduler xét lại; không clear các task đã success. Báo cáo ghi hành vi thực đo và số lần chạy của từng task.

Ảnh `airflow_failed_graph.png` giữ lại Graph đỏ trước phục hồi để đối chiếu; đây không phải ảnh Graph sau phục hồi.
