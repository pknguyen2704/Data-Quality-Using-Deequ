# Spark Jobs for Data Quality
## Version 1: 
Sử dụng Deequ trực tiếp trong job ingestion dữ liệu từ các source về sink, sau đó ghi kết quả các metrics xuống Hudi và hiển thị trên Superset
- Chỉ gồm job đọc thông tin từ spark monitoring và deequ
- Chưa áp dụng Anomaly Detection
## Version 2: 
Sử dụng Deequ và Anomaly Detection với XGBoost để đọc dữ liệu từ các bảng đích
Sản phẩm là các job độc lập, các job sẽ đọc, phân tích, kiểm tra chất lượng dữ liệu ở bảng cuối, đã được lưu.
Các job bao gồm:
- AutoDQ_AnomalyDetection: Được sử dụng để chạy XGBoost để phát hiện các lỗi sai bất thường ở dữ liệu
- AutoDQ_DataProfiling: Được sử dụng để thu thập các metric về dữ liệu
- AutoDQ_ConstraintsSuggestion: Được sử dụng để đề xuất các ràng buộc chất lượng dữ liệu mới
- AutoDQ_ConstraintsVerification: Được sử dụng để kiểm tra các ràng buộc chất lượng dữ liệu đã được đề xuất
Các job được chạy bằng cách chạy lệnh submit spark job (cụ thể trong submit.txt)
Lưu ý: Các job cần được chạy theo thứ tự: DataProfiling -> ConstraintsSuggestion -> ConstraintsVerification -> AnomalyDetection

Mỗi job sẽ đọc dữ liệu từ Hive Metastore thông qua Thrift protocol để truy vấn và phân tích dữ liệu. Kết quả được lưu vào các bảng tương ứng trong Hive để phục vụ cho việc theo dõi và báo cáo chất lượng dữ liệu.

