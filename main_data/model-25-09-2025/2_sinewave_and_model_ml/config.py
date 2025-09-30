# config.py
# ไฟล์นี้เป็นศูนย์กลางสำหรับตั้งค่าที่ใช้ร่วมกันระหว่างสคริปต์

# --- ตั้งค่าการสร้าง Timestamp ---
SIMULATION_START_DATE = "2020-01-01 00:00:00"
TIMESTAMP_FORMAT = "string"  # "string" หรือ "numeric"

# --- ตั้งค่านโยบายการเก็บรักษาข้อมูล ---
RETENTION_DAYS = 90

# --- ตั้งค่าคอลัมน์และลำดับของไฟล์ผลลัพธ์ ---
# สคริปต์ที่ 3 จะใช้ List นี้เพื่อสร้างไฟล์
# สคริปต์ที่ 4 จะอ่าน List นี้เพื่อทำความเข้าใจโครงสร้างและเพิ่มคอลัมน์ของตัวเอง
BASE_COLUMNS = [
    'time_stamp',
    'Time',
    'target_send_rate',
    'increase_size_bytes',
    'total_file_size_bytes',
    'total_dir_size_bytes',
]