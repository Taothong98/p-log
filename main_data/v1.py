import socket
import time
import random
import os
import threading
import json
import csv
from datetime import datetime

# --- CONFIGURATION ---
# ปรับค่าต่างๆ ได้ที่นี่

# การตั้งค่า Log Sender
LOGSTASH_HOST = "logstash"
LOGSTASH_PORT = 5044
INPUT_LOG_FILE = "/home/master/nas/examples_messages.txt"
SEND_RATE = 4  # เทียบเท่ากับ -c 4 (ส่ง 4 ข้อความต่อวินาที)

# การตั้งค่า Directory Monitor
TARGET_DIR = "/home/master/nas/outputlogs"
MONITOR_INTERVAL_SECONDS = 60  # ตรวจสอบและบันทึกทุกๆ 60 วินาที (1 นาที)
CSV_OUTPUT_FILE = "log_stats.csv"
JSON_OUTPUT_FILE = "log_stats.json" # เปลี่ยนเป็นไฟล์ JSON ปกติ

# --- END CONFIGURATION ---


def send_logs():
    """
    ฟังก์ชันสำหรับส่ง Syslog ไปยัง Logstash อย่างต่อเนื่อง
    ทำงานใน Thread แยก
    """
    print(f"🚀 Log sender started. Sending to {LOGSTASH_HOST}:{LOGSTASH_PORT} at {SEND_RATE} msg/sec.")
    try:
        with open(INPUT_LOG_FILE, 'r') as f:
            log_lines = [line.strip() for line in f.readlines()]

        if not log_lines:
            print(f"❌ Error: Input file '{INPUT_LOG_FILE}' is empty. Sender stopped.")
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        delay = 1 / SEND_RATE

        while True:
            message = random.choice(log_lines)
            sock.sendto(message.encode('utf-8'), (LOGSTASH_HOST, LOGSTASH_PORT))
            time.sleep(delay)

    except FileNotFoundError:
        print(f"❌ Error: Input file not found at '{INPUT_LOG_FILE}'. Sender stopped.")
    except Exception as e:
        print(f"❌ An error occurred in the log sender: {e}")

def get_directory_stats(directory_path):
    """
    คำนวณขนาดทั้งหมด, จำนวนไฟล์, และข้อมูลไฟล์ล่าสุดใน Directory
    """
    total_size = 0
    file_count = 0
    latest_file_path = None
    latest_file_mtime = 0

    try:
        for root, _, files in os.walk(directory_path):
            file_count += len(files)
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    file_mtime = os.path.getmtime(file_path)
                    if file_mtime > latest_file_mtime:
                        latest_file_mtime = file_mtime
                        latest_file_path = file_path
                except FileNotFoundError:
                    continue
        
        latest_file_info = {
            "name": os.path.basename(latest_file_path) if latest_file_path else "N/A",
            "size_bytes": os.path.getsize(latest_file_path) if latest_file_path else 0
        }

        return {
            "total_size_bytes": total_size,
            "file_count": file_count,
            "latest_file": latest_file_info
        }
        
    except FileNotFoundError:
        return None

def monitor_and_log():
    """
    ฟังก์ชันสำหรับตรวจสอบ Directory และบันทึกผลลงไฟล์
    ทำงานใน Thread แยก
    """
    print(f"🔎 Directory monitor started. Checking '{TARGET_DIR}' every {MONITOR_INTERVAL_SECONDS} seconds.")
    
    # เพิ่ม send_rate_msg_per_sec เข้าไปใน Header ของ CSV
    csv_headers = [
        "timestamp", "send_rate_msg_per_sec", "total_size_bytes", "total_size_mb", 
        "file_count", "latest_file_name", "latest_file_size_bytes"
    ]

    while True:
        stats = get_directory_stats(TARGET_DIR)
        
        if stats is None:
            print(f"⚠️ Warning: Directory '{TARGET_DIR}' not found. Retrying in {MONITOR_INTERVAL_SECONDS} seconds.")
            time.sleep(MONITOR_INTERVAL_SECONDS)
            continue
            
        timestamp = datetime.now().isoformat()
        total_size_mb = round(stats["total_size_bytes"] / (1024 * 1024), 2)
        
        # เตรียมข้อมูลสำหรับบันทึก โดยเพิ่มค่า SEND_RATE เข้าไปด้วย
        record = {
            "timestamp": timestamp,
            "send_rate_msg_per_sec": SEND_RATE,
            "total_size_bytes": stats["total_size_bytes"],
            "total_size_mb": total_size_mb,
            "file_count": stats["file_count"],
            "latest_file_name": stats["latest_file"]["name"],
            "latest_file_size_bytes": stats["latest_file"]["size_bytes"]
        }

        # บันทึกเป็น JSON แบบปกติ (อ่านไฟล์เก่า -> เพิ่มข้อมูลใหม่ -> เขียนทับทั้งหมด)
        try:
            all_data = []
            if os.path.exists(JSON_OUTPUT_FILE):
                with open(JSON_OUTPUT_FILE, 'r') as f:
                    try:
                        all_data = json.load(f)
                    except json.JSONDecodeError:
                        # กรณีไฟล์มีอยู่แต่ว่างเปล่า หรือข้อมูลเสียหาย
                        all_data = []
            
            all_data.append(record)
            
            with open(JSON_OUTPUT_FILE, 'w') as f:
                json.dump(all_data, f, indent=4)

        except Exception as e:
            print(f"❌ Error writing to JSON file: {e}")

        # บันทึกเป็น CSV
        try:
            file_exists = os.path.exists(CSV_OUTPUT_FILE)
            with open(CSV_OUTPUT_FILE, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(record)
        except Exception as e:
            print(f"❌ Error writing to CSV file: {e}")
            
        print(f"✅ [{timestamp}] Stats logged: {stats['file_count']} files, Total size: {total_size_mb} MB (Rate: {SEND_RATE} msg/s)")
        
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    sender_thread = threading.Thread(target=send_logs, daemon=True)
    sender_thread.start()

    monitor_thread = threading.Thread(target=monitor_and_log, daemon=True)
    monitor_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user. Exiting.")