import socket
import time
import random
import os
import threading
import json
import csv
from datetime import datetime
import numpy as np

# --- CONFIGURATION ---
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# ** ปรับแก้แค่บรรทัดนี้เพื่อเลือกระหว่าง 'tcp' หรือ 'udp' **
PROTOCOL = 'tcp'  # สามารถเปลี่ยนเป็น 'udp' ได้
# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

# การตั้งค่า Log Sender
LOGSTASH_HOST = "logstash"
LOGSTASH_PORT = 5044
INPUT_LOG_FILE = "/home/master/nas/examples_messages.txt"

# พารามิเตอร์การจำลอง
MESSAGES_PER_PERSON = 10
BASE_PEOPLE = 1
MIN_PEOPLE = 1
PEAK_PEOPLE_INCREASE = 100
PEOPLE_NOISE_STD = 30
PEOPLE_PERIOD_MINUTES = 1440
PEOPLE_ALPHA = 0.5

# การตั้งค่า Directory Monitor
TARGET_DIR = "/home/master/nas/outputlogs"
MONITOR_INTERVAL_SECONDS = 60
# ** ชื่อไฟล์จะเปลี่ยนตาม PROTOCOL ที่เลือก **
CSV_OUTPUT_FILE = f"log_stats_{PROTOCOL}.csv"
JSON_OUTPUT_FILE = f"log_stats_{PROTOCOL}.json"

# --- END CONFIGURATION ---

def calculate_current_people(current_minute_of_day):
    sine_component = PEAK_PEOPLE_INCREASE * np.sin(2 * np.pi * (1 / PEOPLE_PERIOD_MINUTES) * current_minute_of_day)
    noise = np.random.normal(loc=0, scale=PEOPLE_NOISE_STD)
    raw_people_count = PEOPLE_ALPHA * sine_component + BASE_PEOPLE + (1 - PEOPLE_ALPHA) * noise
    final_people_count = int(np.maximum(raw_people_count, MIN_PEOPLE))
    return final_people_count

def send_logs():
    """
    ฟังก์ชันสำหรับส่ง Log โดยจะเลือกระหว่าง TCP หรือ UDP ตามตัวแปร PROTOCOL
    """
    print(f"🚀 {PROTOCOL.upper()}-based log sender started. Target: {LOGSTASH_HOST}:{LOGSTASH_PORT}.")
    
    try:
        with open(INPUT_LOG_FILE, 'r') as f:
            log_lines = [line.strip() for line in f.readlines()]
        if not log_lines:
            print(f"❌ Error: Input file '{INPUT_LOG_FILE}' is empty.")
            return
    except Exception as e:
        print(f"❌ Failed to read log file: {e}")
        return

    # --- Logic สำหรับ TCP ---
    if PROTOCOL.lower() == 'tcp':
        while True:
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((LOGSTASH_HOST, LOGSTASH_PORT))
                print(f"✅ TCP Connection to Logstash successful.")
                while True:
                    now = datetime.now()
                    minute_of_day = now.hour * 60 + now.minute
                    current_people = calculate_current_people(minute_of_day)
                    current_send_rate = current_people * MESSAGES_PER_PERSON
                    start_time = time.time()
                    for _ in range(current_send_rate):
                        message = random.choice(log_lines)
                        sock.sendall((message + '\n').encode('utf-8'))
                    elapsed_time = time.time() - start_time
                    time.sleep(max(0, 1.0 - elapsed_time))
            except (socket.error, ConnectionRefusedError, BrokenPipeError) as e:
                print(f"⚠️ TCP Connection lost: {e}. Retrying in 5 seconds...")
            finally:
                if sock: sock.close()
                time.sleep(5)

    # --- Logic สำหรับ UDP ---
    elif PROTOCOL.lower() == 'udp':
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            while True:
                now = datetime.now()
                minute_of_day = now.hour * 60 + now.minute
                current_people = calculate_current_people(minute_of_day)
                current_send_rate = current_people * MESSAGES_PER_PERSON
                start_time = time.time()
                for _ in range(current_send_rate):
                    message = random.choice(log_lines)
                    sock.sendto(message.encode('utf-8'), (LOGSTASH_HOST, LOGSTASH_PORT))
                elapsed_time = time.time() - start_time
                time.sleep(max(0, 1.0 - elapsed_time))
        except Exception as e:
            print(f"❌ An unexpected error occurred in UDP sender: {e}")
    else:
        print(f"❌ Invalid PROTOCOL configured: '{PROTOCOL}'. Please use 'tcp' or 'udp'.")

def get_directory_stats(directory_path):
    # (ฟังก์ชันนี้เหมือนเดิม ไม่มีการเปลี่ยนแปลง)
    total_size = 0; file_count = 0; latest_file_path = None; latest_file_mtime = 0
    try:
        for root, _, files in os.walk(directory_path):
            file_count += len(files)
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    file_size = os.path.getsize(file_path); total_size += file_size
                    file_mtime = os.path.getmtime(file_path)
                    if file_mtime > latest_file_mtime: latest_file_mtime = file_mtime; latest_file_path = file_path
                except FileNotFoundError: continue
        latest_file_info = {"name": os.path.basename(latest_file_path) if latest_file_path else "N/A", "size_bytes": os.path.getsize(latest_file_path) if latest_file_path else 0}
        return {"total_size_bytes": total_size, "file_count": file_count, "latest_file": latest_file_info}
    except FileNotFoundError: return None

def monitor_and_log():
    # (ฟังก์ชันนี้เหมือนเดิม ไม่มีการเปลี่ยนแปลง)
    print(f"🔎 Directory monitor started. Checking '{TARGET_DIR}' every {MONITOR_INTERVAL_SECONDS} seconds.")
    csv_headers = [
        "timestamp", "current_people", "messages_per_person", "final_send_rate_msg_per_sec",
        "total_size_bytes", "total_size_mb", "size_increase_bytes", "size_increase_mb",
        "file_count", "latest_file_name", "latest_file_size_bytes", "latest_file_size_mb"
    ]
    previous_total_size = 0
    while True:
        stats = get_directory_stats(TARGET_DIR)
        if stats is None: time.sleep(MONITOR_INTERVAL_SECONDS); continue
        now = datetime.now(); timestamp = now.isoformat()
        current_total_size = stats["total_size_bytes"]
        size_increase_bytes = current_total_size - previous_total_size
        if size_increase_bytes < 0: size_increase_bytes = 0
        previous_total_size = current_total_size
        total_size_mb = round(current_total_size / (1024 * 1024), 2)
        size_increase_mb = round(size_increase_bytes / (1024 * 1024), 2)
        latest_file_size_bytes = stats["latest_file"]["size_bytes"]
        latest_file_size_mb = round(latest_file_size_bytes / (1024 * 1024), 2)
        minute_of_day = now.hour * 60 + now.minute
        current_people = calculate_current_people(minute_of_day)
        final_send_rate = current_people * MESSAGES_PER_PERSON
        record = {
            "timestamp": timestamp, "current_people": current_people, "messages_per_person": MESSAGES_PER_PERSON,
            "final_send_rate_msg_per_sec": final_send_rate, "total_size_bytes": current_total_size,
            "total_size_mb": total_size_mb, "size_increase_bytes": size_increase_bytes, "size_increase_mb": size_increase_mb,
            "file_count": stats["file_count"], "latest_file_name": stats["latest_file"]["name"],
            "latest_file_size_bytes": latest_file_size_bytes, "latest_file_size_mb": latest_file_size_mb
        }
        try:
            all_data = []
            if os.path.exists(JSON_OUTPUT_FILE):
                with open(JSON_OUTPUT_FILE, 'r') as f:
                    try: all_data = json.load(f)
                    except json.JSONDecodeError: all_data = []
            all_data.append(record)
            with open(JSON_OUTPUT_FILE, 'w') as f: json.dump(all_data, f, indent=4)
        except Exception as e: print(f"❌ Error writing to JSON file: {e}")
        try:
            file_exists = os.path.exists(CSV_OUTPUT_FILE)
            with open(CSV_OUTPUT_FILE, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                if not file_exists: writer.writeheader()
                writer.writerow(record)
        except Exception as e: print(f"❌ Error writing to CSV file: {e}")
        print(f"✅ [{timestamp}] Stats logged: {stats['file_count']} files, {total_size_mb} MB (Increase: +{size_increase_mb} MB) (People: {current_people}, Rate: {final_send_rate} msg/s)")
        time.sleep(MONITOR_INTERVAL_SECONDS)

if __name__ == "__main__":
    sender_thread = threading.Thread(target=send_logs, daemon=True)
    monitor_thread = threading.Thread(target=monitor_and_log, daemon=True)
    sender_thread.start()
    monitor_thread.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user. Exiting.")