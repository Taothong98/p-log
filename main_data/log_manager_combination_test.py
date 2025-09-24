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
# ** ตั้งค่าการทดสอบ Combination ที่นี่ **

# 1. กำหนดช่วงของ Rate ที่ต้องการทดสอบ (Start, End, Step)
RATE_START = 0
RATE_END = 1000
RATE_STEP = 1

# 2. กำหนดระยะเวลาที่จะทดสอบในแต่ละ Rate (หน่วยเป็นวินาที)
DURATION_PER_RATE_SECONDS = 10

# 3. กำหนดจำนวนรอบที่จะทดสอบซ้ำทั้งหมด
TOTAL_REPETITIONS = 30

# 4. กำหนดเวลาพักระหว่างการทดสอบแต่ละ Combination (หน่วยเป็นวินาที)
REST_PERIOD_SECONDS = 60

# 5. กำหนดว่าจะให้เริ่มทำงานต่อจาก Loop ที่เท่าไหร่ (เริ่มใหม่ = 1)
START_FROM_LOOP = 1

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

# การตั้งค่าพื้นฐาน
PROTOCOL = 'tcp'
LOGSTASH_HOST = "logstash"
LOGSTASH_PORT = 5044
INPUT_LOG_FILE = "/home/master/nas/examples_messages.txt"
TARGET_DIR = "/home/master/nas/outputlogs"
MONITOR_INTERVAL_SECONDS = 60
CSV_OUTPUT_FILE = f"log_stats_final_test.csv"
JSON_OUTPUT_FILE = f"log_stats_final_test.json"

# --- END CONFIGURATION ---

SEND_RATES_TO_TEST = list(range(RATE_START, RATE_END + 1, RATE_STEP))

# --- Global State & Resource Management ---
CURRENT_LOOP = 0
TOTAL_LOOPS = 0
CURRENT_REPETITION = 0
CURRENT_SEND_RATE = 0
EXPERIMENT_STATUS = "Initializing"
log_lock = threading.Lock() # Lock สำหรับป้องกันการเขียนไฟล์พร้อมกัน
previous_total_size = 0

def log_current_stats(is_final_check=False):
    """
    ฟังก์ชันแกนหลักสำหรับตรวจสอบและบันทึกข้อมูล ป้องกันด้วย Lock
    """
    global previous_total_size
    
    with log_lock:
        if EXPERIMENT_STATUS in ["Initializing", "Completed"] and not is_final_check:
            return

        stats = get_directory_stats(TARGET_DIR)
        if stats is None: return
        
        now = datetime.now(); timestamp = now.isoformat()
        current_total_size = stats["total_size_bytes"]
        size_increase_bytes = current_total_size - previous_total_size
        if size_increase_bytes < 0: size_increase_bytes = 0
        previous_total_size = current_total_size
        
        total_size_mb = round(current_total_size / (1024 * 1024), 2)
        size_increase_mb = round(size_increase_bytes / (1024 * 1024), 2)
        latest_file_size_bytes = stats["latest_file"]["size_bytes"]
        latest_file_size_mb = round(latest_file_size_bytes / (1024 * 1024), 2)
        
        csv_headers = [
            "timestamp", "loop_number", "total_loops", "repetition", "target_send_rate", "experiment_status",
            "total_size_bytes", "total_size_mb", "size_increase_bytes", "size_increase_mb",
            "file_count", "latest_file_name", "latest_file_size_bytes", "latest_file_size_mb"
        ]
        
        record = {
            "timestamp": timestamp, "loop_number": CURRENT_LOOP, "total_loops": TOTAL_LOOPS,
            "repetition": CURRENT_REPETITION, "target_send_rate": CURRENT_SEND_RATE,
            "experiment_status": EXPERIMENT_STATUS, "total_size_bytes": current_total_size,
            "total_size_mb": total_size_mb, "size_increase_bytes": size_increase_bytes,
            "size_increase_mb": size_increase_mb, "file_count": stats["file_count"],
            "latest_file_name": stats["latest_file"]["name"], "latest_file_size_bytes": latest_file_size_bytes,
            "latest_file_size_mb": latest_file_size_mb
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
        
        log_prefix = "🏁" if is_final_check else "⏱️"
        print(f"{log_prefix} [{timestamp}] Stats logged: {total_size_mb} MB (+{size_increase_mb} MB) | Status: {EXPERIMENT_STATUS}")

def monitor_and_log():
    print(f"🔎 Directory monitor started. Checking '{TARGET_DIR}' every {MONITOR_INTERVAL_SECONDS} seconds.")
    while True:
        if EXPERIMENT_STATUS == "Completed":
            print("🔎 Monitor thread finished."); break
        
        # หน่วงเวลาก่อนแล้วค่อยตรวจสอบ เพื่อไม่ให้ทำงานซ้ำซ้อนกับการสรุปผลทันที
        time.sleep(MONITOR_INTERVAL_SECONDS)
        log_current_stats()

def send_logs_experiment():
    global CURRENT_LOOP, TOTAL_LOOPS, CURRENT_REPETITION, CURRENT_SEND_RATE, EXPERIMENT_STATUS

    TOTAL_LOOPS = len(SEND_RATES_TO_TEST) * TOTAL_REPETITIONS
    print("======================================================"); print("        🚀 Starting Final Combination Test         "); print("======================================================")
    print(f"Protocol: {PROTOCOL.upper()}"); print(f"Total Repetitions: {TOTAL_REPETITIONS}"); print(f"Generated Rates to Test: {SEND_RATES_TO_TEST} (msg/s)"); print(f"Total Test Loops: {TOTAL_LOOPS}")
    if START_FROM_LOOP > 1: print(f"▶️ Resuming from Loop: {START_FROM_LOOP}")
    print("------------------------------------------------------")
    
    try:
        with open(INPUT_LOG_FILE, 'r') as f:
            log_lines = [line.strip() for line in f.readlines()]
        if not log_lines: print("❌ Error: Input file is empty."); return
    except Exception as e: print(f"❌ Failed to read log file: {e}"); return
    
    loop_counter = 0
    for repetition in range(1, TOTAL_REPETITIONS + 1):
        for rate in SEND_RATES_TO_TEST:
            loop_counter += 1
            CURRENT_LOOP = loop_counter
            if loop_counter < START_FROM_LOOP:
                print(f"⏭️ Skipping Loop {loop_counter}/{TOTAL_LOOPS}..."); continue

            CURRENT_REPETITION = repetition; CURRENT_SEND_RATE = rate
            EXPERIMENT_STATUS = f"Loop {loop_counter}/{TOTAL_LOOPS} | Rep {repetition}/{TOTAL_REPETITIONS} | Rate {rate} msg/s"
            print(f"\n--- ▶️ Starting: {EXPERIMENT_STATUS} for {DURATION_PER_RATE_SECONDS}s ---")

            sock = None
            try:
                if PROTOCOL.lower() == 'tcp':
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); sock.connect((LOGSTASH_HOST, LOGSTASH_PORT))
                else:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                
                start_time = time.time()
                while time.time() - start_time < DURATION_PER_RATE_SECONDS:
                    loop_start_time = time.time()
                    for _ in range(rate):
                        message = random.choice(log_lines)
                        if PROTOCOL.lower() == 'tcp':
                            sock.sendall((message + '\n').encode('utf-8'))
                        else:
                            sock.sendto(message.encode('utf-8'), (LOGSTASH_HOST, LOGSTASH_PORT))
                    time.sleep(max(0, 1.0 - (time.time() - loop_start_time)))
            except Exception as e:
                print(f"❌ Error during test ({EXPERIMENT_STATUS}): {e}"); time.sleep(5)
            finally:
                if sock and PROTOCOL.lower() == 'tcp': sock.close()
            
            # ** สรุปผลทันทีหลังจบการทดสอบ **
            print(f"--- ✅ Test for Rate {rate} msg/s finished. Logging final stats... ---")
            log_current_stats(is_final_check=True)

            if loop_counter < TOTAL_LOOPS:
                print(f"--- ⏸️ Resting for {REST_PERIOD_SECONDS} seconds... ---")
                time.sleep(REST_PERIOD_SECONDS)

    EXPERIMENT_STATUS = "Completed"
    print("\n🎉🎉🎉 Final Combination Test Completed. 🎉🎉🎉")

def get_directory_stats(directory_path):
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

if __name__ == "__main__":
    sender_thread = threading.Thread(target=send_logs_experiment)
    monitor_thread = threading.Thread(target=monitor_and_log)
    
    sender_thread.start()
    monitor_thread.start()
    
    sender_thread.join()
    monitor_thread.join()
    print("Main thread finished.")