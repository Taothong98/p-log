# 4_simulate_log_rotation.py
import os
import pandas as pd
import config  # <--- (ใหม่) Import ไฟล์ตั้งค่า

# --- 1. Configuration ---
# ดึงค่ามาจากไฟล์ config ทั้งหมด
input_base_dir = "output_cumulative_sizes"
output_base_dir = "output_log_rotation"
RETENTION_DAYS = config.RETENTION_DAYS
TIMESTAMP_FORMAT = config.TIMESTAMP_FORMAT

# (ใหม่) สร้าง List ของคอลัมน์สุดท้ายโดยอัตโนมัติ
# โดยนำคอลัมน์จาก config มา แล้วแทนที่ 'total_dir_size_bytes' ด้วยตัวใหม่
FINAL_COLUMNS = [col if col != 'total_dir_size_bytes' else 'rotated_total_dir_size_bytes' for col in config.BASE_COLUMNS]

generate_arff = True
generate_csv = True

# --- 2. Setup ---
for dataset in ["train", "test"]:
    os.makedirs(os.path.join(output_base_dir, dataset), exist_ok=True)

def save_arff(df, filename, relation_name):
    df_arff = df.copy()
    with open(filename, 'w') as f:
        f.write(f"@relation {relation_name}\n\n")
        for column in df_arff.columns:
            if column == 'time_stamp' and TIMESTAMP_FORMAT == "string":
                f.write(f"@attribute {column} DATE \"yyyy-MM-dd'T'HH:mm:ss\"\n")
            else:
                f.write(f"@attribute {column} numeric\n")
        f.write("\n@data\n")
        if 'time_stamp' in df_arff.columns and TIMESTAMP_FORMAT == "string":
             df_arff['time_stamp'] = pd.to_datetime(df_arff['time_stamp']).dt.strftime("%Y-%m-%dT%H:%M:%S")
        f.write(df_arff.to_string(header=False, index=False))

# --- 3. Main Execution ---
try:
    print(f"\n🚀 Starting log rotation simulation (Retention = {RETENTION_DAYS} days)...")
    processed_files, failed_files = 0, 0

    for dataset in ["train", "test"]:
        input_dir = os.path.join(input_base_dir, dataset)
        output_dir = os.path.join(output_base_dir, dataset)
        print(f"\n📁 Processing dataset: '{dataset}' in '{input_dir}'")

        if not os.path.isdir(input_dir):
            print(f"   -> ⚠️  Warning: Input directory not found. Skipping.")
            continue
        
        csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
        if not csv_files:
            print(f"   -> ⚠️  Warning: No .csv files found.")
            continue
            
        file_count = len(csv_files)
        for i, input_filename in enumerate(csv_files):
            print(f"   -> 📄 Processing file {i+1}/{file_count}: {input_filename}")
            input_filepath = os.path.join(input_dir, input_filename)
            base_output_filename = os.path.splitext(input_filename)[0].replace("cumulative_", "rotated_")

            try:
                # อ่านไฟล์ CSV (ซึ่งมีคอลัมน์ตามที่ config.py กำหนด)
                df = pd.read_csv(input_filepath)
                if df.empty:
                    print(f"      -> 🟡 Skipping empty file.")
                    failed_files += 1; continue
                
                # --- ส่วนคำนวณหลัก (Sliding Window) ---
                df['lookback_time'] = df['Time'] - (RETENTION_DAYS * 1440)
                df_merged = pd.merge_asof(
                    df.sort_values('Time'),
                    df[['Time', 'total_file_size_bytes']],
                    left_on='lookback_time', right_on='Time',
                    suffixes=('', '_lookback')
                )
                df_merged['total_file_size_bytes_lookback'] = df_merged['total_file_size_bytes_lookback'].fillna(0)
                df_merged['rotated_total_dir_size_bytes'] = df_merged['total_file_size_bytes'] - df_merged['total_file_size_bytes_lookback']
                
                final_df = df_merged[FINAL_COLUMNS]

                # --- บันทึกไฟล์ผลลัพธ์ ---
                if generate_csv:
                    output_filepath_csv = os.path.join(output_dir, base_output_filename + ".csv")
                    final_df.to_csv(output_filepath_csv, index=False, float_format='%.4f')

                if generate_arff:
                    output_filepath_arff = os.path.join(output_dir, base_output_filename + ".arff")
                    relation_name = f"'{base_output_filename}_rotated'"
                    save_arff(final_df, output_filepath_arff, relation_name)

                print(f"      -> ✅ Saved rotated log size results.")
                processed_files += 1

            except Exception as e:
                print(f"      -> 🔴 ERROR processing this file: {e}")
                failed_files += 1; continue
    
    print("\n" + "="*40)
    print("🎉 Log Rotation Simulation Complete! 🎉")
    print(f"   Successfully processed: {processed_files} files")
    print(f"   Failed to process: {failed_files} files")
    print("="*40)

except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")