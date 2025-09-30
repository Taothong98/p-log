# 3_calculate_cumulative_sizes.py
import os
import pandas as pd
from datetime import datetime, timedelta
import config # <--- (ใหม่) Import ไฟล์ตั้งค่า

# --- 1. Configuration (ส่วนตั้งค่า) ---
# ดึงค่ามาจากไฟล์ config ทั้งหมด
input_base_dir = "output_predictions"
output_base_dir = "output_cumulative_sizes"
SCALING_FACTOR = 6
FINAL_COLUMNS = config.BASE_COLUMNS # <--- (ใหม่) ใช้ค่าจาก config
TIMESTAMP_FORMAT = config.TIMESTAMP_FORMAT
SIMULATION_START_DATE = config.SIMULATION_START_DATE

generate_arff = True
generate_csv = True

# (โค้ดส่วนที่เหลือของไฟล์ที่ 3 ไม่ต้องแก้ไข)
# ... (คัดลอกส่วนที่เหลือของไฟล์ที่ 3 มาวางที่นี่) ...
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
    start_time_obj = datetime.strptime(SIMULATION_START_DATE, '%Y-%m-%d %H:%M:%S')
    print(f"Simulation Start Time is fixed to: {start_time_obj.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Timestamp format is set to: '{TIMESTAMP_FORMAT}'")
    print("\n🚀 Starting calculation of cumulative sizes...")
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
            print(f"   -> ⚠️  Warning: No .csv files found in this directory.")
            continue
        file_count = len(csv_files)
        for i, input_filename in enumerate(csv_files):
            print(f"   -> 📄 Processing file {i+1}/{file_count}: {input_filename}")
            input_filepath = os.path.join(input_dir, input_filename)
            base_output_filename = os.path.splitext(input_filename)[0].replace("predicted_", "cumulative_")
            try:
                df = pd.read_csv(input_filepath)
                if df.empty:
                    print(f"      -> 🟡 Skipping empty file.")
                    failed_files += 1; continue
                if TIMESTAMP_FORMAT == "string":
                    df['time_stamp'] = df['Time'].apply(lambda minutes: start_time_obj + timedelta(minutes=minutes))
                else:
                    df['time_stamp'] = df['Time'].apply(lambda minutes: int((start_time_obj + timedelta(minutes=minutes)).timestamp()))
                df['increase_size_bytes'] = df['sum_size_increase_bytes'] * SCALING_FACTOR
                df['total_file_size_bytes'] = df['increase_size_bytes'].cumsum()
                df['day'] = (df['Time'] // 1440).astype(int)
                df['total_dir_size_bytes'] = df.groupby('day')['increase_size_bytes'].cumsum()
                final_df = df[FINAL_COLUMNS]
                if generate_csv:
                    output_filepath_csv = os.path.join(output_dir, base_output_filename + ".csv")
                    csv_df = final_df.copy()
                    if TIMESTAMP_FORMAT == "string":
                        csv_df['time_stamp'] = pd.to_datetime(csv_df['time_stamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    csv_df.to_csv(output_filepath_csv, index=False, float_format='%.4f')
                if generate_arff:
                    output_filepath_arff = os.path.join(output_dir, base_output_filename + ".arff")
                    relation_name = f"'{base_output_filename}_cumulative'"
                    save_arff(final_df, output_filepath_arff, relation_name)
                print(f"      -> ✅ Saved cumulative size results.")
                processed_files += 1
            except Exception as e:
                print(f"      -> 🔴 ERROR processing this file: {e}")
                failed_files += 1; continue
    print("\n" + "="*40)
    print("🎉 Cumulative Calculation Complete! 🎉")
    print(f"   Successfully processed: {processed_files} files")
    print(f"   Failed to process: {failed_files} files")
    print("="*40)
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")