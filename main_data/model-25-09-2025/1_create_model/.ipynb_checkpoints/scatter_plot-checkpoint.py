import json
import os
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter
import arff 
from sklearn.model_selection import train_test_split

# ========================================
# 🎨 Plotting & Style Configuration
# ========================================

# --- Font Family ---
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Times New Roman'] + plt.rcParams['font.serif']
plt.rcParams['axes.unicode_minus'] = False

# --- Font Sizes ---
FONT_AXIS_LABEL = 20
FONT_TICK_LABEL = 18
FONT_TITLE = 22
FONT_LEGEND = 16

# --- Apply Font Sizes ---
plt.rcParams['axes.labelsize'] = FONT_AXIS_LABEL
plt.rcParams['xtick.labelsize'] = FONT_TICK_LABEL
plt.rcParams['ytick.labelsize'] = FONT_TICK_LABEL
plt.rcParams['axes.titlesize'] = FONT_TITLE
plt.rcParams['legend.fontsize'] = FONT_LEGEND

# --- General Plotting Settings ---
HIGH_RES_DPI = 300
PAPER_FORMAT_DPI = 75
SINGLE_PLOT_FIGSIZE = (12, 8)

# =======================
# 🔧 Configurable Params
# =======================
INPUT_JSON = "log_stats_final_test.json" # <-- ตั้งค่าเป็นไฟล์ที่คุณใช้
TEST_SET_SIZE = 0.2
RANDOM_STATE = 42

# ========================================================
# 💾 ARFF File Generation Function (ปรับปรุงใหม่)
# ========================================================
def save_dataframe_to_arff(df, output_path, relation_name):
    """
    Saves a pandas DataFrame to an ARFF file.
    """
    # เปลี่ยนชื่อ attribute ตามข้อมูลใหม่
    attributes = [
        ('target_send_rate', 'NUMERIC'),
        ('sum_size_increase_bytes', 'NUMERIC') # <-- เปลี่ยนชื่อ
    ]
    data = df.values.tolist()
    
    arff_data = {
        'relation': relation_name,
        'attributes': attributes,
        'data': data
    }
    
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            arff.dump(arff_data, f)
        print(f"  ✅ ARFF saved: {os.path.basename(output_path)}")
    except Exception as e:
        print(f"  ❌ Error saving ARFF file: {e}")

# ========================================================
# 📈 Plotting Function (ปรับปรุงใหม่)
# ========================================================
def generate_split_scatter_plot(df_train, df_test, output_dir, dpi):
    """
    Generates a scatter plot showing both training and testing data.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    fig, ax = plt.subplots(figsize=SINGLE_PLOT_FIGSIZE)
    
    # --- เปลี่ยนชื่อคอลัมน์ในการพล็อต ---
    ax.scatter(df_train['target_send_rate'], df_train['sum_size_increase_bytes'], 
               alpha=0.7, s=50, color='blue', label='Train Set')
    ax.scatter(df_test['target_send_rate'], df_test['sum_size_increase_bytes'], 
               alpha=0.7, s=50, color='red', label='Test Set')
    
    # --- เปลี่ยนชื่อแกน ---
    ax.set_xlabel("Target Send Rate (msg/s)")
    ax.set_ylabel("Sum of Size Increase (Bytes)") # <-- เปลี่ยนชื่อ
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.legend()
    
    ax.get_yaxis().set_major_formatter(
        FuncFormatter(lambda y, p: format(int(y), ','))
    )
    plt.xticks(rotation=45)
    
    output_path = os.path.join(output_dir, "scatter_rate_vs_size_increase_split.png")
    fig.savefig(output_path, dpi=dpi, bbox_inches='tight')
    plt.close(fig)
    
    print(f"📈 Scatter plot saved to '{os.path.basename(output_dir)}/' (DPI={dpi})")

# =======================
# 🔧 Main (ตรรกะใหม่ทั้งหมด)
# =======================
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, INPUT_JSON)
    if not os.path.exists(json_path):
        print(f"❌ Error: Input file not found at {json_path}"); return

    with open(json_path, "r", encoding="utf-8") as f: data = json.load(f)

    # --- ✨ ตรรกะใหม่ในการประมวลผลข้อมูล ---
    processed_records = []
    # วนลูปเพื่อเปรียบเทียบรายการปัจจุบัน (i) กับรายการถัดไป (i+1)
    for i in range(len(data) - 1):
        current_record = data[i]
        next_record = data[i+1]

        # ตรวจสอบว่า target_send_rate ของสองรายการที่ติดกันเท่ากันหรือไม่
        if current_record.get("target_send_rate") == next_record.get("target_send_rate"):
            
            # ดึงค่า send_rate (เป็น feature)
            send_rate = current_record.get("target_send_rate")
            
            # คำนวณผลรวม size_increase_bytes (เป็น target)
            sum_increase = current_record.get("size_increase_bytes", 0) + next_record.get("size_increase_bytes", 0)
            
            # เพิ่มข้อมูลที่ประมวลผลแล้วเข้าไปในลิสต์
            processed_records.append({
                "target_send_rate": send_rate,
                "sum_size_increase_bytes": sum_increase
            })

    if not processed_records:
        print("❌ Error: No adjacent records with matching 'target_send_rate' were found."); return
        
    df_processed = pd.DataFrame(processed_records)
    # อาจมี send_rate เดียวกันที่เกิดจาก repetition อื่นๆ เราจะเฉลี่ยค่าเพื่อลดความซ้ำซ้อน
    df_final = df_processed.groupby('target_send_rate')['sum_size_increase_bytes'].mean().reset_index()

    print(f"📊 Found {len(data)} records in total.")
    print(f"⚖ Processed and aggregated into {len(df_final)} unique data points.")

    # --- 📂 แบ่งข้อมูล Train/Test ---
    df_train, df_test = train_test_split(df_final, test_size=TEST_SET_SIZE, random_state=RANDOM_STATE)
    print(f" partitioning Train set size: {len(df_train)}, Test set size: {len(df_test)}")

    # --- 💾 สร้างไฟล์ ARFF สำหรับ Train และ Test ---
    print("\n--- Exporting ARFF Datasets ---")
    save_dataframe_to_arff(df_train, os.path.join(script_dir, "rate_vs_increase_train.arff"), "rate_vs_increase_training")
    save_dataframe_to_arff(df_test, os.path.join(script_dir, "rate_vs_increase_test.arff"), "rate_vs_increase_testing")
    
    # --- 🖼️ สร้างกราฟที่แสดงผลทั้งสองชุดข้อมูล ---
    print("\n--- Generating Scatter Plots ---")
    generate_split_scatter_plot(df_train, df_test, os.path.join(script_dir, "plots_send_rate_high_res"), HIGH_RES_DPI)
    generate_split_scatter_plot(df_train, df_test, os.path.join(script_dir, "plots_send_rate_small"), PAPER_FORMAT_DPI)

    print("\n✅ All tasks completed successfully.")

if __name__ == "__main__":
    main()