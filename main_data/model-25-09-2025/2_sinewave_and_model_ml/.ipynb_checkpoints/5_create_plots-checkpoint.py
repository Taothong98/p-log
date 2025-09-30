import os
import pandas as pd
import matplotlib.pyplot as plt
import config  # Import ไฟล์ตั้งค่ากลาง

# --- 1. Configuration (ส่วนตั้งค่า) ---
# คุณยังคงตั้งค่าการทำงานหลักได้ที่นี่เหมือนเดิม
SOURCE_SCRIPT_NUMBER = 4
ATTRIBUTES_TO_PLOT = [
    'target_send_rate',
    'increase_size_bytes',
    'total_file_size_bytes',
    'rotated_total_dir_size_bytes',
]
TIME_AXIS_COLUMN = 'time_stamp'
PLOT_OUTPUT_DIR = "output_plots"


# --- 2. Setup ---
os.makedirs(PLOT_OUTPUT_DIR, exist_ok=True)
if SOURCE_SCRIPT_NUMBER == 3:
    input_base_dir = "output_cumulative_sizes"
elif SOURCE_SCRIPT_NUMBER == 4:
    input_base_dir = "output_log_rotation"
else:
    raise ValueError("SOURCE_SCRIPT_NUMBER must be 3 or 4")

# --- 3. Main Execution ---
try:
    print(f"\n🚀 Starting plot generation from '{input_base_dir}' with multiple Y-axes...")
    processed_files, failed_files = 0, 0

    for dataset in ["train", "test"]:
        input_dir = os.path.join(input_base_dir, dataset)
        output_dir_dataset = os.path.join(PLOT_OUTPUT_DIR, dataset)
        os.makedirs(output_dir_dataset, exist_ok=True)
        
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
            print(f"   -> 📈 Plotting file {i+1}/{file_count}: {input_filename}")
            input_filepath = os.path.join(input_dir, input_filename)
            base_output_filename = os.path.splitext(input_filename)[0]

            try:
                df = pd.read_csv(input_filepath)
                if df.empty:
                    print(f"      -> 🟡 Skipping empty file.")
                    failed_files += 1; continue

                if TIME_AXIS_COLUMN == 'time_stamp' and config.TIMESTAMP_FORMAT == "string":
                    df[TIME_AXIS_COLUMN] = pd.to_datetime(df[TIME_AXIS_COLUMN])

                # --- ✨ (ใหม่) ส่วนการสร้างกราฟที่มีหลายแกน Y ---
                fig, ax1 = plt.subplots(figsize=(17, 8))
                
                # กำหนดสีสำหรับแต่ละเส้น
                colors = plt.cm.get_cmap('tab10', len(ATTRIBUTES_TO_PLOT))
                
                # พล็อต Attribute แรกบนแกน Y ด้านซ้าย (ax1)
                first_col = ATTRIBUTES_TO_PLOT[0]
                if first_col in df.columns:
                    color = colors(0)
                    ax1.set_xlabel('Time', fontsize=12)
                    ax1.set_ylabel(first_col, color=color, fontsize=12)
                    ax1.plot(df[TIME_AXIS_COLUMN], df[first_col], color=color, label=first_col)
                    ax1.tick_params(axis='y', labelcolor=color)

                # สร้างแกน Y ใหม่สำหรับ Attribute ที่เหลือทางด้านขวา
                other_axes = [ax1]
                for j, col_to_plot in enumerate(ATTRIBUTES_TO_PLOT[1:], start=1):
                    if col_to_plot in df.columns:
                        ax_new = ax1.twinx() # สร้างแกนใหม่ที่ใช้แกน X ร่วมกัน
                        other_axes.append(ax_new)
                        
                        # จัดตำแหน่งแกน Y ใหม่ไม่ให้ซ้อนกัน
                        ax_new.spines['right'].set_position(('outward', 60 * (j - 1)))
                        
                        color = colors(j)
                        ax_new.set_ylabel(col_to_plot, color=color, fontsize=12)
                        ax_new.plot(df[TIME_AXIS_COLUMN], df[col_to_plot], color=color, label=col_to_plot)
                        ax_new.tick_params(axis='y', labelcolor=color)

                # ตกแต่งกราฟ
                ax1.set_title(f'Data Visualization for\n{input_filename}', fontsize=16)
                fig.tight_layout() # จัด Layout ให้พอดีกับแกน Y ที่เพิ่มเข้ามา
                
                # รวม legend จากทุกแกนมาไว้ที่เดียว
                lines, labels = [], []
                for ax in other_axes:
                    ax_lines, ax_labels = ax.get_legend_handles_labels()
                    lines.extend(ax_lines)
                    labels.extend(ax_labels)
                ax1.legend(lines, labels, loc='upper left')

                # บันทึกไฟล์
                output_plot_path = os.path.join(output_dir_dataset, base_output_filename + ".png")
                plt.savefig(output_plot_path)
                plt.close(fig)

                processed_files += 1

            except Exception as e:
                print(f"      -> 🔴 ERROR plotting this file: {e}")
                if 'fig' in locals() and plt.fignum_exists(fig.number):
                    plt.close(fig)
                failed_files += 1; continue
    
    print("\n" + "="*40)
    print("🎉 Plot Generation Complete! 🎉")
    print(f"   Plots saved to: '{PLOT_OUTPUT_DIR}'")
    print(f"   Successfully processed: {processed_files} files")
    print(f"   Failed to process: {failed_files} files")
    print("="*40)

except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")