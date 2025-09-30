# --- 1. Import Libraries ---
# นำเข้าไลบรารีที่จำเป็นสำหรับการทำงาน
import numpy as np                  # ใช้สำหรับการคำนวณทางคณิตศาสตร์และ Array
import pandas as pd                 # ใช้สำหรับจัดการข้อมูลในรูปแบบตาราง (DataFrame) และบันทึกเป็น CSV
import matplotlib                   # ไลบรารีหลักสำหรับการสร้างกราฟ
matplotlib.use('Agg')               # ตั้งค่า Backend ของ Matplotlib ให้ไม่แสดงหน้าต่างกราฟ แต่บันทึกเป็นไฟล์โดยตรง
import matplotlib.pyplot as plt     # ส่วนที่ใช้ในการวาดและปรับแต่งกราฟ
import os                           # ใช้สำหรับจัดการไฟล์และไดเรกทอรี (เช่น สร้างโฟลเดอร์)

# ==============================================================================
# --- 2. Parameters (ส่วนตั้งค่า) ---
# ส่วนนี้คือที่ที่คุณสามารถปรับค่าต่างๆ เพื่อควบคุมผลลัพธ์ของการจำลองข้อมูล
# ==============================================================================

# --- พารามิเตอร์การจำลอง Sine Wave แบบไดนามิก ---
A_PEAK_INCREASE = 100               # (Amplitude) จำนวน 'คน' ที่เพิ่มขึ้นสูงสุดในช่วงพีค
D_BASE_PEOPLE = 1                  # (Baseline) จำนวน 'คน' พื้นฐาน หรือค่ากลางของกราฟ
T_PERIOD_MINUTES = 1440             # (Period) คาบเวลาที่รูปแบบจะซ้ำ 1 รอบ (1440 นาที = 1 วัน)
ALPHA_VALUES = np.arange(0.1, 1.1, 0.1) # สร้างรายการค่า alpha [0.1, 0.2, ..., 1.0] เพื่อวนลูป
NOISE_STD = 30                      # (Noise) ค่าเบี่ยงเบนมาตรฐานของสัญญาณรบกวน ยิ่งมากยิ่งผันผวน
MIN_PEOPLE = 10                      # จำนวน 'คน' ขั้นต่ำที่สุดที่เป็นไปได้ ป้องกันค่าติดลบ
MESSAGES_PER_PERSON = 1            # ตัวคูณเพื่อแปลง 'จำนวนคน' เป็น 'อัตราการส่ง Log'

# --- พารามิเตอร์โครงสร้างไฟล์ ---
POINTS_PER_MINUTE = 1               # ความละเอียดของข้อมูล (จำนวนจุดข้อมูลต่อนาที)
train_days = 200                     # จำนวนวันสำหรับสร้างข้อมูลชุด Training
test_days = 2                       # จำนวนวันสำหรับสร้างข้อมูลชุด Testing
num_machines = 5                    # จำนวนเครื่อง (ไฟล์) ที่จะสร้างในแต่ละรอบการตั้งค่า
generate_plot = True                # ตั้งเป็น True เพื่อสร้างไฟล์กราฟ .png
generate_arff = True                # ตั้งเป็น True เพื่อสร้างไฟล์ .arff สำหรับ Weka

# --- 3. Main Script Logic ---
base_dir = 'output_dynamic_sine_wave'
train_dir = os.path.join(base_dir, 'train')
test_dir = os.path.join(base_dir, 'test')
os.makedirs(train_dir, exist_ok=True)
os.makedirs(test_dir, exist_ok=True)

def generate_dynamic_log_rate(total_days, points_per_minute, alpha):
    total_minutes = total_days * 24 * 60
    num_points = total_minutes * points_per_minute
    x = np.linspace(0, total_minutes, num_points, endpoint=False)
    sine_component = A_PEAK_INCREASE * np.sin(2 * np.pi * (1 / T_PERIOD_MINUTES) * (x % T_PERIOD_MINUTES))
    noise = np.random.normal(loc=0, scale=NOISE_STD, size=x.shape)
    people_count = alpha * sine_component + D_BASE_PEOPLE + (1 - alpha) * noise
    people_count = np.maximum(people_count, MIN_PEOPLE)
    final_log_rate = people_count * MESSAGES_PER_PERSON
    return x, final_log_rate

# --- ส่วนที่แก้ไข ---
# เปลี่ยนชื่อ Attribute ในฟังก์ชัน save_arff ให้ตรงกับที่โมเดลต้องการ
def save_arff(filename, time_data, rate_data, machine_id, alpha):
    with open(filename, 'w') as f:
        f.write(f'@relation Dynamic_Log_Rate_s{machine_id}_alpha{alpha:.1f}\n')
        f.write('@attribute Time numeric\n')
        # ** นี่คือจุดที่แก้ไข: เปลี่ยนชื่อคอลัมน์ที่ 2 **
        f.write('@attribute target_send_rate numeric\n')
        f.write('@data\n')
        for t, r in zip(time_data, rate_data):
            f.write(f'{t:.2f},{r:.2f}\n')

for dataset, num_days, output_dir in [('train', train_days, train_dir), ('test', test_days, test_dir)]:
    if num_days == 0: continue
    print(f'\nProcessing {dataset} dataset ({num_days} days)...')
    for alpha in ALPHA_VALUES:
        print(f'-- Processing for alpha = {alpha:.1f} --')
        for machine_id in range(1, num_machines + 1):
            try:
                time_total, rate_total = generate_dynamic_log_rate(num_days, POINTS_PER_MINUTE, alpha)
                
                # ** นี่คือจุดที่แก้ไข: เปลี่ยนชื่อคอลัมน์ใน DataFrame สำหรับ CSV **
                data = {'Time': time_total, 'target_send_rate': rate_total}
                df = pd.DataFrame(data)
                
                base_filename = f's{machine_id}_dynamic_rate_d{num_days}_alpha{alpha:.1f}'
                
                filename_csv = os.path.join(output_dir, f'{base_filename}.csv')
                df.to_csv(filename_csv, index=False)
                print(f'Saved: {filename_csv}')
                
                if generate_arff:
                    filename_arff = os.path.join(output_dir, f'{base_filename}.arff')
                    save_arff(filename_arff, time_total, rate_total, machine_id, alpha)
                    print(f'Saved: {filename_arff}')
                
                # (ส่วนที่เหลือเหมือนเดิม)
                min_rate = np.min(rate_total); max_rate = np.max(rate_total)
                print(f'Machine s{machine_id}, alpha={alpha:.1f}, Min Rate: {min_rate:.2f} msg/s, Max Rate: {max_rate:.2f} msg/s')
                
                if generate_plot:
                    plt.figure(figsize=(15, 6))
                    plt.plot(time_total, rate_total, label=f's{machine_id}, alpha={alpha:.1f}')
                    plt.title(f'Dynamic Sine Wave Log Rate (s{machine_id}, {num_days} days, alpha={alpha:.1f})')
                    plt.xlabel('Time (minutes)')
                    plt.ylabel('Target Send Rate (msg/s)') # อัปเดตชื่อแกน Y
                    plt.grid(True); plt.legend()
                    plot_filename = os.path.join(output_dir, f'{base_filename}.png')
                    plt.savefig(plot_filename)
                    plt.close()
                    print(f'Saved plot: {plot_filename}')
            except Exception as e:
                print(f'Error processing {dataset}, machine s{machine_id}, alpha={alpha:.1f}: {str(e)}')
                continue
print('\nSimulation completed.')