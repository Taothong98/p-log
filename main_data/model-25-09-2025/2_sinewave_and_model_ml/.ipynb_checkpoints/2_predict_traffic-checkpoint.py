import os
import pandas as pd
import weka.core.jvm as jvm
from weka.classifiers import Classifier
from weka.core.serialization import read
from weka.core.converters import Loader
from weka.filters import Filter
import logging

# --- 1. Configuration ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
# ปิด logging แบบละเอียดไปก่อนเพื่อใช้ print() ที่ดูง่ายกว่า

model_filename = "linear_regression_S0_R00001.model"
input_base_dir = "output_dynamic_sine_wave"
output_base_dir = "output_predictions"
generate_arff = True
generate_csv = True

# --- 2. Setup ---
for dataset in ["train", "test"]:
    os.makedirs(os.path.join(output_base_dir, dataset), exist_ok=True)

# --- 3. Main Execution ---
try:
    print(" JVM starting...")
    jvm.start(max_heap_size="2048m")

    if not os.path.exists(model_filename):
        print(f"❌ ERROR: Model file not found -> {model_filename}")
        raise FileNotFoundError()

    print(f" Loading model '{model_filename}'...")
    cls = Classifier(jobject=read(model_filename))
    print("✅ Model loaded successfully.")

    processed_files, failed_files = 0, 0

    print("\n🚀 Starting prediction process...")
    for dataset in ["train", "test"]:
        input_dir = os.path.join(input_base_dir, dataset)
        output_dir = os.path.join(output_base_dir, dataset)
        
        print(f"\n📁 Processing dataset: '{dataset}' in '{input_dir}'")
        
        if not os.path.isdir(input_dir):
            print(f"   -> ⚠️  Warning: Input directory not found. Skipping.")
            continue

        arff_files = [f for f in os.listdir(input_dir) if f.endswith('.arff')]
        if not arff_files:
            print(f"   -> ⚠️  Warning: No .arff files found in this directory.")
            continue
        
        file_count = len(arff_files)
        for i, input_filename in enumerate(arff_files):
            print(f"   -> 📄 Processing file {i+1}/{file_count}: {input_filename}")
            input_filepath = os.path.join(input_dir, input_filename)
            base_output_filename = "predicted_" + os.path.splitext(input_filename)[0]
            
            try:
                loader = Loader(classname="weka.core.converters.ArffLoader")
                data = loader.load_file(input_filepath)
                if data.num_instances == 0:
                    print(f"      -> 🟡 Skipping empty file.")
                    failed_files += 1; continue

                # (ขั้นตอนการเตรียมข้อมูลเหมือนเดิม)
                remove = Filter(classname="weka.filters.unsupervised.attribute.Remove", options=["-R", "1"])
                remove.inputformat(data)
                data_no_time = remove.filter(data)
                
                add = Filter(classname="weka.filters.unsupervised.attribute.Add", options=["-N", "sum_size_increase_bytes", "-T", "NUM"])
                add.inputformat(data_no_time)
                data_ready_for_pred = add.filter(data_no_time)
                data_ready_for_pred.class_is_last()
                
                predictions = [round(float(cls.classify_instance(inst)), 4) for inst in data_ready_for_pred]
                
                with open(input_filepath, 'r') as f:
                    data_section = False; original_data_lines = []
                    for line in f:
                        if line.lower().strip().startswith('@data'): data_section = True; continue
                        if data_section and line.strip() and not line.startswith('%'): original_data_lines.append(line.strip())
                
                if len(original_data_lines) != len(predictions):
                    print(f"      -> 🔴 ERROR: Mismatch instance count. Skipping.")
                    failed_files += 1; continue

                if generate_arff:
                    output_filepath_arff = os.path.join(output_dir, base_output_filename + ".arff")
                    output_lines = [ f"@relation '{os.path.splitext(input_filename)[0]}_predictions'", "@attribute Time numeric", "@attribute target_send_rate numeric", "@attribute sum_size_increase_bytes numeric", "@data" ]
                    for j, original_line in enumerate(original_data_lines):
                        parts = original_line.split(',')
                        output_lines.append(f"{parts[0]},{parts[1]},{predictions[j]}")
                    with open(output_filepath_arff, 'w') as f: f.write('\n'.join(output_lines))

                if generate_csv:
                    output_filepath_csv = os.path.join(output_dir, base_output_filename + ".csv")
                    data_for_df = []
                    for j, original_line in enumerate(original_data_lines):
                        parts = original_line.split(',')
                        data_for_df.append({ 'Time': float(parts[0]), 'target_send_rate': float(parts[1]), 'sum_size_increase_bytes': predictions[j] })
                    df = pd.DataFrame(data_for_df)
                    df.to_csv(output_filepath_csv, index=False, float_format='%.4f')
                
                print(f"      -> ✅ Saved prediction results.")
                processed_files += 1

            except Exception as e:
                print(f"      -> 🔴 ERROR processing this file: {e}")
                failed_files += 1
                continue

    print("\n" + "="*40)
    print("🎉 Processing Complete! 🎉")
    print(f"   Successfully processed: {processed_files} files")
    print(f"   Failed to process: {failed_files} files")
    print("="*40)

except Exception as e:
    print(f"❌ An unexpected error occurred in the main script: {e}")
finally:
    if jvm.started:
        jvm.stop()
        print(" JVM stopped.")