#!/usr/bin/env python3
import os
import subprocess
import pandas as pd
from itertools import product
import glob

# --- CONFIGURATION ---
HYPERPARAMETER_TUNING_MODE = True

# !!! สำคัญ: แก้ไข path ของ weka.jar ให้ถูกต้องตามเครื่องของคุณ !!!
WEKA_CLASSPATH = (
    "/opt/weka/weka-3-8-5/weka.jar:"
    "/opt/weka/weka-3-8-5/mtj-1.0.4.jar:"
    "/opt/weka/weka-3-8-5/native_ref-java-1.1.jar"  # <-- ใช้ไฟล์ 1.1 ที่เราดาวน์โหลดมา
)
JAVA_CMD = f"java -Xmx4g -cp \"{WEKA_CLASSPATH}\""

ALGORITHMS = [
    ("weka.classifiers.functions.LinearRegression", "-S 1 -R 1.0E-8", "linear_regression"),
    ("weka.classifiers.trees.RandomForest", "-I 100", "randomforest_regression"),
    ("weka.classifiers.functions.MultilayerPerceptron", "-L 0.3 -M 0.2 -N 500 -H a", "mlp_regression"),
    ("weka.classifiers.lazy.IBk", "-K 3", "ibk_regression"),
]

HYPERPARAMETER_GRIDS = {
    "weka.classifiers.functions.LinearRegression": { "S": [0, 1, 2], "R": [1.0e-8, 1.0e-6, 1.0e-4] },
    "weka.classifiers.trees.RandomForest": { "I": [100, 200], "K": [0, 5], "depth": [0, 10] },
    "weka.classifiers.lazy.IBk": { "K": [1, 3, 5, 7], "weight_option": ["", "-I", "-F"] },
    "weka.classifiers.functions.MultilayerPerceptron": { "L": [0.1, 0.3], "M": [0.2, 0.4], "N": [100, 500], "H": ["a", "10"] }
}

def discover_dataset_pairs():
    """ค้นหาคู่ไฟล์ .arff สำหรับ train/test โดยอัตโนมัติ"""
    print("--- Discovering dataset pairs ---")
    arff_files = set(glob.glob("*.arff"))
    found_pairs = []

    # ค้นหาไฟล์ train ทั้งหมด (ไฟล์ที่ลงท้ายด้วย _train.arff)
    train_files = [f for f in arff_files if f.endswith("_train.arff")]

    for train_file in train_files:
        # สร้างชื่อไฟล์ test ที่คาดหวังจากชื่อไฟล์ train
        potential_test_file = train_file.replace("_train.arff", "_test.arff")
        
        # ตรวจสอบว่าไฟล์ test นั้นมีอยู่จริงหรือไม่
        if potential_test_file in arff_files:
            # สร้าง output prefix จากชื่อไฟล์ (เช่น 'rate_vs_increase')
            output_prefix = train_file.replace("_train.arff", "")
            found_pairs.append((train_file, potential_test_file, output_prefix))
            print(f"  ✅ Found pair: {train_file} -> {potential_test_file}")

    if not found_pairs:
        print("  ❌ No valid train/test pairs found. Please check your .arff files.")
        
    return found_pairs

def print_file_head(filename, num_lines=5):
    """แสดงเนื้อหา 5 บรรทัดแรกของไฟล์เพื่อตรวจสอบ"""
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            print(f"\n--- Verifying content of: {filename} ---")
            for i in range(num_lines):
                line = f.readline().strip()
                if line: print(f"  | {line}")
            print("------------------------------------------")
    except Exception as e:
        print(f"Could not read head of {filename}: {e}")

def get_arff_instance_count(arff_file):
    """นับจำนวน instance (ข้อมูล) ในไฟล์ .arff"""
    if not os.path.isfile(arff_file): return 0
    try:
        with open(arff_file, 'r', encoding='utf-8', errors='ignore') as f: lines = f.readlines()
        data_section, instance_count = False, 0
        for line in lines:
            line = line.strip().lower()
            if not line or line.startswith('%'): continue
            if line.startswith('@data'): data_section = True; continue
            if data_section and line: instance_count += 1
        return instance_count
    except Exception as e:
        print(f"Error reading {arff_file}: {e}"); return 0

def parse_weka_output(file_path):
    """ดึงค่า metrics จากผลลัพธ์ของ Weka"""
    metrics = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        for section_name in ["training", "test"]:
            section_header = f"=== Error on {section_name} data ==="
            if section_header in content:
                metrics[section_name] = {}
                section_content = content.split(section_header)[1].split("===")[0]
                for line in section_content.strip().split('\n'):
                    line = line.strip()
                    if "Correlation coefficient" in line: metrics[section_name]["Correlation"] = float(line.split()[-1])
                    elif "Mean absolute error" in line: metrics[section_name]["MAE"] = float(line.split()[-1])
                    elif "Root mean squared error" in line: metrics[section_name]["RMSE"] = float(line.split()[-1])
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
    return metrics

def create_summary_csv(results_data, output_prefix):
    """สร้างไฟล์ CSV สรุปผล"""
    training_data = [res['training'] for res in results_data if res.get('training')]
    test_data = [res['test'] for res in results_data if res.get('test')]
    if not training_data and not test_data: print("No data to create summary CSV."); return
    
    df_train = pd.DataFrame(training_data)
    df_test = pd.DataFrame(test_data)

    df_train.to_csv(f"{output_prefix}_training_error_metrics.csv", index=False)
    df_test.to_csv(f"{output_prefix}_test_error_metrics.csv", index=False)
    print(f"\n✅ Summary CSV files created: {output_prefix}_training_error_metrics.csv, {output_prefix}_test_error_metrics.csv")

def run_weka_with_tuning(train_file, test_file, data_model_subdir):
    """รัน Weka พร้อมกับการทำ Hyperparameter Tuning"""
    final_best_results = []
    for algo_class, _, file_base in ALGORITHMS:
        algo_name = os.path.basename(algo_class).split('.')[-1]
        print(f"\n--- Tuning {algo_name} ---")
        algo_tuning_dir = os.path.join(data_model_subdir, algo_name)
        os.makedirs(algo_tuning_dir, exist_ok=True)
        param_grid = HYPERPARAMETER_GRIDS.get(algo_class)
        if not param_grid: continue
        
        keys, values = zip(*param_grid.items()); param_combinations = [dict(zip(keys, v)) for v in product(*values)]
        best_result_for_algo, best_rmse = None, float('inf')

        for i, params in enumerate(param_combinations):
            options_list, param_name_list = [], []; current_params = params.copy()
            if 'weight_option' in params:
                weight_flag = current_params.pop('weight_option')
                if weight_flag: options_list.append(weight_flag)
                param_name_list.append(f"w_{'inv' if weight_flag == '-I' else 'lin' if weight_flag == '-F' else 'none'}")
            for key, value in current_params.items():
                options_list.append(f"-{key} {value}"); param_name_list.append(f"{key}{value}")
            
            options = " ".join(options_list); param_str = "_".join(param_name_list).replace('.', '').replace(' ','')
            run_name = f"{file_base}_{param_str}"; model_output = os.path.join(algo_tuning_dir, f"{run_name}.model"); result_output = os.path.join(algo_tuning_dir, f"{run_name}.txt")
            weka_cmd = f"{JAVA_CMD} {algo_class} {options} -t {train_file} -T {test_file} -d {model_output} > {result_output}"
            print(f"  ({i+1}/{len(param_combinations)}) Running: {algo_name} with options: '{options}'")

            try:
                subprocess.run(weka_cmd, shell=True, check=True, capture_output=True, text=True)
                metrics = parse_weka_output(result_output)
                if 'test' in metrics and 'RMSE' in metrics.get('test', {}):
                    current_rmse = metrics['test']['RMSE']
                    if current_rmse < best_rmse:
                        best_rmse = current_rmse; best_params_str = ', '.join([f'{k}={v}' for k, v in params.items()])
                        best_result_for_algo = {
                            'training': {"Algorithm": algo_name, "Best_Params": best_params_str, **metrics.get('training', {})},
                            'test': {"Algorithm": algo_name, "Best_Params": best_params_str, **metrics.get('test', {})}
                        }
            except subprocess.CalledProcessError as e:
                print(f"    Error during Weka execution. Stderr: {e.stderr[:200]}")
        
        if best_result_for_algo:
            final_best_results.append(best_result_for_algo)
            print(f"  > Best RMSE for {algo_name}: {best_rmse:.6f} with params: {best_result_for_algo['test']['Best_Params']}")
    return final_best_results

def main():
    os.makedirs("model", exist_ok=True); os.makedirs("data_model", exist_ok=True)
    file_configs = discover_dataset_pairs()

    for train_file, test_file, output_prefix in file_configs:
        print(f"\n\n{'='*25}\n Processing dataset: {output_prefix}\n{'='*25}")
        
        current_data_model_dir = os.path.join("data_model", output_prefix)
        os.makedirs(current_data_model_dir, exist_ok=True)
        
        print_file_head(train_file); print_file_head(test_file)
        
        train_instances = get_arff_instance_count(train_file)
        test_instances = get_arff_instance_count(test_file)
        print(f"Instances found -> Train: {train_instances}, Test: {test_instances}")

        if train_instances < 2 or test_instances < 1:
            print(f"Insufficient data for {output_prefix}. Skipping model training."); continue

        if HYPERPARAMETER_TUNING_MODE:
            best_results = run_weka_with_tuning(train_file, test_file, current_data_model_dir)
            if best_results: create_summary_csv(best_results, output_prefix)
        else:
            print("Standard mode (no tuning) is currently disabled.")

    print("\n\nAll processing complete!")

if __name__ == "__main__":
    main()