import sys
import shlex
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QLabel, QComboBox, QTextEdit, QListWidget,
                             QFileDialog, QMessageBox, QLineEdit, QInputDialog)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import subprocess
import os
import stat 
import time

try:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import config
    EMR_MASTER_DNS = config.EMR_MASTER_DNS
    EMR_KEY_PATH = config.EMR_KEY_PATH
    EMR_SSH_USER = config.EMR_SSH_USER
    S3_CODE_BUCKET = config.S3_CODE_BUCKET
except ImportError:
    print("WARNING: config.py not found. Using default values.")

def execute_remote_ssh_command(command_str, window_for_logging=None):
    if not EMR_MASTER_DNS or not EMR_KEY_PATH:
        if window_for_logging:
            log_message(window_for_logging, "ERROR: EMR Master DNS or Key Path is not set.")
        return None, "EMR connection information is missing."

    if not os.path.exists(EMR_KEY_PATH):
        if window_for_logging:
            log_message(window_for_logging, f"ERROR: SSH key file not found: {EMR_KEY_PATH}")
        return None, f"SSH key file not found: {EMR_KEY_PATH}"
    
    try:
        with open(EMR_KEY_PATH, 'r') as f:
            first_line = f.readline()
            if window_for_logging:
                log_message(window_for_logging, f"The SSH key file is readable.")
    except Exception as e:
        if window_for_logging:
            log_message(window_for_logging, f"ERROR: Unable to read SSH key file: {e}")
        return None, f"Unable to read SSH key file: {e}"
    try:
        file_stat = os.stat(EMR_KEY_PATH)
        file_mode = stat.S_IMODE(file_stat.st_mode)
        
        if file_mode != 0o400:
            if window_for_logging:
                log_message(window_for_logging, f"SSH key file permissions: {oct(file_mode)}, are being corrected...")
            os.chmod(EMR_KEY_PATH, 0o400)
            if window_for_logging:
                log_message(window_for_logging, "SSH key file permissions are set to 400.")
    except Exception as e:
        if window_for_logging:
            log_message(window_for_logging, f"WARNING: Failed to check file permissions: {e}")

    ssh_command = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "PasswordAuthentication=no",
        "-o", "IdentitiesOnly=yes",
        "-i", EMR_KEY_PATH,
        f"{EMR_SSH_USER}@{EMR_MASTER_DNS}",
        command_str
    ]
    
    if window_for_logging:
        log_message(window_for_logging, f"SSH command prepared.") 
        log_message(window_for_logging, f"Command: {' '.join(shlex.quote(c) for c in ssh_command[:6])}...")

    try:
        process = subprocess.Popen(
            ssh_command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True,
            env=os.environ.copy()
        )
        stdout, stderr = process.communicate(timeout=600)
        
        if window_for_logging:
            if stdout:
                log_message(window_for_logging, "--- Uzak Komut STDOUT ---")
                log_message(window_for_logging, stdout)
            if stderr:
                log_message(window_for_logging, "--- Uzak Komut STDERR ---")
                log_message(window_for_logging, stderr)
        
        if process.returncode != 0:
            if window_for_logging:
                log_message(window_for_logging, f"HATA: Uzak komut {process.returncode} ile sonlandı.")
            return None, stderr
            
        return stdout, stderr
        
    except subprocess.TimeoutExpired:
        if window_for_logging:
            log_message(window_for_logging, "ERROR: The remote command timed out.")
        process.kill()
        stdout, stderr = process.communicate()
        return None, "Time out" + stderr
    except Exception as e:
        if window_for_logging:
            log_message(window_for_logging, f"ERROR: Exception while running subprocess: {type(e).__name__}: {e}")
        return None, str(e)
app = None

def init_ui(window):
    window.setWindowTitle('BLM4120/4821 - Big Data Analysis Tool')
    window.setGeometry(100, 100, 900, 700)  
    main_layout = QVBoxLayout()
    window.setLayout(main_layout)
    data_selection_group = QVBoxLayout()
    category_layout = QHBoxLayout()
    lbl_category = QLabel('Data Category:')
    lbl_category.setMinimumWidth(120)
    combo_categories = QComboBox()
    categories = [
        "Performance Testing",      
        "Full Production Data",    
        "Geographic Specific",      
        "Manual Path Entry"         
    ]
    combo_categories.addItems(categories)
    category_layout.addWidget(lbl_category)
    category_layout.addWidget(combo_categories)
    dataset_layout = QHBoxLayout()
    lbl_dataset = QLabel('Veri Seti Seç:')
    lbl_dataset.setMinimumWidth(120)  
    combo_datasets = QComboBox()
    combo_datasets.setMinimumWidth(300)  
    dataset_layout.addWidget(lbl_dataset)
    dataset_layout.addWidget(combo_datasets)
    path_layout = QHBoxLayout()
    lbl_hdfs_path = QLabel('HDFS Path:')
    lbl_hdfs_path.setMinimumWidth(120)
    entry_hdfs_path = QLineEdit()
    entry_hdfs_path.setStyleSheet("""
        QLineEdit {
            background-color: #2b2b2b;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 4px;
            border-radius: 3px;
        }
        QLineEdit:disabled {
            background-color: #3a3a3a;
            color: #cccccc;
        }
    """)
    path_layout.addWidget(lbl_hdfs_path)
    path_layout.addWidget(entry_hdfs_path)
    data_selection_group.addLayout(category_layout)
    data_selection_group.addLayout(dataset_layout)
    data_selection_group.addLayout(path_layout)
    main_layout.addLayout(data_selection_group)
    function_layout = QHBoxLayout()
    lbl_function = QLabel('İstatistiksel Fonksiyon Seçin:')
    lbl_function.setMinimumWidth(120)
    combo_functions = QComboBox()
    functions = [
        "Min-Max Normalization", 
        "Skewness", 
        "Median", 
        "Standard Deviation", 
        "90th Percentile"
    ]
    combo_functions.addItems(functions)
    function_layout.addWidget(lbl_function)
    function_layout.addWidget(combo_functions)
    main_layout.addLayout(function_layout)
    btn_run = QPushButton('Start Analysis')
    btn_run.setStyleSheet("""
        QPushButton {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
            padding: 8px;
            border-radius: 4px;
        }
        QPushButton:hover {
            background-color: #45a049;
        }
        QPushButton:disabled {
            background-color: #cccccc;
        }
    """)
    main_layout.addWidget(btn_run)
    lbl_status = QLabel('Durum ve Loglar:')
    lbl_status.setStyleSheet("font-weight: bold; margin-top: 10px;")
    text_status_log = QTextEdit()
    text_status_log.setReadOnly(True)
    text_status_log.setMaximumHeight(200) 
    text_status_log.setStyleSheet("""
        QTextEdit {
            background-color: #2b2b2b;
            color: #ffffff;
            font-family: 'Consolas', monospace;
            font-size: 10px;
        }
    """)
    
    main_layout.addWidget(lbl_status)
    main_layout.addWidget(text_status_log)
    lbl_results = QLabel('Results:')
    lbl_results.setStyleSheet("font-weight: bold; margin-top: 10px;")
    text_results = QTextEdit()
    text_results.setReadOnly(True)
    text_results.setStyleSheet("""
        QTextEdit {
            background-color: #2b2b2b;
            color: #ffffff;
            border: 1px solid #555555;
            font-family: 'Consolas', monospace;
            padding: 8px;
            border-radius: 3px;
        }
    """)
    main_layout.addWidget(lbl_results)
    main_layout.addWidget(text_results)
    window.combo_categories = combo_categories
    window.combo_datasets = combo_datasets  
    window.entry_hdfs_path = entry_hdfs_path
    window.combo_functions = combo_functions
    window.text_status_log = text_status_log
    window.text_results = text_results
    window.btn_run = btn_run
    combo_categories.currentTextChanged.connect(lambda: update_dataset_options(window))
    combo_datasets.currentTextChanged.connect(lambda: update_hdfs_path_from_selection(window))
    btn_run.clicked.connect(lambda: handle_run_analysis(window))
    update_dataset_options(window)  
    window.show()

def update_dataset_options(window):
    category = window.combo_categories.currentText()
    window.combo_datasets.clear()  
    if category == "Performance Testing":
        datasets = [
            "1K Records (157 KB) - Baseline Test",
            "5K Records (786 KB) - Small Scale", 
            "10K Records (1.5 MB) - Medium Scale",
            "50K Records (7.8 MB) - Large Scale",
            "100K Records (15.7 MB) - Enterprise Scale"
        ]  
    elif category == "Full Production Data":
        datasets = [
            "PM2.5 Data 2018-2020 (Complete Dataset)",
            "Ozone Data 2018-2020 (Complete Dataset)", 
            "California PM2.5 Data (Regional)",
            "LA Station Time Series (Temporal Analysis)"
        ]
    elif category == "Geographic Specific":
        datasets = [
            "California Only - PM2.5 Measurements",
            "LA Metro Area - Station Network",
        ]     
    else:  
        # Advanced user'lar için custom path option
        datasets = ["Custom Path (Enter Below)"]
        window.entry_hdfs_path.setReadOnly(False)
        window.entry_hdfs_path.setStyleSheet("""
            QLineEdit {
                background-color: #1a1a1a;
                color: #ffffff;
                border: 2px solid #4CAF50;
                padding: 4px;
                border-radius: 3px;
            }
        """)
        window.entry_hdfs_path.setPlaceholderText("Enter HDFS path manually...")
        return
    window.combo_datasets.addItems(datasets)
    window.entry_hdfs_path.setReadOnly(True)
    window.entry_hdfs_path.setStyleSheet("""
        QLineEdit {
            background-color: #2b2b2b;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 4px;
            border-radius: 3px;
        }
        QLineEdit:disabled {
            background-color: #3a3a3a;
            color: #cccccc;
        }
    """)
    update_hdfs_path_from_selection(window)

def update_hdfs_path_from_selection(window):
    category = window.combo_categories.currentText()
    dataset = window.combo_datasets.currentText()
    if category == "Performance Testing":
        if "1K" in dataset:
            path = "/user/hadoop/epa_air_quality/test_data/sample_1000_pm25_performance_test_data.csv"
        elif "5K" in dataset:
            path = "/user/hadoop/epa_air_quality/test_data/sample_5000_pm25_performance_test_data.csv"
        elif "10K" in dataset:
            path = "/user/hadoop/epa_air_quality/test_data/sample_10000_pm25_performance_test_data.csv"
        elif "50K" in dataset:
            path = "/user/hadoop/epa_air_quality/test_data/sample_50000_pm25_performance_test_data.csv"
        elif "100K" in dataset:
            path = "/user/hadoop/epa_air_quality/test_data/sample_100000_pm25_performance_test_data.csv"
        else:
            path = "/user/hadoop/epa_air_quality/test_data/"  
    
    elif category == "Full Production Data":
        if "PM2.5 Data 2018-2020" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_pm25_data_2018_2020.csv"
        elif "Ozone" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_ozone_data_2018_2020.csv"
        elif "California" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_california_pm25_data.csv"
        elif "LA Station" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_la_station_timeseries.csv"
        else:
            path = "/user/hadoop/epa_air_quality/raw/"  # Fallback
    elif category == "Geographic Specific":
        if "California" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_california_pm25_data.csv"
        elif "LA Metro" in dataset:
            path = "/user/hadoop/epa_air_quality/raw/optimized_la_station_timeseries.csv"
        else:
            path = "/user/hadoop/epa_air_quality/raw/"  # Fallback
    
    else: 
        return
    window.entry_hdfs_path.setText(path)

def log_message(window, message):
    window.text_status_log.append(message)
    QApplication.processEvents()

def show_results(window, result_text):
    window.text_results.setText(result_text)
    QApplication.processEvents()

def handle_run_analysis(window):
    selected_category = window.combo_categories.currentText()
    if selected_category == "Performance Testing":
        analysis_start_time = time.time()
        show_performance_metrics = True
        log_message(window, "🔬 Performance Testing modu - Timing ölçümü aktif")
    else:
        show_performance_metrics = False
        
    log_message(window, "Starting analysis...")
    selected_function = window.combo_functions.currentText()
    hdfs_input_path = window.entry_hdfs_path.text()
    if not hdfs_input_path:
        QMessageBox.warning(window, "Login Error", "Please specify HDFS login path.")
        log_message(window, "ERROR: HDFS input path is empty.")
        return
      
    log_message(window, f"Selected Function: {selected_function}")
    log_message(window, f"Entryway: {hdfs_input_path}")
    window.btn_run.setEnabled(False)
    window.text_results.clear()
    QApplication.processEvents()
    if show_performance_metrics:
        mr_prep_start = time.time()
    mr_script_source_s3_path = ""
    emr_mr_script_target_dir = ""
    local_mapper_path_on_emr = ""
    local_reducer_path_on_emr = ""
    hdfs_output_path = ""
    job_name = ""
    if selected_function == "Skewness":
        job_name = "GUI_Skewness_Analysis"
        mr_script_source_s3_path = f"{S3_CODE_BUCKET}/skewness/"
        emr_mr_script_target_dir = "/home/hadoop/mr_scripts_for_gui/skewness"  # Tam yol
        local_mapper_path_on_emr = "skewness_stats_mapper.py"
        local_reducer_path_on_emr = "skewness_stats_reducer.py"
        hdfs_output_path = f"/user/hadoop/epa_air_quality/results/gui_skewness_{selected_function.lower().replace(' ','_')}"
    
    elif selected_function == "Min-Max Normalization":
        items = ["1. Find Min-Max Values.", "2. Do Normalization"]
        item, ok = QInputDialog.getItem(window, "Stage Selection", 
                                    "Which stage of Min-Max Normalization do you want to run?", 
                                    items, 0, False)
        
        if ok and item:
            mr_script_source_s3_path = f"{S3_CODE_BUCKET}/min_max/"
            emr_mr_script_target_dir = "/home/hadoop/mr_scripts_for_gui/min_max"
            
            if "1." in item:  # İlk aşama: Min-Max bulma
                job_name = "GUI_MinMax_Find_Values"
                local_mapper_path_on_emr = "min_max_finder_mapper.py"
                local_reducer_path_on_emr = "min_max_finder_reducer.py"
                hdfs_output_path = "/user/hadoop/epa_air_quality/results/gui_minmax_values"
            else:  
                job_name = "GUI_MinMax_Normalize"
                local_mapper_path_on_emr = "min_max_normalizer_mapper.py"
                local_reducer_path_on_emr = ""  
                hdfs_output_path = "/user/hadoop/epa_air_quality/results/gui_normalized_data"
    elif selected_function == "Median":
        job_name = "GUI_Median_Analysis"
        mr_script_source_s3_path = f"{S3_CODE_BUCKET}/median/"
        emr_mr_script_target_dir = "/home/hadoop/mr_scripts_for_gui/median"
        local_mapper_path_on_emr = "median_histogram_mapper.py"
        local_reducer_path_on_emr = "median_histogram_reducer.py"
        hdfs_output_path = f"/user/hadoop/epa_air_quality/results/gui_median"
    elif selected_function == "Standard Deviation":
        job_name = "GUI_StdDev_Analysis"
        mr_script_source_s3_path = f"{S3_CODE_BUCKET}/stddev/"
        emr_mr_script_target_dir = "/home/hadoop/mr_scripts_for_gui/stddev"
        local_mapper_path_on_emr = "stddev_welford_mapper.py"
        local_reducer_path_on_emr = "stddev_welford_reducer.py"
        hdfs_output_path = f"/user/hadoop/epa_air_quality/results/gui_stddev"
    elif selected_function == "90th Percentile":
        job_name = "GUI_90th_Percentile_Analysis"
        mr_script_source_s3_path = f"{S3_CODE_BUCKET}/percentile/"
        emr_mr_script_target_dir = "/home/hadoop/mr_scripts_for_gui/percentile"
        local_mapper_path_on_emr = "percentile_90_mapper.py"
        local_reducer_path_on_emr = "percentile_90_reducer.py"
        hdfs_output_path = f"/user/hadoop/epa_air_quality/results/gui_percentile"
    else:
        QMessageBox.warning(window, "Selection Error", f"MapReduce function for '{selected_function}' is not defined yet.")
        log_message(window, f"ERROR: No MR function for '{selected_function}'.")
        window.btn_run.setEnabled(True)
        return

    if mr_script_source_s3_path:
        cmd_list_s3_files = f"aws s3 ls {mr_script_source_s3_path}"
        log_message(window, f"Checking files on S3: {mr_script_source_s3_path}")
        stdout_list, stderr_list = execute_remote_ssh_command(cmd_list_s3_files, window)
        
        if stdout_list:
            log_message(window, f"Files located on S3:\n{stdout_list}")
        else:
            log_message(window, f"WARNING: File not found or accessible in S3 path: {stderr_list}")
        cmd_prepare_mr_scripts = f"""
            mkdir -p {emr_mr_script_target_dir} && \\
            echo "Directory created: {emr_mr_script_target_dir}" && \\
            aws s3 cp {mr_script_source_s3_path} {emr_mr_script_target_dir}/ --recursive && \\
            echo "Files copied from S3" && \\
            ls -la {emr_mr_script_target_dir}/ && \\
            if [ -n "$(ls -A {emr_mr_script_target_dir}/*.py 2>/dev/null)" ]; then
                chmod +x {emr_mr_script_target_dir}/*.py && \\
                echo "Granted execution permission to Python files"
            else
                echo "WARNING: Python files not found"
            fi && \\
            echo 'MR scripts for {selected_function} were prepared on the EMR master node.'
        """
        log_message(window, f"Preparing MR scripts for {selected_function} to EMR master node...")
        stdout, stderr = execute_remote_ssh_command(cmd_prepare_mr_scripts, window)
        if stdout is None:
            log_message(window, f"ERROR: MR scripts could not be prepared for EMR.{stderr}")
            window.btn_run.setEnabled(True)
            return
    if show_performance_metrics:
        mr_prep_time = time.time() - mr_prep_start
        log_message(window, f"⏱️ MR script preparation time: {mr_prep_time:.2f} seconds")
    if show_performance_metrics:
        mapreduce_start = time.time()
    cmd_delete_hdfs_output_on_emr = f"hdfs dfs -rm -r {hdfs_output_path} 2>/dev/null || true"
    log_message(window, f"Legacy HDFS output directory '{hdfs_output_path}' is deleting (If it is available)...")
    stdout_del, stderr_del = execute_remote_ssh_command(cmd_delete_hdfs_output_on_emr, window)
    cmd_find_streaming_jar = "find /usr/lib/hadoop-mapreduce/ -name 'hadoop-streaming*.jar' | head -1"
    log_message(window, "Searching for Hadoop streaming JAR file...")
    stdout_jar, stderr_jar = execute_remote_ssh_command(cmd_find_streaming_jar, window)
    
    if stdout_jar and stdout_jar.strip():
        streaming_jar_path = stdout_jar.strip()
        log_message(window, f"Streaming JAR found: {streaming_jar_path}")
    else:
        log_message(window, "ERROR: Hadoop streaming JAR file not found!")
        window.btn_run.setEnabled(True)
        return
    hadoop_command_parts = [
        'hadoop', 'jar', streaming_jar_path,
        '-D', f'mapreduce.job.name={job_name}',
    ]
    if selected_function in ["Skewness", "Min-Max Normalization", "Median", "Standard Deviation", "90th Percentile"]:
        hadoop_command_parts.extend(['-D', 'mapreduce.job.reduces=1'])
    abs_mapper_on_emr = f"{emr_mr_script_target_dir}/{local_mapper_path_on_emr}"
    files_for_hadoop_cmd = [abs_mapper_on_emr]
    if local_reducer_path_on_emr and local_reducer_path_on_emr != "None":
        abs_reducer_on_emr = f"{emr_mr_script_target_dir}/{local_reducer_path_on_emr}"
        files_for_hadoop_cmd.append(abs_reducer_on_emr)
    if local_reducer_path_on_emr and local_reducer_path_on_emr != "":
        hadoop_command_parts.extend(['-reducer', f'./{local_reducer_path_on_emr}'])
    else:
        hadoop_command_parts.extend(['-D', 'mapreduce.job.reduces=0'])
    for file_path in files_for_hadoop_cmd:
        hadoop_command_parts.extend(['-file', file_path])
    if selected_function == "Min-Max Normalization" and "2." in item:
        minmax_result_path = "/user/hadoop/epa_air_quality/results/gui_minmax_values/part-00000"
        cmd_read_minmax = f"hdfs dfs -cat {minmax_result_path}"
        log_message(window, "Min-Max values ​​are read from the previous job...")
        minmax_output, minmax_stderr = execute_remote_ssh_command(cmd_read_minmax, window)
        if minmax_output is None:  
            log_message(window, f"ERROR: Could not read Min-Max values. Run stage 1 first. {minmax_stderr}")
            window.btn_run.setEnabled(True)
            return
        try:
            lines = minmax_output.strip().split('\n')
            global_min = None
            global_max = None
            for line in lines:
                if line.startswith('global_min'):
                    global_min = float(line.split('\t')[1])
                elif line.startswith('global_max'):
                    global_max = float(line.split('\t')[1])
            if global_min is None or global_max is None:
                log_message(window, "HATA: Min-Max değerleri parse edilemedi.")
                window.btn_run.setEnabled(True)
                return
            log_message(window, f"Dynamic Min-Max values: min={global_min}, max={global_max}")
        except Exception as parse_error:
            log_message(window, f"ERROR: Error while parsing Min-Max values: {parse_error}")
            window.btn_run.setEnabled(True)
            return
        mapper_command_with_params = f'./{local_mapper_path_on_emr} {global_min} {global_max}'
        hadoop_command_parts.extend(['-mapper', mapper_command_with_params])
    else:
        hadoop_command_parts.extend(['-mapper', f'./{local_mapper_path_on_emr}'])

    if local_reducer_path_on_emr and local_reducer_path_on_emr != "None":
        hadoop_command_parts.extend(['-reducer', f'./{local_reducer_path_on_emr}'])
    hadoop_command_parts.extend(['-input', hdfs_input_path])
    hadoop_command_parts.extend(['-output', hdfs_output_path])
    final_hadoop_command_on_emr = ' '.join(shlex.quote(c) for c in hadoop_command_parts)
    log_message(window, "Starting Hadoop streaming job on EMR...")
    stdout_mr, stderr_mr = execute_remote_ssh_command(final_hadoop_command_on_emr, window)
    if show_performance_metrics:
        mapreduce_time = time.time() - mapreduce_start
        log_message(window, f"⏱️ MapReduce process time: {mapreduce_time:.2f} saniye")
    application_id = None
    job_successful = False
    if stderr_mr is not None and "completed successfully" in stderr_mr.lower():
        job_successful = True
        for line in stderr_mr.splitlines():
            if "Submitted application" in line:
                try:
                    app_id_part = line.split("Submitted application")[1].strip()
                    if app_id_part:
                        application_id = app_id_part.split()[0]
                        log_message(window, f"Captured YARN App ID: {application_id}")
                        break
                except:
                    pass
        log_message(window, f"MapReduce job '{job_name}' completed successfully on EMR.")
    elif stdout_mr is None:
        log_message(window, f"ERROR: MapReduce job '{job_name}' failed to run on EMR. {stderr_mr}")
    else:
        log_message(window, f"ERROR: MapReduce job '{job_name}' terminated with error on EMR.")
    if job_successful:
        log_message(window, "Results are read from HDFS...")
        result_file_hdfs_path = f"{hdfs_output_path}/part-00000"
        cmd_read_results_on_emr = f"hdfs dfs -cat {result_file_hdfs_path}"
        results_content, stderr_read = execute_remote_ssh_command(cmd_read_results_on_emr, window)
        
        if results_content:
            log_message(window, "Results read sucessfully.")
            if show_performance_metrics and selected_category == "Performance Testing":
                analysis_end_time = time.time()
                total_duration = analysis_end_time - analysis_start_time
                dataset_text = window.combo_datasets.currentText()
                processed_records = 0
                if "1K" in dataset_text:
                    processed_records = 1000
                elif "5K" in dataset_text:
                    processed_records = 5000
                elif "10K" in dataset_text:
                    processed_records = 10000
                elif "50K" in dataset_text:
                    processed_records = 50000
                elif "100K" in dataset_text:
                    processed_records = 100000
                enhanced_results = results_content + "\n\n" + "="*60 + "\n"
                enhanced_results += f"🔬 PERFORMANCE ANALYSIS RESULTS\n"
                enhanced_results += f"📊 Total Execution Time: {total_duration:.2f} seconds\n"
                enhanced_results += f"📈 Dataset: {dataset_text}\n"
                if processed_records > 0:
                    enhanced_results += f"⚡ Processing Rate: {processed_records/total_duration:.0f} records/sec\n"
                enhanced_results += f"\nDetailed Timing Breakdown:\n"
                enhanced_results += f"   • MR Script Preparation: {mr_prep_time:.2f} seconds\n"
                enhanced_results += f"   • MapReduce Execution: {mapreduce_time:.2f} seconds\n"
                enhanced_results += "="*60
                
                log_message(window, f"⏱️ Performance Test completed: {total_duration:.2f} saniye")
                show_results(window, enhanced_results)
            else:
                log_message(window, "✅ Analysis completed sucessfully")
                show_results(window, results_content)
        else:
            log_message(window, f"ERROR: Failed to read results from HDFS. {stderr_read}")
            show_results(window, f"ERROR: Failed to read results from HDFS.\n{stderr_read}")
    else:
        show_results(window, "The results could not be read because the MapReduce job failed.")
    window.btn_run.setEnabled(True)
    log_message(window, "Analysis process completed")

def main():
    global app
    app = QApplication(sys.argv)
    main_window = QWidget()
    init_ui(main_window)
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
