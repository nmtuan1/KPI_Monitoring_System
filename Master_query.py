# run_all.py
import subprocess
import time

script1_path = "C:/../query_jira.py"
script2_path = "C:/../query_resolved.py" 
script3_path = "C:/../query_PR.py" 

# Chạy nền script 1
process1 = subprocess.Popen(["python", script1_path])
print(f"Running {script1_path} in background with PID: {process1.pid}")

# Chạy nền script 2
process2 = subprocess.Popen(["python", script2_path])
print(f"Running {script2_path} in background with PID: {process2.pid}")

# Chạy nền script 2
process3 = subprocess.Popen(["python", script3_path])
print(f"Running {script3_path} in background with PID: {process3.pid}")

# Giữ script master chạy để các script con tiếp tục chạy
while True:
    time.sleep(3600) # Kiểm tra mỗi giờ (hoặc lâu hơn) nếu cần