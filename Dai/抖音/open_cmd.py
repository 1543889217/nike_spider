import subprocess, psutil
import time


proc = subprocess.Popen('mitmdump -s .\get_json.py', creationflags=subprocess.CREATE_NEW_CONSOLE)
# proc = subprocess.Popen('mitmdump --mode upstream:http://58.218.201.122:9093 -s ./get_json.py', creationflags=subprocess.CREATE_NEW_CONSOLE)
time.sleep(28800)
pobj = psutil.Process(proc.pid)
pobj.kill()


