import subprocess, psutil
import time


# proc = subprocess.Popen('mitmweb -p 12345 --web-port 8088 -s .\get_json_info.py', creationflags=subprocess.CREATE_NEW_CONSOLE)
proc = subprocess.Popen('mitmdump -p 12345 -s .\get_json_info.py', creationflags=subprocess.CREATE_NEW_CONSOLE)


# time.sleep(28000)
pobj = psutil.Process(proc.pid)
# pobj.kill()


