import os
import subprocess as s


result = s.run("rm -rf /mmfs1/home/seunguk/spark/logs/*", shell=True, capture_output=True, text=True, check=True)
print(f"Logs stderr: {result.stderr}")
print(f"Logs stdout: {result.stdout}\n")

result = s.run("rm -rf /mmfs1/home/seunguk/spark/temp/*", shell=True, capture_output=True, text=True, check=True)
print(f"Temp stderr: {result.stderr}")
print(f"Temp stdout: {result.stdout}\n")

result = s.run("rm -rf /mmfs1/home/seunguk/spark/work/*", shell=True, capture_output=True, text=True, check=True)
print(f"Work stderr: {result.stderr}")
print(f"Work stdout: {result.stdout}")



result = s.run("rm -f /mmfs1/home/seunguk/jobs/error.txt", shell=True, capture_output=True, text=True, check=True)
print(f"Logs stderr: {result.stderr}")
print(f"Logs stdout: {result.stdout}\n")

result = s.run("rm -f /mmfs1/home/seunguk/jobs/output.txt", shell=True, capture_output=True, text=True, check=True)
print(f"Logs stderr: {result.stderr}")
print(f"Logs stdout: {result.stdout}\n")

result = s.run("rm -f /mmfs1/home/seunguk/jobs/spark_nodes.txt", shell=True, capture_output=True, text=True, check=True)
print(f"Logs stderr: {result.stderr}")
print(f"Logs stdout: {result.stdout}\n")