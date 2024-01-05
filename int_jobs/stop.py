import subprocess as sp

start_master_command = "/spark/spark-3.4.0-bin-hadoop3/sbin/stop-all.sh"
result = sp.run(start_master_command, shell=True, capture_output=True, text=True)
print("Clean STDOUT: ", result.stdout)
print("Clean STDERR: ", result.stderr)
print("\n")

