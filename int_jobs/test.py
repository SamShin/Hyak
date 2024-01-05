import time
original_file_line = None
test_case_count = 4


with open("/mmfs1/home/seunguk/int_jobs/spark.txt", "r") as f:
    original_file_line = sum(1 for line in f)


def check_file():
    with open("/mmfs1/home/seunguk/int_jobs/spark.txt", "r") as file:
        line_count = sum(1 for line in file)


        return line_count == original_file_line + test_case_count

while not check_file():
    time.sleep(1)
print('done')