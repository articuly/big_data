import sys
import re
import time
from concurrent.futures import ProcessPoolExecutor, wait

f = open(r'data\bbs.71114.com.access.log', 'r')
data_lines = f.readlines()
print(sys.getsizeof(data_lines))
count = {}


def map_os(data):
    res = re.findall("(\d+\.\d+\.\d+\.\d+\s)", data)
    res_count = {(val, res.count(val)) for val in set(res)}
    return res_count


def reduce(data):
    for key, val in data.result():
        if count.get(key) is None:
            count[key] = val
        else:
            count[key] += val


if __name__ == "__main__":
    pool = ProcessPoolExecutor(max_workers=2)
    start_time = time.time()
    cut_len = round(len(data_lines) / 64)
    p_list = []
    while True:
        current_data, data_lines = data_lines[:cut_len], data_lines[cut_len:]
        p_list.append(pool.submit(map_os, '\n'.join(current_data)))
        p_list[-1].add_done_callback(reduce)
        if not data_lines:
            break
    wait(p_list)
    print(count)
    print(time.time() - start_time)

# 8线程49s
# 4线程54s
# 2线程81