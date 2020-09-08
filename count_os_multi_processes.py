import re
import time
import os
from concurrent.futures import ProcessPoolExecutor, wait

# multiprocess reading multi files
# reduce by the func
start_time = time.time()
count = {}


def map_os(file):
    # print(file)
    file_data = open(os.path.join('data', file), 'r', encoding='utf-8').read()
    res = re.findall("\"os\":(.*?),", file_data)
    res_count = [(val, res.count(val)) for val in set(res)]
    print(res_count)
    return res_count


def reduce(data):
    for key, val in data.result():
        if count.get(key) is None:
            count[key] = val
        else:
            count[key] += val


if __name__ == '__main__':
    pool = ProcessPoolExecutor(max_workers=8)
    p_list = []
    for file in ['profile.json', 'profile1.json', 'profile2.json', 'profile3.json', 'profile4.json', 'profile5.json',
                 'profile6.json', 'profile7.json']:
        p_list.append(pool.submit(map_os, file))
        p_list[-1].add_done_callback(reduce)

    wait(p_list)
    print(count)
    print(time.time() - start_time)
