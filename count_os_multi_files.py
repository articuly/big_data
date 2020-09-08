import time
import re
import os

# single processing
start_time = time.time()
count = {}


def reduce(res_count={}, count={}):
    for key, val in res_count:
        if count.get(key) is None:
            count[key] = val
        else:
            count[key] += val


for file in ['profile.json', 'profile1.json', 'profile2.json', 'profile3.json', 'profile4.json', 'profile5.json',
             'profile6.json', 'profile7.json']:
    file_data = open(os.path.join('data', file), 'r', encoding='utf-8').read()
    res = re.findall("\"os\":(.*?),", file_data)
    res_count = [(val, res.count(val)) for val in set(res)]
    print(res_count)
    reduce(res_count, count)

print(count)
print(time.time() - start_time)
