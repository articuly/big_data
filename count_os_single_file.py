import time
import re

# read single file
start_time = time.time()
file_data = open('data/profile.json', 'r', encoding='utf-8').read()
res = re.findall("\"os\":(.*?),", file_data)
res_count = [(val, res.count(val)) for val in set(res)]
print(res_count)
print(time.time() -start_time)
