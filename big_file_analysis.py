import sys
import re
import time

f = open("data/bbs.71114.com.access.log", "r")
data = f.read()
print(sys.getsizeof(data))

start_time = time.time()
res = re.findall("(\d+\.\d+\.\d+\.\d+\s)", data)
res_count = [(val, res.count(val)) for val in set(res)]
print(res_count)
print(time.time() - start_time)

# 共2158s