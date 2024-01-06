import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
import json
import time
import subprocess
import re

def load_month_data(partition_files, month):
    max_year = ""
    max_year_avg = ""
    for file_path in partition_files:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                if month in data:
                    year_data = data[month]
                    latest_year = max(year_data, key=lambda x: datetime.strptime(x, "%Y"))
                    latest_year_avg = year_data[latest_year]["avg"]
                    if latest_year > max_year:
                        max_year = latest_year
                        max_year_avg = latest_year_avg
        except FileNotFoundError:
            continue

    return (max_year, max_year_avg)

mon_to_plot = ['January', 'February', 'March']
if os.path.exists("/files/month.svg"):
    print("Deleting the file.")
    os.remove("/files/month.svg")

partition_numbers = [0, 1, 2, 3]
partition_files = [f"/files/partition-{n}.json" for n in partition_numbers]

month_data = {month: load_month_data(partition_files, month) for month in mon_to_plot}
# print(month_data)
updated_data = {}
for month in month_data:
    updated_data[month+"-"+month_data[month][0]] = [month_data[month][1]]

# print(updated_data)
month_df = pd.DataFrame(updated_data)


fig, ax = plt.subplots()
month_df.mean().plot(kind='bar', ax=ax)
ax.set_ylabel('Avg. Max Temperature')
ax.set_title('Month Averages')
plt.tight_layout()

plt.savefig("/files/month.svg")

# time.sleep(1)
# plt_output = str(subprocess.check_output("cat /files/month.svg", shell=True))
# old_years = dict()
# patterns = [r'January-\d{4}', r'February-\d{4}', r'March-\d{4}']
# for pattern in patterns:
#     matches = re.findall(pattern, plt_output)
#     print(matches)
#     if len(matches) == 0:
#         print(f"Couldn't find pattern {pattern} in month.svg")
#     old_years[matches[0].split("-")[0]] = int(matches[0].split("-")[1]) 
# print(old_years)



