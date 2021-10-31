# This script needs to be run about early June, when GTA is awarded.
# See updates on https://www.socgtoday.com/events
from typing import List

from bs4 import BeautifulSoup
import pandas as pd
import requests

import models.cudata as cudata
from cu_catalog.models.util import words_match2

url = "https://www.socgtoday.com/masterrecipientlist"

instructors = cudata.load_instructors()

r = requests.get(url)
data = r.text

soup = BeautifulSoup(data, features="lxml")


def match_name(gta_name) -> List[str]:
    result = []
    for name in instructors.keys():
        if words_match2(name, gta_name):
            result.append(name)
    return result


for h3 in soup.select('h3'):
    line = h3.getText()
    if ',' not in line:
        continue
    line2, _ = line.split(',', 1)
    if '-' not in line:
        continue  # this makes it skip awards given to two profs, but those are old anyway
    year, instructor_name = line2.split('-', 1)
    year = int(year)
    instructor_name = instructor_name.strip()
    print(year, instructor_name)

    names = match_name(instructor_name)
    if len(names) > 1:
        print("WARN: matched many names:", names, "for:", instructor_name)
    if len(names) > 0:
        matched_name = names[0]
        instructors[matched_name]['gta'] = year

# save
df_json = pd.DataFrame(instructors.values())
cudata.store_instructors(df_json)
print("Done")
