import logging
import time
import datetime
import sys
import pandas as pd

from splink.duckdb.linker import DuckDBLinker
from splink.duckdb.blocking_rule_library import block_on

settings = {"link_type": "link_only",
            "unique_id_column_name": "simulant_id",}

path = "/gscratch/stf/seunguk/pseudo_df/usa/"
path_big = "/gscratch/stf/seunguk/recordlinkage/big_df/"


block4 = block_on(["first_name", "middle_initial", "last_name", "street_name"])
block = block_on(["zipcode", "date_of_birth", "city", "state"])
block2 = block_on(["first_name", "middle_initial"])
# dfA = pd.read_csv(path + "1000000_dfA.csv")
# dfB = pd.read_csv(path + "1000000_dfB.csv")
total = [block, block4]
dfA = pd.read_csv(path + "2000000_dfA.csv")
dfB = pd.read_csv(path + "2000000_dfB.csv")

linker = DuckDBLinker([dfA, dfB], settings)
count = linker.count_num_comparisons_from_blocking_rule(block4)

print(count)
# 100,000 | "l.first_name = r.first_name" | 21,330,044 <FINISH>
# 100,000 | "substr(l.first_name,1,2) = substr(r.first_name,1,2)" | 149,448,781

# 1,000,000 | "substr(l.first_name,1,1) = substr(r.first_name,1,1) and substr(l.last_name,1,2) = substr(r.last_name,1,2) and l.zip_code = r.zip_code" | 390,449
# 1,000,000 | "l.first_name = r.first_name and l.last_name = r.last_name" | 1,885,134
# 1,000,000 | "l.first_name = r.first_name and substr(l.last_name,1,2) = substr(r.last_name,1,2)" | 21,816,866 <ERROR>
# 1,000,000 | "l.first_name = r.first_name and l.zip_code = r.zip_code" | 140,975,921
# 1,000,000 | "substr(l.first_name,1,1) = substr(r.first_name,1,1) and substr(l.last_name,1,2) = substr(r.last_name,1,2)" | 551,272,289
# 1,000,000 | "l.first_name = r.first_name" | 2,156,093,919
# 1,000,000 | "substr(l.first_name,1,2) = substr(r.first_name,1,2)" | 15,007,426,000
