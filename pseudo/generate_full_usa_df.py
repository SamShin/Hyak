import os
import pandas as pd

directory = "/gscratch/stf/seunguk/pseudo_df/usa"
csv_files = [file for file in os.listdir(directory) if file.endswith('csv')]

dfs = []

for file in csv_files:
    f_path = os.path.join(directory, file)
    df = pd.read_csv(f_path, low_memory=False)
    dfs.append(df)

merged_df = pd.concat(dfs)

merged_df.to_csv("full_usa.csv", index=False)