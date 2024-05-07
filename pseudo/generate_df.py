import pandas as pd

file_path = "/gscratch/stf/seunguk/pseudo_df/full_usa.csv"
size_needed = 150_000_000
df = pd.read_csv(file_path, nrows=size_needed)
# df = df[columns]
x = [1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 75_000_000, 100_000_000]
for size in x:
    sample_size = round(size * 1.5)
    sample_set = df.sample(sample_size)
    cut = round(sample_size / 3)

    dfA_first = sample_set[0:cut]
    dfB_first = sample_set[0:cut]

    dfA_last = sample_set[(cut):(2 * cut)]
    dfB_last = sample_set.tail(cut)

    frame_a = [dfA_first, dfA_last]
    frame_b = [dfB_first, dfB_last]
    dfA = pd.concat(frame_a)
    dfB = pd.concat(frame_b)

    dfA.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfA.csv", index=False)
    dfB.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfB.csv", index=False)