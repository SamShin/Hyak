import pandas as pd
file_path = "/gscratch/stf/seunguk/pseudo_df/usa_data/LOUISIANA.csv"
chunk_size = 10_000_000
total_size = 330_574_214

chunk_size = 10_000
total_L = 4_531_308
df_chunks = pd.read_csv(file_path, chunksize=chunk_size)
print(df_chunks.head(10))
num_chunks = total_size // chunk_size + (1 if total_size % chunk_size != 0 else 0)
# x = [1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 75_000_000, 100_000_000, 125_000_000, 150_000_000, 175_000_000, 200_000_000]
x = [1000, 2000, 5000, 10000, 25000]
for size in x:
    sampled_chunks = []
    sample_size = round(size * 1.5)

    for chunk in df_chunks:
        sampled_chunk = chunk.sample(n=sample_size // num_chunks, replace=False) # type: ignore
        sampled_chunks.append(sampled_chunk)

    sample_set = pd.concat(sampled_chunks, ignore_index=True)

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

# df = pd.read_csv("/gscratch/stf/seunguk/pseudo_df/full_usa.csv")
# sampled_and_split_dfs = []

# for size in x:
#     total_sampled = 0
#     print(f"{size} start")
#     sample_set = df.sample(sample_size)
#     cut = round(sample_size / 3)

#     dfA_first = sample_set[0:cut]
#     dfB_first = sample_set[0:cut]

#     dfA_last = sample_set[(cut):(2 * cut)]
#     dfB_last = sample_set.tail(cut)

#     frame_a = [dfA_first, dfA_last]
#     frame_b = [dfB_first, dfB_last]

#     dfA = pd.concat(frame_a)
#     dfB = pd.concat(frame_b)

#     print(f"{size} write")

#     dfA.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfA.csv", index=False)
#     dfB.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfB.csv", index=False)