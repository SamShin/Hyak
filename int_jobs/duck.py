# from splink.duckdb.duckdb_linker import DuckDBLinker
# import splink.duckdb.duckdb_comparison_library as cl
# from splink.duckdb.blocking_rule_library import block_on

'''
apptainer shell --bind /gscratch:/gscratch /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif

'''
from splink.duckdb.linker import DuckDBLinker
from splink.duckdb.blocking_rule_library import block_on
import splink.duckdb.comparison_library as cl
import duckdb
import logging
import time
import pandas as pd
import datetime
# logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
# for log in logs:
#     logging.getLogger(log).setLevel(logging.ERROR)


columns = ["id", "first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",
    "comparisons": [
        # cl.levenshtein_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        # cl.levenshtein_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        # cl.levenshtein_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        # cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        # cl.levenshtein_at_thresholds(col_name="birth_year", distance_threshold_or_thresholds=1, include_exact_match_level=False),

        cl.jaro_at_thresholds("first_name", [0.9, 0.7], term_frequency_adjustments=True),
        cl.jaro_at_thresholds("last_name", [0.9, 0.7], term_frequency_adjustments=True),
        cl.jaro_at_thresholds("middle_name", [0.9]),
        cl.levenshtein_at_thresholds("birth_year", [1, 2]),
        cl.levenshtein_at_thresholds("res_street_address", 2),
    ],
    #Blocking used here
    "blocking_rules_to_generate_predictions": [
        {"blocking_rule":
            "l.zip_code = r.zip_code and l.first_name = r.first_name",
            "salting_partitions": 4},
        {"blocking_rule":
            "l.last_name = r.last_name and l.middle_name = r.middle_name",
            "salting_partitions": 4},
        {"blocking_rule":
            "l.res_street_address = r.res_street_address",
            "salting_partitions": 4},
    ],
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False,
    "max_iterations": 20,
    "em_convergence": 0.001,
}

con = duckdb.connect(":memory:")
con.execute("SET temp_directory='/gscratch/stf/seunguk/duck';")

# path = "/mmfs1/home/seunguk/apptainer/test_files/test_df/"
# lst = [2000]
path = "/gscratch/stf/seunguk/big_df/"
lst = [1_000_000]
for x in lst:
    print(x)
    dfA = pd.read_csv(path + str(x) + "_dfA.csv", header = 0)
    dfB = pd.read_csv(path + str(x) + "_dfB.csv", header = 0)

    train_time_start = time.time()

    linker = DuckDBLinker([dfA, dfB], settings, connection=con)

    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    training = ["l.first_name = r.first_name",
                "l.middle_name = r.middle_name",
                ]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)
    train_time_end = time.time()


    predict_time_start = time.time()

    predict = linker.predict(0.5) #NOTE: Has None as a dafault value, thus 0.95 was needed for any analysis | 0.5 was suggested by Robin
    predict_time_end = time.time()

    test_time_start = time.time()
    df_predict = predict.as_pandas_dataframe()
    test_time_end = time.time()

    pairs1 = linker.count_num_comparisons_from_blocking_rule(
        "l.zip_code = r.zip_code and l.first_name = r.first_name")
    pairs2 = linker.count_num_comparisons_from_blocking_rule(
        "l.last_name = r.last_name and l.middle_name = r.middle_name")
    pairs3 = linker.count_num_comparisons_from_blocking_rule(
        "l.res_street_address = r.res_street_address")

    false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
    true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
    false_negative = round(x / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    with open("/mmfs1/home/seunguk/int_jobs/duck.txt", "a") as f:
        f.writelines(
            "Sample Size: " + str(f"{x:_}") +
            "|Links Predicted: " + str(f"{len(df_predict):_}") +
            "|Total Time Taken: " + str(f"{round((test_time_end - train_time_start), 2):_}") +
            "|Estimate Parameters Time Taken: " + str(f"{round((train_time_end - train_time_start), 2):_}") +
            "|Predict Time Taken: " + str(f"{round((predict_time_end - predict_time_start),2):_}") +
            "|Dataframe Convergence Time Taken: " + str(f"{round((test_time_end - test_time_start), 2):_}") +
            "|Precision: " + str(round(precision, 4)) +
            "|Recall: " + str(round(recall, 4)) +
            "|Linkage Pairs: " + str(f"{pairs1:_}," + f" {pairs2:_}," + f" {pairs3:_}") +
            "|Salting Partition: " + str(4) +
            "|Finish Time: " + str(datetime.datetime.now()) +
            "\n"
        )
