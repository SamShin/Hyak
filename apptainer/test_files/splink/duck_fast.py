from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd
# from  pathlib import Path
# import os
logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)

columns = ["id", "first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

blocking_rules_for_prediction = [
        "l.first_name = r.first_name and l.last_name = r.last_name",
        "l.first_name = r.first_name and l.middle_name = r.middle_name",
        "l.res_street_address = r.res_street_address",
        "l.birth_year = r.birth_year and l.middle_name = r.middle_name",
        "l.birth_year = r.birth_year and l.last_name = r.last_name",
        "l.birth_year = r.birth_year and l.first_name = r.first_name"
]

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",
    "comparisons": [
        cl.jaro_winkler_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=[1,3,5], term_frequency_adjustments=False),
        cl.levenshtein_at_thresholds(col_name="birth_year", distance_threshold_or_thresholds=1, term_frequency_adjustments=True)
    ],
    #Blocking used here
    "blocking_rules_to_generate_predictions": blocking_rules_for_prediction,
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False

}
path = "/mmfs1/home/seunguk/apptainer/test_files/test_df/"
lst = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]
for x in lst:
    print(x)
    dfA = pd.read_csv(path + str(x) + "_dfA.csv", header = 0)
    dfB = pd.read_csv(path + str(x) + "_dfB.csv", header = 0)

    time_start = time.time()

    linker = DuckDBLinker([dfA, dfB], settings)

    linker.estimate_probability_two_random_records_match(["l.first_name = r.first_name and l.last_name = r.last_name and l.birth_year = r.birth_year",
                                                          "l.birth_year = r.birth_year and l.res_street_address = r.res_street_address",
                                                          "l.first_name = r.first_name and l.middle_name = r.middle_name and l.last_name = r.last_name"], recall=0.8)
    linker.estimate_u_using_random_sampling(target_rows=1e6)

    training = [
        "l.first_name = r.first_name and l.last_name = r.last_name",
        "l.res_street_address = r.res_street_address"
    ]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)
    predict = linker.predict(0.5) # This should be 0.5 for your accuracy derivations to be correct

    time_end = time.time()

    df_predict = predict.as_pandas_dataframe()
    pairs_data = linker.cumulative_comparisons_from_blocking_rules_records(blocking_rules_for_prediction)
    pairs = sum([r['row_count'] for r in pairs_data])

    false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
    true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
    false_negative = round(x / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    with open("/mmfs1/home/seunguk/apptainer/test_files/splink/fast_duck.txt", "a") as f:
        f.writelines(
            "Sample Size: " + str(x) +
            "|Links Predicted: " + str(len(df_predict)) +
            "|Time Taken: " + str(round((time_end - time_start),2)) +
            "|Precision: " + str(precision) +
            "|Recall: " + str(recall) +
            "|Linkage Pairs: " + str(pairs) +
            "\n"
        )