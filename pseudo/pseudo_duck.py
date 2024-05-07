from splink.duckdb.linker import DuckDBLinker
from splink.duckdb.blocking_rule_library import block_on
import splink.duckdb.comparison_library as cl
from duckdb import connect
import time
from pandas import read_csv
# import pandas as pd
import datetime
# import pseudopeople as pp
from os import remove

'''
apptainer shell --bind /gscratch:/gscratch /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif

'''

my_config = {
    'decennial_census': {
        'column_noise': {
            'first_name': {
                'leave_blank': {
                    'cell_probability': 0.02
                },
                'use_nickname': {
                    'cell_probability': 0.02
                },
                'use_fake_name': {
                    'cell_probability': 0.02
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.02
                },
                'make_typos': {
                    'cell_probability': 0.02
                }
            },
            'middle_initial': {
                'leave_blank': {
                    'cell_probability': 0.033
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.033
                },
                'make_typos': {
                    'cell_probability': 0.033
                }
            },
            'last_name': {
                'leave_blank': {
                    'cell_probability': 0.025
                },
                'use_fake_name': {
                    'cell_probability': 0.025
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.025
                },
                'make_typos': {
                    'cell_probability': 0.025
                }
            },
            'age': {
                'leave_blank': {
                    'cell_probability': 0.025
                },
                'copy_from_household_member': {
                    'cell_probability': 0.025
                },
                'misreport_age': {
                    'cell_probability': 0.025
                },
                'make_typos': {
                    'cell_probability': 0.025
                }
            },
            'date_of_birth': {
                'leave_blank': {
                    'cell_probability': 0.02
                },
                'copy_from_household_member': {
                    'cell_probability': 0.02
                },
                'swap_month_and_day': {
                    'cell_probability': 0.02
                },
                'write_wrong_digits': {
                    'cell_probability': 0.02
                },
                'make_typos': {
                    'cell_probability': 0.02
                }
            },
            'street_name': {
                'leave_blank': {
                    'cell_probability': 0.033
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.033
                },
                'make_typos': {
                    'cell_probability': 0.033
                }
            },
            'city': {
                'leave_blank': {
                    'cell_probability': 0.033
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.033
                },
                'make_typos': {
                    'cell_probability': 0.033
                }
            },
            'state': {
                'leave_blank': {
                    'cell_probability': 0.05
                },
                'choose_wrong_option': {
                    'cell_probability': 0.05
                }
            },
            'sex': {
                'leave_blank': {
                    'cell_probability': 0.05
                },
                'choose_wrong_option': {
                    'cell_probability': 0.05
                }
            },
            'race_ethnicity': {
                'leave_blank': {
                    'cell_probability': 0.05
                },
                'choose_wrong_option': {
                    'cell_probability': 0.05
                }
            }
        },
    },
}

# columns = ['simulant_id', 'first_name', 'middle_initial', 'last_name', 'age', 'date_of_birth', 'street_name', 'city', 'state', 'zip_code', 'sex', 'race_ethnicity']


br_conditions = [
    ['first_name', 'middle_initial'],
    ['middle_initial', 'last_name'],
    ['last_name', 'age'],
    ['age', 'date_of_birth'],
    ['date_of_birth', 'street_name'],
    ['street_name', 'city'],
    ['city', 'state'],
    # ['state', 'zip_code'],
    # ['zip_code', 'sex'],
    ['sex', 'race_ethnicity'],
    ['first_name', 'race_ethnicity']
]

brs = [block_on(c) for c in br_conditions]

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "simulant_id",
    # "unique_id_column_name": "id",
    "comparisons": [
        cl.jaro_at_thresholds("first_name", [0.9, 0.7], term_frequency_adjustments=True),
        cl.jaro_at_thresholds("last_name", [0.9, 0.7], term_frequency_adjustments=True),
        cl.levenshtein_at_thresholds("date_of_birth", [1, 2], term_frequency_adjustments=True),
        cl.levenshtein_at_thresholds("street_name", 2),
        cl.exact_match("middle_initial"),
        cl.exact_match("sex")
    ],
    "blocking_rules_to_generate_predictions": brs,
        # {"blocking_rule":
        #     "l.middle_initial = r.middle_initial and l.first_name = r.first_name",
        #     "salting_partitions": 4},
        # {"blocking_rule":
        #     "l.last_name = r.last_name and l.age = r.age",
        #     "salting_partitions": 4},
        # {"blocking_rule":
        #     "l.street_name = r.street_name",
        #     "salting_partitions": 4},
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False,
    "max_iterations": 20,
    "em_convergence": 0.001,
}


# con.execute("SET temp_directory='/gscratch/stf/seunguk/duck';")

# df = pp.generate_decennial_census(source = "/gscratch/stf/seunguk/recordlinkage/state", config = my_config)
# df = df[columns]
# x = [10_000, 20_000, 30_000, 40_000, 50_000, 75_000, 100_000, 125_000, 150_000, 200_000, 250_000, 300_000, 400_000, 500_000]

# x = [1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 75_000_000]
x = [10_000]
for size in x:
    print(size)
    # path = "/gscratch/stf/seunguk/pseudo_df/usa/"
    path = "/gscratch/stf/seunguk/pseudo_df/state/"
    # path = "/mmfs1/home/seunguk/apptainer/test_files/test_df/"
    dfA = read_csv(path + str(size) + "_dfA.csv", header = 0)
    dfB = read_csv(path + str(size) + "_dfB.csv", header = 0)

    testA= "/gscratch/stf/seunguk/l.db"
    testB = "/gscratch/stf/seunguk/r.db"
    train_time_start = time.time()

    # con = connect(database="/gscratch/stf/seunguk/duck/duck.db")

    linker = DuckDBLinker([testA, testB], settings)#, connection=con)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    training = [
        block_on(["first_name", "middle_initial", "last_name"]),
        block_on(["street_name", "city", "state"]),
        block_on(["age", "sex", "first_name"])]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i, estimate_without_term_frequencies=True) # NOTE: Set to False

    train_time_end = time.time()

    predict_time_start = time.time()

    print("Starting prediction")
    predict = linker.predict(0.5) #NOTE: Has None as a dafault value, thus 0.95 was needed for any analysis | 0.5 was suggested by Robin
    predict_time_end = time.time()

    df_predict = predict.as_pandas_dataframe()

    # pairs1 = linker.count_num_comparisons_from_blocking_rule(block_on(["last_name", "city"]))
    # pairs2 = linker.count_num_comparisons_from_blocking_rule(block_on(["first_name", "last_name"]))
    # pairs3 = linker.count_num_comparisons_from_blocking_rule(block_on(["first_name", "middle_initial"]))
    # pairs4 = linker.count_num_comparisons_from_blocking_rule(block_on(["middle_initial", "last_name"]))
    # pairs5 = linker.count_num_comparisons_from_blocking_rule(block_on(["first_name", "date_of_birth"]))

    # false_positive = len(df_predict.loc[df_predict["simulant_id_l"] != df_predict["simulant_id_r"]])
    # true_positive = len(df_predict.loc[df_predict["simulant_id_l"] == df_predict["simulant_id_r"]])
    false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
    true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
    false_negative = round(size / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    #remove("/gscratch/stf/seunguk/duck/duck.db")

    with open("/mmfs1/home/seunguk/pseudo/duck.txt", "a") as f:
        f.writelines(
            "Sample Size: " + str(f"{size:_}") +
            "|Links Predicted: " + str(f"{len(df_predict):_}") +
            "|Total Time Taken: " + str(f"{round((predict_time_end - train_time_start), 2):_}") +
            "|Estimate Parameters Time Taken: " + str(f"{round((train_time_end - train_time_start), 2):_}") +
            "|Predict Time Taken: " + str(f"{round((predict_time_end - predict_time_start),2):_}") +
            "|Precision: " + str(round(precision, 4)) +
            "|Recall: " + str(round(recall, 4)) +
            # "|Linkage Pairs: " + str(f"{pairs1:_}," + f" {pairs2:_}," + f" {pairs3:_}" + f" {pairs4:_}" + f" {pairs5:_}") +
            "|Finish Time: " + str(datetime.datetime.now()) +
            "\n"
        )