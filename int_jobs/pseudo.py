# from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl
# import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd
import pseudopeople as pp


# pseudopeople config
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
            'zipcode': {
                'leave_blank': {
                    'cell_probability': 0.033
                },
                'write_wrong_zipcode_digits': {
                    'cell_probability': 0.033
                },
                'make_typos': {
                    'cell_probability': 0.033
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

# Splink config
settings = {
    "link_type": "link_only",
    "unique_id_column_name": "simulant_id",
    "comparisons": [
        cl.levenshtein_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        # cl.levenshtein_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="street_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="date_of_birth", distance_threshold_or_thresholds=1, include_exact_match_level=False)
    ],
    #Blocking used here

    # Zip code is not working
    # "blocking_rules_to_generate_predictions": [
    #    "l.zipcode = r.zipcode"
    # ]
}

columns = ['simulant_id', 'first_name', 'middle_initial', 'last_name', 'age', 'date_of_birth', 'street_name', 'city', 'state', 'zipcode', 'sex', 'race_ethnicity']

logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)


df = pp.generate_decennial_census(source = "/gscratch/stf/seunguk/state", config = my_config)
x = [1000,2000,3000,4000,5000,6000]
for size in x:

    # Generate a decennial census from pseudopeople

    # Select subset columns
    # df = df[columns]

    # Separate into dfA and dfB
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

    time_start = time.time() # <- Start the counter


    # Start linkage process
    linker = DuckDBLinker([dfA, dfB], settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    training = ["l.first_name = r.first_name",
                "l.last_name = r.last_name",
                "l.street_name = r.street_name",
                "l.date_of_birth = r.date_of_birth"
                ]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)
    predict = linker.predict(0.95) # Has None as a dafault value, thus 0.95 was needed for any analysis

    time_end = time.time() #<- Stop the counter


    # Calculate statistics
    df_predict = predict.as_pandas_dataframe()
    pairs = linker.count_num_comparisons_from_blocking_rule("l.zipcode = r.zipcode")

    false_positive = len(df_predict.loc[df_predict["simulant_id_l"] != df_predict["simulant_id_r"]])
    true_positive = len(df_predict.loc[df_predict["simulant_id_l"] == df_predict["simulant_id_r"]])
    false_negative = round(size / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    with open("test.txt", "a") as f:
        f.writelines(
            "Sample Size: " + str(size) +
            "|Links Predicted: " + str(len(df_predict)) +
            "|Time Taken: " + str(round((time_end - time_start),2)) +
            "|Precision: " + str(precision) +
            "|Recall: " + str(recall) +
            "|Linkage Pairs: " + str(pairs) +
            "\n"
        )