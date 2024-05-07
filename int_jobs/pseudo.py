import pandas as pd
import pseudopeople
import time
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
states_list = [
    'WISCONSIN', 'WYOMING', 'DISTRICT OF COLUMBIA']

columns = ['simulant_id', 'first_name', 'middle_initial', 'last_name', 'age', 'date_of_birth', 'street_name', 'city', 'state', 'zipcode', 'sex', 'race_ethnicity']

start_time = time.time()
for name in states_list:
    df = pseudopeople.generate_decennial_census(source = "/gscratch/stf/seunguk/recordlinkage/usa", config = my_config, verbose = True, state = name)
    df = df[columns]
    df.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{name}.csv", index=False)
finish_time = time.time()

diff = finish_time - start_time

with open("/gscratch/stf/seunguk/pseudo_df/usa/time.txt", "w") as f:
    f.write(str(diff))
# x = [1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000, 200,000,000]
# for size in x:

#     sample_size = round(size * 1.5)
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

#     dfA.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfA.csv", index=False)
#     dfB.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa/{size}_dfB.csv", index=False)