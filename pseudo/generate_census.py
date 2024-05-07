import pandas as pd
import pseudopeople
import time
import os
'''
apptainer shell --bind /gscratch:/gscratch /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif
'''

my_config = {
    'decennial_census': {
        'column_noise': {
            'first_name': {
                'leave_blank': {
                    'cell_probability': 0.06
                },
                'use_nickname': {
                    'cell_probability': 0.06
                },
                'use_fake_name': {
                    'cell_probability': 0.06
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.06
                },
                'make_typos': {
                    'cell_probability': 0.06
                }
            },
            'middle_initial': {
                'leave_blank': {
                    'cell_probability': 0.1
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.1
                },
                'make_typos': {
                    'cell_probability': 0.1
                }
            },
            'last_name': {
                'leave_blank': {
                    'cell_probability': 0.075
                },
                'use_fake_name': {
                    'cell_probability': 0.075
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.075
                },
                'make_typos': {
                    'cell_probability': 0.075
                }
            },
            'age': {
                'leave_blank': {
                    'cell_probability': 0.075
                },
                'copy_from_household_member': {
                    'cell_probability': 0.075
                },
                'misreport_age': {
                    'cell_probability': 0.075
                },
                'make_typos': {
                    'cell_probability': 0.075
                }
            },
            'date_of_birth': {
                'leave_blank': {
                    'cell_probability': 0.05
                },
                'copy_from_household_member': {
                    'cell_probability': 0.05
                },
                'swap_month_and_day': {
                    'cell_probability': 0.05
                },
                'write_wrong_digits': {
                    'cell_probability': 0.05
                },
                'make_typos': {
                    'cell_probability': 0.05
                }
            },
            'street_name': {
                'leave_blank': {
                    'cell_probability': 0.1
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.1
                },
                'make_typos': {
                    'cell_probability': 0.1
                }
            },
            'city': {
                'leave_blank': {
                    'cell_probability': 0.1
                },
                'make_phonetic_errors': {
                    'cell_probability': 0.1
                },
                'make_typos': {
                    'cell_probability': 0.1
                }
            },
            'state': {
                'leave_blank': {
                    'cell_probability': 0.15
                },
                'choose_wrong_option': {
                    'cell_probability': 0.15
                }
            },
            'sex': {
                'leave_blank': {
                    'cell_probability': 0.15
                },
                'choose_wrong_option': {
                    'cell_probability': 0.15
                }
            },
            'zipcode': {
                'leave_blank': {
                    'cell_probability': 0.1
                },
                'write_wrong_zipcode_digits': {
                    'cell_probability': 0.1
                },
                'make_typos': {
                    'cell_probability': 0.1
                }
            },
            'race_ethnicity': {
                'leave_blank': {
                    'cell_probability': 0.15
                },
                'choose_wrong_option': {
                    'cell_probability': 0.15
                }
            }
        },
    },
}

states_list = [
    'ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII',
    'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN',
    'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK',
    'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA',
    'TENNESSEE', 'TEXAS', 'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING', 'DISTRICT OF COLUMBIA'
    ]

columns = ['simulant_id', 'first_name', 'middle_initial', 'last_name', 'age', 'date_of_birth', 'street_name', 'city', 'state', 'zipcode', 'sex', 'race_ethnicity']

for name in states_list:
    file_path = f"/gscratch/stf/seunguk/pseudo_df/usa_data/{name}.csv"
    if os.path.exists(file_path):
        continue

    print(name)
    start_time = time.time()
    df = pseudopeople.generate_decennial_census(source = "/gscratch/stf/seunguk/recordlinkage/usa", config = my_config, verbose = True, state = name, engine="dask")
    df = df[columns]
    df_len = len(df)
    df.to_csv(f"/gscratch/stf/seunguk/pseudo_df/usa_data/{name}.csv", index=False)
    finish_time = time.time()

    diff = finish_time - start_time

    with open("/gscratch/stf/seunguk/pseudo_df/time.txt", "a") as f:
        f.write(f"{name}: {df_len} lines | {diff:.2f} seconds\n")
