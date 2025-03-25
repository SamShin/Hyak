from pseudopeople import generate_decennial_census
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

columns = ['simulant_id', 'first_name', 'middle_initial', 'last_name', 'age', 'date_of_birth', 'street_name', 'city', 'state', 'zipcode', 'sex', 'race_ethnicity']

df = generate_decennial_census(verbose = True, seed = 1)
df = df[columns]
df.to_csv("test.csv", index=False)

# df = generate_decennial_census(config = my_config, verbose = True, seed = 2)
# df = df[columns]
# df.to_csv("seed2.csv", index=False)

# df = generate_decennial_census(config = my_config, verbose = True, seed = 3)
# df = df[columns]
# df.to_csv("seed3.csv", index=False)

# df = generate_decennial_census(config = my_config, verbose = True, seed = 100)
# df = df[columns]
# df.to_csv("seed4.csv", index=False)

# df = generate_decennial_census(config = my_config, verbose = True, seed = 100)
# df = df[columns]
# df.to_csv("seed5.csv", index=False)