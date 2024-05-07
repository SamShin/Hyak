#!/bin/bash

#SBATCH --job-name=pseudo

#SBATCH --account=stf
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

#SBATCH --time=00-24:00:00 # Max runtime in DD-HH:MM:SS format.

#SBATCH --chdir=/mmfs1/home/seunguk/slurm
#SBATCH --export=all
#SBATCH --output=output.txt
#SBATCH --error=error.txt

module load apptainer

# apptainer exec --bind /mmfs1/home/seunguk/spark/work:/spark/spark-3.4.0-bin-hadoop3/work,/mmfs1/home/seunguk/spark/conf:/spark/spark-3.4.0-bin-hadoop3/conf /mmfs1/home/seunguk/apptainer/python.sif bash -c "python3 main.py; sleep 5"
apptainer exec --bind /gscratch:/gscratch /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif bash -c "python3 /mmfs1/home/seunguk/pseudo/generate_census.py >> output.log"
# apptainer exec /mmfs1/home/seunguk/apptainer/python.sif bash -c "/spark/spark-3.4.0-bin-hadoop3/bin/spark-submit /mmfs1/home/seunguk/apptainer/test_files/splink/spark.py"
