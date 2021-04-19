#!/bin/sh

#SBATCH -J dr_features
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --output=/home/apps/astro/alercebroker/logs/dr_features_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/dr_features_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mem-per-cpu=5432

ml Python/3.7.2
source ~/venvs/env/bin/activate

input_bucket=$1
output_bucket=$2

task_partition=${SLURM_ARRAY_TASK_ID}
total_partitions=${SLURM_ARRAY_TASK_MAX}

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1

dr compute-features $input_bucket s$output_bucket $task_partition -t $total_partition