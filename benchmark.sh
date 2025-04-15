#!/bin/bash

# Usage: ./run_benchmark.sh input.fastq

# Check if input FASTQ file is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 input.fastq"
    exit 1
fi

INPUT_FASTQ=$1
BASENAME=$(basename "$INPUT_FASTQ" .fastq)
SPLIT_PREFIX="${BASENAME}_part"

# 1. Split the FASTQ file into 3 parts (each read is 4 lines)
echo "[INFO] Splitting $INPUT_FASTQ into 3 parts..."
total_lines=$(wc -l < "$INPUT_FASTQ")
lines_per_file=$(( (total_lines + 3) / 3 / 4 * 4 ))  # Round up to multiple of 4

split -d -l "$lines_per_file" "$INPUT_FASTQ" "${SPLIT_PREFIX}"

# Rename split files to .fastq
for i in 00 01 02; do
    mv "${SPLIT_PREFIX}${i}" "${SPLIT_PREFIX}${i}.fastq"
done

echo "[INFO] Split complete: ${SPLIT_PREFIX}00.fastq, ${SPLIT_PREFIX}01.fastq, ${SPLIT_PREFIX}02.fastq"

# 2. Create HTCondor submit file
echo "[INFO] Creating HTCondor submit file..."

cat <<EOF > bwa_benchmark.sub
universe = vanilla
executable = /usr/bin/python3
arguments = bwa_alt.py \$(input_file) hg19.fa
should_transfer_files = NO
log = bwa_\$(input_file).log
output = bwa_\$(input_file).out
error = bwa_\$(input_file).err
request_cpus = 1
queue input_file from (
${SPLIT_PREFIX}00.fastq
${SPLIT_PREFIX}01.fastq
${SPLIT_PREFIX}02.fastq
)
EOF

# 3. Submit jobs to HTCondor
echo "[INFO] Submitting jobs to HTCondor..."
condor_submit bwa_benchmark.sub

