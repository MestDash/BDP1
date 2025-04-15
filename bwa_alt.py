import concurrent.futures
import subprocess
import os
import time
import sys


def split_fastq(fastq_file, batch_size):
    batches = []
    batch_number = 1
    with open(fastq_file, 'r') as f:
        while True:
            batch_file = f'batch_{batch_number:03}.fastq'
            with open(batch_file, 'w') as batch_f:
                lines_written = 0
                for _ in range(batch_size):
                    line = f.readline()
                    if not line:
                        break
                    batch_f.write(line)
                    for _ in range(3):
                        batch_f.write(f.readline())
                    lines_written += 4

            if lines_written > 0:
                batches.append(batch_file)
                batch_number += 1
            else:
                os.remove(batch_file)
                break
    return batches


def align_batch(batch_file, reference_genome, threads):
    output_file = batch_file.replace('.fastq', '.sam')
    bwa_command = [
        'bwa', 'mem', '-t', str(threads), reference_genome, batch_file
    ]
    with open(output_file, 'w') as out_f:
        subprocess.run(bwa_command, stdout=out_f)


def clean_up(batches):
    for batch in batches:
        os.remove(batch)
    for sam_file in os.listdir():
        if sam_file.endswith(".sam"):
            os.remove(sam_file)


def process_fastq(fastq_file, reference_genome, mode):
    batch_sizes = [32000, 128000, 256000, 512000]
    all_execution_times = []

    for batch_size in batch_sizes:
        print(f"\n[{mode}] Processing batches of {batch_size} reads.")
        batches = split_fastq(fastq_file, batch_size)

        for i in range(5):
            print(f"[{mode}] Run {i + 1} starting...")
            start_time = time.time()

            if mode == "parallel_single_thread":
                with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
                    futures = [executor.submit(align_batch, batch, reference_genome, 1) for batch in batches]
                    concurrent.futures.wait(futures)

            elif mode == "single_parallel_thread":
                for batch in batches:
                    align_batch(batch, reference_genome, 2)

            end_time = time.time()
            total_time = end_time - start_time
            all_execution_times.append((mode, batch_size, total_time))
            print(f"[{mode}] Run {i + 1} complete in {total_time:.2f} seconds.")

        clean_up(batches)

    return all_execution_times


def log_execution_time(all_times):
    with open('execution_times.txt', 'w') as f:
        for i, (mode, batch_size, time_taken) in enumerate(all_times, 1):
            f.write(f"{mode}, Batch Size {batch_size}, Run {i % 5 or 5}: {time_taken:.2f} seconds\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bwa_mem_align.py <fastq_file> <reference_genome>")
        sys.exit(1)

    fastq_file = sys.argv[1]
    reference_genome = sys.argv[2]

    results_1 = process_fastq(fastq_file, reference_genome, "parallel_single_thread")
    results_2 = process_fastq(fastq_file, reference_genome, "single_parallel_thread")

    all_results = results_1 + results_2
    log_execution_time(all_results)
    print(f"\nAll execution times logged in 'execution_times.txt'.")
