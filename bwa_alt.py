import concurrent.futures

def process_fastq(fastq_file, reference_genome, threads_per_task, num_runs=10):
    batch_sizes = [32000, 128000, 256000, 512000]
    all_execution_times = []

    for batch_size in batch_sizes:
        print(f"Processing batches of {batch_size} reads.")
        batches = split_fastq(fastq_file, batch_size)

        for i in range(num_runs):
            print(f"Run {i + 1} starting...")
            start_time = time.time()

            # Use parallel processing with 2 workers, each running a single-threaded alignment
            with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(align_batch, batch, reference_genome, 1) for batch in batches]
                concurrent.futures.wait(futures)

            end_time = time.time()
            total_time = end_time - start_time
            all_execution_times.append(total_time)
            print(f"Run {i + 1} complete in {total_time:.2f} seconds.")

        for batch in batches:
            os.remove(batch)
        for sam_file in os.listdir():
            if sam_file.endswith(".sam"):
                os.remove(sam_file)

    return all_execution_times
