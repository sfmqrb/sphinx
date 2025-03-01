import csv

def compute_weighted_average(csv_file, latency_column):
    """
    Compute the weighted average latency for the given latency_column.
    Each row’s latency applies to the new entries (difference from previous row).
    The final average is the sum of (new_entries * latency) divided by total entries.
    """
    total_weighted_latency = 0.0
    previous_entries = 0
    rows = []

    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    if not rows:
        return 0
    
    # Total number of entries is taken from the last row.
    total_entries = int(float(rows[-1]['num_entries']))
    
    for row in rows:
        # Convert num_entries from scientific notation by first converting to float then to int.
        current_entries = int(float(row['num_entries']))
        diff = current_entries - previous_entries
        # Convert the latency to float (it may be a string in scientific notation)
        latency = float(row[latency_column])
        total_weighted_latency += diff * latency
        previous_entries = current_entries

    return total_weighted_latency / total_entries

def compute_simple_average(csv_file, latency_column):
    """
    Compute the simple (arithmetic) average latency for the given latency_column.
    This is simply the sum of all latency values divided by the number of rows.
    """
    total_latency = 0.0
    count = 0

    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_latency += float(row[latency_column])
            count += 1

    return total_latency / count if count > 0 else 0

if __name__ == "__main__":
    csv_filename = "benchmark_Fleck_ExtraBits1.csv"  # change this to your CSV file path

    # Compute weighted averages:
    avg_write_weighted = compute_weighted_average(csv_filename, "insertion_time")
    avg_read_weighted  = compute_weighted_average(csv_filename, "query_time")

    # Compute simple averages:
    avg_write_simple = compute_simple_average(csv_filename, "insertion_time")
    avg_read_simple  = compute_simple_average(csv_filename, "query_time")
    
    print("Weighted average write latency (insertion_time):", avg_write_weighted)
    print("Weighted average read latency (query_time):", avg_read_weighted)
    print("Simple average write latency (insertion_time):", avg_write_simple)
    print("Simple average read latency (query_time):", avg_read_simple)

