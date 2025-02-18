import random
from dask.distributed import Client

def inside_circle(num_samples: int) -> int:
    """
    Returns how many random points (out of num_samples) fall inside 
    a quarter of a unit circle (radius = 1).
    """
    count = 0
    for _ in range(num_samples):
        x = random.random()  # random number in [0, 1)
        y = random.random()  # random number in [0, 1)
        if x*x + y*y <= 1:
            count += 1
    return count

def estimate_pi(total_points: int, chunks: int = 8) -> float:
    """
    Estimate π by distributing 'inside_circle' computations across
    Dask workers.
    
    :param total_points:  total number of random points to sample
    :param chunks:        how many chunks to split the total_points into
    :return:              approximate value of π
    """
    # Connect to the Dask scheduler
    # (By default, tries to connect to localhost:8786; 
    #  you can specify a different address if needed.)
    client = Client(address="tcp://127.0.0.1:8786")
    print("Connected to Dask scheduler:", client)

    # Points per chunk
    points_per_chunk = total_points // chunks

    # Submit tasks to the Dask cluster
    futures = []
    for i in range(chunks):
        future = client.submit(inside_circle, points_per_chunk)
        futures.append(future)

    # Gather results (block until all tasks complete)
    counts = client.gather(futures)
    total_in_circle = sum(counts)

    # The fraction of points in the quarter circle is:
    #    (total_in_circle / total_points)
    # The area of a quarter circle of radius 1 is π/4. 
    # So: π/4 ~= in_circle_fraction => π ~= 4 * in_circle_fraction
    pi_estimate = 4.0 * (total_in_circle / float(total_points))

    return pi_estimate


if __name__ == "__main__":
    # Example usage:
    # Estimate π using 1,000,000 points total, split into 8 chunks
    TOTAL_POINTS = 1_000_000
    CHUNKS = 8

    pi_approx = estimate_pi(TOTAL_POINTS, CHUNKS)
    print(f"Estimated π with {TOTAL_POINTS} points: {pi_approx}")

