import pandas as pd

def csv_data_generator(data_path: str, chunk_size: int = 1000):
    """
    Load data from csv file and yield chunk of data.
    """

    reader = pd.read_csv(data_path, chunksize=chunk_size)
    idx = 0
    for chunk in reader:
        for record in chunk.to_dict(orient="records"):
            yield idx, record
            idx += 1