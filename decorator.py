import os
import pandas as pd
from atomic_writer import atomic_writer
import fastparquet


# instead of using mutex or spinlock, try different way to solve the problem.
def save_atomic(path_param="filepath"):
    def save_atomic_wrapper(func):
        def wrapper(*args, **kwargs):
            # sanitization check
            if path_param not in kwargs:
                raise KeyError(f"path parameter {path_param} not provided")

            file_path = kwargs[path_param]
            assert isinstance(file_path, str), "file path must be string"

            if os.path.isdir(file_path):
                raise IsADirectoryError(f"{file_path} is a directory")

            if os.path.exists(file_path):
                raise FileExistsError(f"{file_path} already exists")

            temp_file_path = file_path + ".tmp"
            if os.path.exists(temp_file_path):
                raise FileExistsError(f"{temp_file_path} already exists")

            # SOLUTION: create a temporary file
            # and rename it after it is completely written on disk
            kwargs[path_param] = temp_file_path
            func(*args, **kwargs)

            # then rename temp file
            os.rename(temp_file_path, file_path)
        return wrapper

    return save_atomic_wrapper


@save_atomic(path_param="filename")
def to_parquet_atomic(**kwargs):
    return fastparquet.write(**kwargs)


if __name__ == '__main__':
    # Implementation
    df_test = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    to_parquet_atomic(filename="test.parquet", data=df_test, open_with=atomic_writer)

# Q.E.D
