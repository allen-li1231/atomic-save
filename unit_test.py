import unittest
import os
import pandas as pd
from atomic_writer import atomic_writer
from decorator import to_parquet_atomic


class TestAtomicSave(unittest.TestCase):
    def test_dataframe(self):
        """
        Test that any dataframe can be saved
        """

        # create a dataframe and atomic save it
        df_test = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        to_parquet_atomic(filename="test1.parquet", data=df_test, open_with=atomic_writer)

        self.assertTrue(os.path.exists("test1.parquet"))
        # delete it afterward
        os.unlink("test1.parquet")


if __name__ == '__main__':
    unittest.main()
