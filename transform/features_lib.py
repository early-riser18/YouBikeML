import pandas as pd
import pyspark.sql
from typing import Union
from abc import ABC, abstractmethod
class DataTransformer(ABC):

    def __init__(self, exec_library):
        self._exec_library = exec_library

    @abstractmethod
    def in_pandas(self):
        pass

    @abstractmethod
    def in_pyspark(self):
        pass

    def run(self, *args, **kwargs) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        match self._exec_library:
            case "pandas":
                return self.in_pandas(*args, **kwargs)
            case "pyspark":
                return self.in_pyspark(*args, **kwargs)
            case _:
                raise ValueError(f"Unknown exec_library value: {self._exec_library}")


