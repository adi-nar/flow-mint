import polars as pl
from abc import ABC, abstractmethod

class Parser(ABC):
    def __init__(self, bank_name: str):
        self.bank_name = bank_name

    @abstractmethod
    def parse(self, file_path: str) -> pl.DataFrame:
        pass
