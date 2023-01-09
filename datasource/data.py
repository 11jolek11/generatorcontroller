import os
import pandas as pd
from abc import ABC, abstractmethod


class Data:
    def __init__(self, path) -> None:
        if os.path.exists(path):
            self.s_content = None
            self._path = path
        else:
            raise FileNotFoundError("Cant find file " + str(path))

        self._name = os.path.split(self._path)[0]
        self._type = os.path.split(self._path)[1]

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def content(self):
        return self.s_content

    @content.setter
    def content(self, new_content):
        self.s_content = new_content

    @content.getter
    def content(self):
        return self.s_content


class DataCSV(Data):
    def __init__(self, path) -> None:
        super().__init__(path)
        self.s_content = pd.DataFrame
        self.content = self._path

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def content(self):
        return self.s_content

    @content.setter
    def content(self, path):
        self.s_content = pd.read_csv(path)

    def sample(self, n: int = 5):
        print(self.s_content.sample(n=n))

    def expose(self):
        return list(self.s_content.to_dict().keys()), self.s_content.to_dict()


if __name__ == "__main__":
    data = DataCSV("./datasets/sea_level.csv")
    labels, holder = data.expose()
