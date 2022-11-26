import os
import pandas as pd
from abc import ABC, abstractmethod


class Data():
    def __init__(self, path) -> None:
        print(path)
        if os.path.exists(path):
            self.s_content = None
            self._path = path
        else:
            raise FileNotFoundError

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
        # print(dir(super()))
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

        # return super().s_content

    @content.setter
    def content(self, path):
        self.s_content = pd.read_csv(path)
        # super().s_content = pd.read_csv(path)

    def sample(self, n:int=5):
        print(self.s_content.sample(n=n))

    def expose(self):
        return list(self.s_content.to_dict().keys()), self.s_content.to_dict()
        # return self.s_content.to_dict()



        # return self.s_content.items()
        # print(next(self.s_content.iterrows())[1]['GMSL'])


if __name__ == "__main__":
    pass
    # data = DataCSV('./datasets/sea_level.csv')
    # data.sample()
    # print()
    # labels, holder = data.expose()

    # print(max(list(holder[labels[0]].keys())))

    # print(str([1]["Time"][265]) + " " + str(data.expose()[1]["GMSL"][265]))
    