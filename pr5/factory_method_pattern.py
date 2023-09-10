from abc import ABC, abstractmethod


class Jewelry:
    pass


class Ring(Jewelry):
    pass


class Chain(Jewelry):
    pass


class Creator(ABC):

    @abstractmethod
    def create_jewelry(self) -> Jewelry:
        pass

    @abstractmethod
    def sell(self):
        pass


class RingCreator(Creator):
    def create_jewelry(self) -> Ring:
        return Ring()

    def sell(self):
        print("The ring has been sold")


class ChainCreator(Creator):
    def create_jewelry(self) -> Chain:
        return Chain()

    def sell(self):
        print("The ring has been sold")
