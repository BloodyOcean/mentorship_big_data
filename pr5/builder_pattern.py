from abc import ABC, abstractmethod
from typing import Any


class Computer:
    _components = None

    def __init__(self):
        self._components = []

    def add_component(self, component: Any):
        self._components.append(component)

    def __str__(self):
        return f"{self._components}"


class BuilderInterface(ABC):

    @abstractmethod
    def create_motherboard(self) -> None:
        pass

    @abstractmethod
    def create_cpu(self) -> None:
        pass

    @abstractmethod
    def create_gpu(self) -> None:
        pass

    @abstractmethod
    def create_memory(self) -> None:
        pass

    @property
    @abstractmethod
    def product(self) -> Computer:
        pass


class ComputerBuilder(BuilderInterface):

    def __init__(self):
        self._computer = Computer()

    def create_motherboard(self) -> None:
        self._computer.add_component('MSI motherboard')

    def create_cpu(self) -> None:
        self._computer.add_component("Intel Core i7 CPU")

    def create_gpu(self) -> None:
        self._computer.add_component("Radeon HD7567")

    def create_memory(self) -> None:
        self._computer.add_component("8 GB RAM, 1TB SSD")

    def reset(self) -> None:
        self._computer = Computer()

    @property
    def product(self) -> Computer:
        result = self._computer
        self.reset()
        return result
