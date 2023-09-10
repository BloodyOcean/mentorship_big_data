from abc import ABC, abstractmethod


class Phone(ABC):
    @abstractmethod
    def call(self, number: str) -> None:
        pass


class Smartphone(Phone):
    def call(self, number: str) -> None:
        print(f'Your are dialing via smartphone {number}')


class Retrophone(Phone):
    def call(self, number: str) -> None:
        print(f'Your are dialing via old phone {number}')


class Computer(ABC):
    @abstractmethod
    def boot_system(self) -> None:
        pass


class Laptop(Computer):
    def boot_system(self) -> None:
        print('Your are booting modern laptop')


class Retrocomputer(Computer):
    def boot_system(self) -> None:
        print('Your are booting old computer')


class AbstractFactory(ABC):
    @abstractmethod
    def create_phone(self) -> Phone:
        pass

    @abstractmethod
    def create_computer(self) -> Computer:
        pass


class RetroFactory(AbstractFactory):
    def create_phone(self) -> Phone:
        return Smartphone()

    def create_computer(self) -> Computer:
        return Laptop()


class ModernFactory(AbstractFactory):
    def create_phone(self) -> Phone:
        return Retrophone()

    def create_computer(self) -> Computer:
        return Retrocomputer()
