from pr5.abstract_factory_pattern import ModernFactory, RetroFactory
from pr5.builder_pattern import ComputerBuilder
from pr5.factory_method_pattern import RingCreator


def main():
    demo_builder()
    demo_abstract_factory()
    demo_factory_method()


def demo_builder():
    builder = ComputerBuilder()
    builder.create_memory()
    builder.create_motherboard()
    builder.create_cpu()
    builder.create_gpu()

    result = builder.product

    print(result)


def demo_factory_method():
    factory_method = RingCreator()
    res = factory_method.create_jewelry()
    print(res)


def demo_abstract_factory():
    modern_factory = ModernFactory()
    modern_computer_result = modern_factory.create_computer()
    modern_phone_result = modern_factory.create_phone()

    retro_factory = RetroFactory()
    retro_computer_result = retro_factory.create_computer()
    retro_phone_result = retro_factory.create_phone()

    retro_phone_result.call("911")
    retro_computer_result.boot_system()

    modern_phone_result.call("102")
    modern_computer_result.boot_system()


if __name__ == '__main__':
    main()
