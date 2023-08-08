class A:
    __a = None

    def __init__(self):
        self.__a = 5

    def foo(self):
        pass


def main():
    x = A()

    # Get object metadata
    print(x.__dict__)
    print(x._A__a)

    x._A__a = 10
    print(x._A__a)


if __name__ == '__main__':
    main()
