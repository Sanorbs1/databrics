n=12


def fun(n):
  if n==0:
    return 1
  else:
    return n*fun(n-1)

print(fun(n))

"Add docstrings : this file is for docstring practice"
def square(number: int) -> int:
    """
    Returns the square of a number.

    Args:
        number: Input integer.

    Returns:
        Square of the integer.
    """
    return number * number

"type hints"
def add(a: int, b: int) -> int:
    return a + b

"F-strings"
salary = 50000

print(f"Salary is {salary}")
