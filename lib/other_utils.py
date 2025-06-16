from collections import Counter
from collections.abc import Iterable
from typing import TypeVar

T = TypeVar('T')


def find_duplicates[T](iterable: Iterable[T]):
	c = Counter(iterable)
	return [k for k, count in c.items() if count > 1]
