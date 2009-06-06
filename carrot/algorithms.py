from itertools import count


def maybe_it(it):
    if hasattr(it, "next"):
        return it
    return iter(it)


def roundrobin(iterables):
    """Cycle between iterators.

    :param iterables: List of iterators to cycle between.

    :raises StopIteration: When there are no values left in any
        of the iterators, or if limit is set and the limit has been reached.

    It's probably best explained by example:

        >>> it = roundrobin([iter([1, 2, 3]),
        ...                  iter([4, 5, 6]),
        ...                  iter([7, 8, 9])])
        >>> list(it)
        [1, 4, 7, 2, 5, 8, 3, 6, 9]

        >>> it = roundrobin([iter([1, 2, 3]),
        ...                  iter([4, 5]),
        ...                  iter([7, 8, 9])])
        >>> list(it)
        [1, 5, 7, 2, 5, 8, 3, 9]

        >>> it = roundrobin([iter([1, 2, 3]),
        ...                  iter([4, 5, 6]),
        ...                  iter([7, 8, 9])], limit=6)
        >>> list(it)
        [1, 4, 7, 2, 5, 8]
        
        >>> it = roundrobin([iter([1, 2, 3]),
        ...                  iter([4, 5, None, 6]),
        ...                  iter([7, 8, 9])], skip_none=True)
        >>> list(it)
        [1, 4, 7, 2, 5, 8, 3, 6, 9]

    """
    for iterations_so_far in count():
        got_value = False
        for it in iterables:
            try:
                value = it.next()
            except StopIteration:
                pass
            else:
                yield value
                got_value = True
        if not got_value:
            raise StopIteration
