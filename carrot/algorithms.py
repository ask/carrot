import itertools


def cycle(iterables, limit=None, skip_none=False, infinite=False):
    """Cycle between iterators.

    :param iterables: List of iterators to cycle between.

    :keyword limit: An optional maximum number of cycles.

    :keyword skip_none: Skip ``None`` values.

    :keyword infinite: Will respect the iterators :exc:`StopIteration`, but
        will cycle around them again if all of them return ``None`` in
        a single pass.

    :raises StopIteration: When there are no values left in any
        of the iterators, or if limit is set and the limit has been reached.

    It's probably best explained by example:

        >>> it = cycleiterators([iter([1, 2, 3]),
        ...                      iter([4, 5, 6]),
        ...                      iter([7, 8, 9])])
        >>> list(it)
        [1, 4, 7, 2, 5, 8, 3, 6, 9]

        >>> it = cycleiterators([iter([1, 2, 3]),
        ...                      iter([4, 5]),
        ...                      iter([7, 8, 9])])
        >>> list(it)
        [1, 5, 7, 2, 5, 8, 3, 9]

        >>> it = cycleiterators([iter([1, 2, 3]),
        ...                      iter([4, 5, 6]),
        ...                      iter([7, 8, 9])], limit=6)
        >>> list(it)
        [1, 4, 7, 2, 5, 8]
        
        >>> it = cycleiterators([iter([1, 2, 3]),
        ...                      iter([4, 5, None, 6]),
        ...                      iter([7, 8, 9])], skip_none=True)
        >>> list(it)
        [1, 4, 7, 2, 5, 8, 3, 6, 9]

    """

    def W(t):
        import sys
        sys.stderr.write(t+"\n")

    def maybe_it(it):
        if hasattr(it, "next"):
            return it
        return iter(it)

    iterators = map(maybe_it, iterables)

    values_so_far = 0
    for iterations_so_far in itertools.count():
        W("Iteration: %d" % iterations_so_far)
        got_value = False
        for it in iterators:
            try:
                W("GETTING VALUE")
                value = it.next()
                if value:
                    W("GOT VALUE: %s" % value.body)
                else:
                    W("GOT VALUE: %s" % value)
            except StopIteration, e:
                if infinite:
                    raise e
            else:
                if value is None and skip_none:
                    pass
                else:
                    got_value = True
                    if limit and values_so_far >= limit:
                        raise StopIteration
                    values_so_far += 1
                    yield value
        if not got_value and not infinite:
            raise StopIteration
        import time
        time.sleep(0.5)
