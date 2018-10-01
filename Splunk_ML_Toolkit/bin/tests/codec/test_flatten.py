#!/usr/bin/env python

from codec.flatten import flatten, expand


class EqualityMixin(object):
    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


class Bar(EqualityMixin):
    def __repr__(self):
        return '%s = %s' % (
            object.__repr__(self),
            self.__dict__
        )


class Foo(EqualityMixin):
    def __init__(self, count=0):
        self.bar = Bar()
        self.count = count
        if count < 10:
            self.foo = Foo(count + 1)

    def __repr__(self):
        return '%s = %s' % (
            object.__repr__(self),
            self.__dict__
        )


def test_basic():
    foo = Foo()

    flat_foo, refs = flatten(foo)
    foo_copy = expand(flat_foo, refs)

    assert foo == foo_copy


class Loop(object):
    def __init__(self):
        self.next = None


def test_loop():
    loop1 = Loop()
    loop2 = Loop()
    loop1.next = loop2
    loop2.next = loop1

    flat_loop, refs = flatten(loop1)
    foo_copy = expand(flat_loop, refs)

    assert foo_copy == foo_copy.next.next
    assert foo_copy.next == foo_copy.next.next.next
