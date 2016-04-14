Overview
========

*Bag* is an abstract collection, like *list* or *set*.  It is a friendly
synonym to multiset_. A bag or a multiset is a generalization of the concept
of a set that, unlike a set, allows multiple instances of the multiset's
elements.

* ``list``: *ordered* collection *with repeats*, ``[1, 2, 3, 2]``
* ``set``: *unordered* collection *without repeats*,  ``{1, 2, 3}``
* ``bag``: *unordered* collection *with repeats*, ``{1, 2, 2, 3}``

So a bag is like a list, but it doesn't guarantee an ordering among elements.
There can be repeated elements but you can't ask for a particular element.

.. _multiset: http://en.wikipedia.org/wiki/Bag_(mathematics)


Known Limitations
-----------------

Bags provide very general computation (any Python function.)  This generality
comes at cost.  Bags have the following known limitations:

1.  By default they rely on the multiprocessing scheduler, which has its own
    set of known limitations (see :doc:`shared`)
2.  Bag operations tend to be slower than array/dataframe computations in the
    same way that Python tends to be slower than NumPy/pandas
3.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
    Using ``Bag.foldby`` requires more thought.
4.  The implementation backing ``Bag.groupby`` is under heavy churn.
