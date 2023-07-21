import functools

from dask.dataframe.utils import (
    AttributeNotImplementedError,
    clear_known_categories,
    has_known_categories,
)
from dask.utils import M

from dask_expr._accessor import Accessor, PropertyMap
from dask_expr._expr import Elemwise


class CategoricalAccessor(Accessor):
    """
    Accessor object for categorical properties of the Series values.

    Examples
    --------
    >>> s.cat.categories  # doctest: +SKIP

    Notes
    -----
    Attributes that depend only on metadata are eager

    * categories
    * ordered

    Attributes depending on the entire dataset are lazy

    * codes
    * ...

    So `df.a.cat.categories` <=> `df.a._meta.cat.categories`
    So `df.a.cat.codes` <=> `df.a.map_partitions(lambda x: x.cat.codes)`
    """

    _accessor_name = "cat"
    _accessor_methods = (
        "add_categories",
        "as_ordered",
        "as_unordered",
        "remove_categories",
        "rename_categories",
        "reorder_categories",
        "set_categories",
    )
    _accessor_properties = ()

    @property
    def known(self):
        """Whether the categories are fully known"""
        return has_known_categories(self._series)

    def as_known(self, **kwargs):
        """Ensure the categories in this series are known.

        If the categories are known, this is a no-op. If unknown, the
        categories are computed, and a new series with known categories is
        returned.

        Parameters
        ----------
        kwargs
            Keywords to pass on to the call to `compute`.
        """
        if self.known:
            return self._series
        from dask_expr._collection import new_collection

        categories = (
            new_collection(PropertyMap(self._series.expr, "cat", "categories"))
            .unique()
            .compute()
        )
        return self.set_categories(categories.values)

    def as_unknown(self):
        """Ensure the categories in this series are unknown"""
        if not self.known:
            return self._series

        from dask_expr import new_collection

        return new_collection(AsUnknown(self._series.expr))

    @property
    def ordered(self):
        """Whether the categories have an ordered relationship"""
        return self._delegate_property(self._series._meta, "cat", "ordered")

    @property
    def categories(self):
        """The categories of this categorical.

        If categories are unknown, an error is raised"""
        if not self.known:
            msg = (
                "`df.column.cat.categories` with unknown categories is not "
                "supported.  Please use `column.cat.as_known()` or "
                "`df.categorize()` beforehand to ensure known categories"
            )
            raise AttributeNotImplementedError(msg)
        return self._delegate_property(self._series._meta, "cat", "categories")

    @property
    def codes(self):
        """The codes of this categorical.

        If categories are unknown, an error is raised"""
        if not self.known:
            msg = (
                "`df.column.cat.codes` with unknown categories is not "
                "supported.  Please use `column.cat.as_known()` or "
                "`df.categorize()` beforehand to ensure known categories"
            )
            raise AttributeNotImplementedError(msg)
        from dask_expr._collection import new_collection

        return new_collection(PropertyMap(self._series.expr, "cat", "codes"))


class AsUnknown(Elemwise):
    _parameters = ["frame"]
    operation = M.copy

    @functools.cached_property
    def _meta(self):
        return clear_known_categories(self.frame._meta)
