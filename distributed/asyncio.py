raise ImportError(
    """

The dask.distributed.AioClient object has been removed.
We recommend using the normal client with asynchonrous=True

        client = await Client(..., asynchronous=True)

and a version of Tornado >= 5.

Documentation: https://distributed.dask.org/en/latest/asynchronous.html
Example: https://examples.dask.org/applications/async-await.html
"""
)
