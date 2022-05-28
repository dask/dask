from json import dumps
from platform import uname
from sys import stdout, version_info

from pandas._typing import JSONSerializable
from pandas.compat._optional import get_version, import_optional_dependency


def show_versions(as_json: bool = False) -> None:
    """Provide version information for bug reports.

    Parameters
    ----------
    as_json : bool, default False
        * If False, outputs info in a YAML format to the console (can be copy-
          pasted into an environment.yml file for conda).
        * If True, outputs info in JSON format to the console.
    """

    def _format_as_conda_environment_yaml(result: dict[str, JSONSerializable]) -> str:
        """A small utility to return dependencies in a conda-friendly format."""
        formatted_result = ""
        for dep, dep_ver in result.items():
            if dep == "Platform" or dep_ver is None:
                continue
            formatted_result += f"- {dep}={dep_ver}"
            formatted_result += "\n"

        return formatted_result

    deps = [
        "dask",
        "distributed",
        "numpy",
        "pandas",
        "cloudpickle",
        "fsspec",
        "bokeh",
        "fastparquet",
        "pyarrow",
        "zarr",
    ]

    result = {
        # note: only major, minor, micro are extracted
        "Python": ".".join([str(i) for i in version_info[:3]]),
        "Platform": uname().system,
    }

    for modname in deps:
        mod = import_optional_dependency(modname, errors="ignore")
        result[modname] = get_version(mod) if mod else None

    if as_json is True:
        stdout.writelines(dumps(result, indent=2))
    elif as_json is False:
        stdout.writelines(_format_as_conda_environment_yaml(result))
