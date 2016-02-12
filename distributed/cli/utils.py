py3_err_msg = """You are using Python 3 and your system is not compatible with the Unicode text model in Python 3
This usually gets solved by exporting this environment variables but it depends on your system:
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

For more information see: http://click.pocoo.org/5/python3/
"""


def check_python_3():
    """Ensures that the environment is good for unicode on Python 3."""
    try:
        from click import _unicodefun
        _unicodefun._verify_python3_env()
    except (TypeError, RuntimeError) as e:
        import sys
        import click
        click.echo(py3_err_msg, err=True)
        sys.exit(1)
