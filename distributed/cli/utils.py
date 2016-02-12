py3_err_msg = """
Your terminal does not properly support unicode text required by command line
utilities running Python 3.  This is commonly solved by specifying encoding
environment variables, though exact solutions may depend on your system:

    $ export LC_ALL=C.UTF-8
    $ export LANG=C.UTF-8

For more information see: http://click.pocoo.org/5/python3/
""".strip()


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
