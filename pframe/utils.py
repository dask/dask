def raises(exc, lamda):
    try:
        lamda()
        return False
    except exc:
        return True
