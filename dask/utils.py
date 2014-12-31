def raises(err, lamda):
    try:
        lamda()
        return False
    except err:
        return True
