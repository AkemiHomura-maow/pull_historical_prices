def get_block(ts, chain):
    if chain == 'op':
        return (ts - 1687598777) // 2 + 106000000 + 1
    elif chain == 'base':
        return (ts - 1688789347) // 2 + 1000000 + 1