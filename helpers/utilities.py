def clean_market_cap(string: str) -> str:
    string = string.strip()
    if string[-1] == 'B':
        string = string.replace('B', '')
        string = string.replace('$', '')
        string = str((float(string) * 1000000000))
    elif string[-1] == 'M':
        string = string.replace('M', '')
        string = string.replace('$', '')
        string = str((float(string) * 1000000))
    elif string[-1] == 'K':
        string = string.replace('K', '')
        string = string.replace('$', '')
        string = str((float(string) * 1000))
    else:
        string = '0'
    return string