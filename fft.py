from numpy.fft import fft as np_fft

def fft(x):
    return np_fft(x)


def volume(x):
    return [abs(val) for val in x]


def make_power_of_2(n):
    if n == 0:
        return 1
    return 2**((n-1).bit_length())