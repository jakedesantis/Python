def jakeadd(a, b):
    """This just adds."""
    return a+b

def jakesubtract(a, b):
    """This just subtracts."""
    return a-b

def exponentialdecay(t, start, end, speed, plot):
    """t can be a positive scalar or vector of positives
    start is a starting float or int
    end is an ending float or int
    speed determines percent move from current towards end for each "t"
        for example speed=0.02 moves 2% closer to end each "t"
    plot is 1 (plot) or 0 (do not plot)
    works?
    """
    import numpy as np
    if plot == 1:
        import matplotlib.pyplot as plt
        plt.plot(t, start*np.exp(-speed*t)+end*(1-np.exp(-speed*t)))
        plt.show()
        map(exponentialdecay, t)
    return start*np.exp(-speed*t)+end*(1-np.exp(-speed*t))
