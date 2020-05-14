#
# Author:      Jake
#
# Created:     12/04/2012
# Copyright:   (c) Jake 2012
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python

def main():
    pass

if __name__ == '__main__':
    main()
def do_nothing():
    pass


def JakeMail(TOADDRESS, SUBJECT, BODY):
    """
    test doc string
    """
    import smtplib
    FROMADDRESS = 'jake.desantis@gmail.com'
    LOGIN = 'jake.desantis'
    PASSWORD = 'revc0ooo'
    SERVERNAME = 'smtp.gmail.com'
    PORT = 587

    MSG = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n"
               % (FROMADDRESS, "".join(TOADDRESS), SUBJECT))
    MSG = MSG + BODY

    server = smtplib.SMTP(SERVERNAME, PORT) # try ports 25 and 465 also
    server.ehlo()
    server.starttls()
    server.login(LOGIN, PASSWORD)
    server.set_debuglevel(1)
    server.sendmail(FROMADDRESS, TOADDRESS, MSG)
    server.quit()

## General Math
def weighted_average(data):
     product = []
     total = 0.0
     count = 0.0
     i = 0
     while i < len(data[0]):
         product.append(data[0][i] * data[1][i])
         total = total + product[i]
         count = count + data[1][i]
         i = i + 1
     return total/count;

def factors(number):
    facts = []
    for i in range(number,0, -1):
        if (number % i) == 0:
            facts.append(i)
    return facts


## Prob & Stat
def perm_norep(n, k):
    import math
    return int(math.factorial(n) / math.factorial(n-k));
def perm_rep(n, k):
    import math
    return int(n**k);


def comb_norep(n, k):
    import math
    return int(math.factorial(n)/(math.factorial(k)*math.factorial(n-k)))

def comb_rep(n, k):
    import math
    return int(   math.factorial(n+k-1) / ((math.factorial(k)*math.factorial(n-1)) )   )


def bin_prob(n, k, p):
    import math
    return comb_norep(n, k)*(p**k)*(1-p)**(n-k);

## Linear Regression
def normalize(_x):
    import numpy as np
    x = np.array(_x)
    m = len(x)
    mu = np.mean(x)
    s = np.std(x)
    x_norm = ((x-mu)/s)
    return (x_norm)


def J(_x,_y):
    import numpy as np
    x = np.array(_x)
    y = np.array(_y)
    m = len(x)
    j = (1/(2*m))*sum(np.square(x-y))
    return(j)




## Physics
def decompose(velocity, angle):
    import math
    v_h = round(velocity * math.cos(angle * (2*math.pi/360)), 2)
    v_v = round(velocity * math.sin(angle * (2*math.pi/360)), 2)
    return v_h, v_v;

def cyl_vol(r,h):
    vol = h * 3.14159 * r**2
    return vol;



## Financial Options
def bs_call(S, K, t, r, q, sigma):
    import math
    from scipy.stats import norm
    S = float(S)
    K = float(K)
    t = float(t)
    r = float(r)
    q = float(q)
    sigma = float(sigma)
    d1 = (math.log(S/K) + t * (r - q + (sigma**2)/2)) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    Nd1 = norm.cdf(d1)
    Nd2 = norm.cdf(d2)
    call_price = S * math.exp(-q * t) * Nd1 - K * math.exp(-r * t) * Nd2
    return call_price, d1, d2;

def bs_put(S, K, t, r, q, sigma):
    import math
    from scipy.stats import norm
    S = float(S)
    K = float(K)
    t = float(t)
    r = float(r)
    q = float(q)
    sigma = float(sigma)
    d1 = (math.log(S/K) + t * (r - q + (sigma**2)/2)) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    Nd1 = norm.cdf(-d1)
    Nd2 = norm.cdf(-d2)
    put_price = - (S * math.exp(-q * t) * Nd1) + K * math.exp(-r * t) * Nd2
    return put_price, d1, d2;


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

