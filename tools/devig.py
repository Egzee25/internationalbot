import numpy as np
from scipy.optimize import root


def dec_to_amer(dec):
    if not dec:
        return None
    if dec >= 2:
        amer = (dec - 1) * 100
    else:
        amer = (-100) / (dec - 1)
    return int(amer)


def calculate_decimal_odds(odds):
    if not odds:
        return None
    odds = float(odds)
    if odds == 100:
        p1dec = 2
    elif odds > 0:
        p1dec = (odds / 100) + 1
    else:
        odds = -1 * odds
        p1dec = (100 / odds) + 1
    return p1dec


def devig_power(odds):
    def pwr_func(nn, io):
        return io ** (1 / nn)

    odds = np.array([[calculate_decimal_odds(i) for i in odds]])
    n_odds, n_outcomes = odds.shape
    probs = np.zeros((n_odds, n_outcomes))
    fairOdds = np.empty((n_odds, n_outcomes))

    exponents = np.zeros(n_odds)
    inverted_odds = 1 / odds

    for ii in range(n_odds):
        def pwr_solvefor(nn, io):
            tmp = pwr_func(nn, io)
            return np.sum(tmp) - 1

        res = root(fun=pwr_solvefor, x0=0.5, args=(inverted_odds[ii,]))  # Using 0.5 as initial guess
        exponents[ii] = res.x
        probs[ii, :] = pwr_func(nn=res.x, io=inverted_odds[ii,])
        fairOdds[ii, :] = [dec_to_amer(1 / i) for i in pwr_func(nn=res.x, io=inverted_odds[ii,])]

    return fairOdds[0]


def calculate_ev(odds, fair_value):
    """
    Calculate the expected value of a bet.

    :param odds: American odds as an integer (e.g., 150 or -150)
    :param fair_value: Fair value probability as a decimal (e.g., 0.5 for 50%)
    :return: Expected value of the bet
        """
    # Convert American odds to implied probability
    if odds == 'N/A':
        return None, None
    if fair_value > 0:
        fair_value = 100 / (fair_value + 100)
    else:
        fair_value = -fair_value / (-fair_value + 100)
    dec = calculate_decimal_odds(odds)

    ev = ((dec - 1) * fair_value) - ((1 - fair_value))
    qk = kelly_criterion(dec, fair_value)
    return ev * 100, qk


def kelly_criterion(odds, probability_of_winning):
    """
    Calculate the optimal bet size using the Kelly Criterion.

    :param odds: Decimal odds of the bet (total return)
    :param probability_of_winning: The probability of winning the bet
    :return: The fraction of the bankroll to wager
    """
    # Convert decimal odds to b in the Kelly formula (b = odds - 1)
    b = odds - 1
    # The probability of losing
    q = 1 - probability_of_winning
    # Calculate the Kelly fraction
    kelly_fraction = (b * probability_of_winning - q) / b

    return kelly_fraction * 25




def worst_case_dec(odds):
    power = devig_power_dec(odds)[0]
    mult = devig_mult_dec(odds)
    if power > mult:
        return power
    return mult

def worst_case_amer(odds):
    odds = [calculate_decimal_odds(odd) for odd in odds]
    return worst_case_dec(odds)

def worst_case_amer_to_dec(odds):
    odds = [calculate_decimal_odds(odd) for odd in odds]
    return calculate_decimal_odds(worst_case_dec(odds))


def devig_power_dec(odds):
    def pwr_func(nn, io):
        return io ** (1 / nn)

    odds = np.array([[i for i in odds]])
    n_odds, n_outcomes = odds.shape
    probs = np.zeros((n_odds, n_outcomes))
    fairOdds = np.empty((n_odds, n_outcomes))

    exponents = np.zeros(n_odds)
    inverted_odds = 1 / odds

    for ii in range(n_odds):
        def pwr_solvefor(nn, io):
            tmp = pwr_func(nn, io)
            return np.sum(tmp) - 1

        res = root(fun=pwr_solvefor, x0=0.5, args=(inverted_odds[ii,]))  # Using 0.5 as initial guess
        exponents[ii] = res.x
        probs[ii, :] = pwr_func(nn=res.x, io=inverted_odds[ii,])
        fairOdds[ii, :] = [dec_to_amer(1 / i) for i in pwr_func(nn=res.x, io=inverted_odds[ii,])]
    return fairOdds[0]


def devig_mult_dec(odds):
    imps = []
    for odd in odds:
        imps.append(1/odd)
    return dec_to_amer(1 / (imps[0] / (sum(imps))))

def calculate_vig(odds: list):
    total = 0
    for odd in odds:
        imp = 1/calculate_decimal_odds(int(odd))
        total += imp
    return total

def hit():
    print('hit')

if __name__ == '__main__':
    odds = [-110, 300, 260]
    fair = worst_case_amer(odds)
    print(fair)
