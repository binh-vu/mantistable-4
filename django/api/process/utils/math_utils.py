import editdistance

def edit_distance(s1, s2):
    """
        Normalized Levhenstein distance function between two strings
    """
    return editdistance.eval(s1, s2) / max((len(s1), len(s2), 1))

def step(x):
    """
        Step function
    """
    return 1 * (x > 0)