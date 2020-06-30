import re


BASE_URL = 'https://api.apexvs.com/'
CAMEL_REG = re.compile(r'(?<!^)(?=[A-Z])')


def get_header(token, custom_args: dict = None) -> dict:
    header = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    if custom_args is not None:
        header.update(custom_args)
    return header


def snake_to_camel(var: str) -> str:
    """Converts snake_case to CamelCase."""
    return ''.join(ch.capitalize() or '_' for ch in var.split('_'))


def camel_to_snake(var: str) -> str:
    """Converts CamelCase to snake_case"""
    return re.sub(CAMEL_REG, '_', var).lower()


def levenshtein_distance(s, t):
    """
    iterative_levenshtein(s, t) -> ldist
    ldist is the Levenshtein distance between the strings
    s and t.
    For all i and j, dist[i,j] will contain the Levenshtein
    distance between the first i characters of s and the
    first j characters of t
    Credit: https://www.python-course.eu/levenshtein_distance.php
    """

    rows = len(s) + 1
    cols = len(t) + 1
    dist = [[0 for x in range(cols)] for x in range(rows)]

    # source prefixes can be transformed into empty strings
    # by deletions:
    for i in range(1, rows):
        dist[i][0] = i

    # target prefixes can be created from an empty source string
    # by inserting the characters
    for i in range(1, cols):
        dist[0][i] = i

    for col in range(1, cols):
        for row in range(1, rows):
            if s[row - 1] == t[col - 1]:
                cost = 0
            else:
                cost = 1
            dist[row][col] = min(dist[row - 1][col] + 1,  # deletion
                                 dist[row][col - 1] + 1,  # insertion
                                 dist[row - 1][col - 1] + cost)  # substitution

    return dist[row][col]
