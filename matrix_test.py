#!/usr/bin/env python3
# coding=utf-8
#
"""
Test numpy matrix operations
"""

import csv
from pprint import pp
from numpy import matrix

def numpy_matrix_test():
    """Test numpy matrix table manipulation"""
    with open('Input/PrisonerAddresses.csv', newline='') as csvfile:
        my_data = matrix(list(csv.reader(csvfile)))
    pp(my_data.tolist())


# Initiate run if executing as a script (not module load)
if __name__ == '__main__':
    # numpy_matrix_test()
    numpy_matrix_test()
