#!/usr/bin/python
import sys, getopt

from main.extract_data import *

def main(*agrs):
    student_id = '0BrbPbwCMz'
    prob_name = 'PROP01'
    prob = prob_solve_single(student_id=student_id, prob_name=prob_name)
    print 'Probability: ', prob

if __name__ == "__main__":
    main(sys.argv[1:])