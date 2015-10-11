#!/usr/bin/python
import sys, getopt

from main.extract_data import *

def main(argv):
    input_file = ''
    output_file = ''

    do_extract = True
    task = ''

    try:
        opts, args = getopt.getopt(argv,"hsi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print 'extract_data.py -i <inputfile> -o <outputfile>'
        sys.exit(2)

    if opts:
        for opt, arg in opts:
            if opt == '-h':
                print 'extract_data.py -i <inputfile> -o <outputfile>'
                sys.exit()
            elif opt in ("-i", "--ifile"):
                input_file = arg
            elif opt in ("-o", "--ofile"):
                output_file = arg
            elif opt in ('-s', "--sim"):
                task = 'calculate_similarity'
                do_extract = False
            else:
                do_extract = False
    else:
        do_extract = False

    if do_extract:
        if not input_file:
            print 'No input file, set default: %s' % KDD_DATA_TEST
        print 'Input: ', input_file
        extract_number(input_file)
    else:
        if task == 'calculate_similarity':
            calculate_similarity()
        else:
            print 'Unknown task: %s' % task
            print 'Use help: extract_data.py -h'

    # print 'Output:', outputfile


if __name__ == "__main__":
    main(sys.argv[1:])