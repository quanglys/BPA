# -*- coding: utf-8 -*-
import argparse

def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-type', dest='type', type=int, nargs = 1)
    parser.add_argument('-name', dest='name', type=str, nargs = 1)
    parser.add_argument('-data', dest='data', type=str, nargs=1)
    parser.add_argument('-order', dest='order', type=int, nargs=1)
    parser.add_argument('-n', dest='n', type=int, nargs=1)
    parser.add_argument('-min', dest='min', type=int, nargs=1)
    parser.add_argument('-ses', dest='session', type=int, nargs=1)
    parser.add_argument('-num_node', dest='num_node', type=int, nargs=1)
    return parser
