import argparse

def createParse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-DEBUG', dest='DEBUG', type = bool, default= False)
    parser.add_argument('-ext', dest='ext', type=str, default='')
    parser.add_argument('-k', dest='k', type=int, default= 4)
    parser.add_argument('-ROW', dest='ROW', type=int, default=1)
    parser.add_argument('-IP_SERVER', dest = 'IP_SERVER', type=str, default=None)
    parser.add_argument('-NUMBER_NODE', dest='NUMBER_NODE', type=int, default=1)
    parser.add_argument('-NUM_MONITOR', dest='NUM_MONITOR', type=int, default=120)
    parser.add_argument('-TIME_CAL_NETWORK', dest='TIME_CAL_NETWORK', type=float, default=3.0)
    return parser

def readConfig(fName:str):
    data = ''
    try:
        with open(fName, 'r') as f:
            while 1:
                temp = f.readline().replace('\n', '')
                if (temp == ''):
                    break
                data += temp + ' '
    except Exception as e:
        return None

    data = data.rstrip()
    if (len(data) == 0):
        return None
    data = data.split(' ')
    arg = createParse()
    return arg.parse_args(data)