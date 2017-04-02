import time
import threading
import socket
import sys
import json
import copy
import operator
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Monitor.ParseMon as ParseMon
except ImportError:
    import MyEnum
    import MyParser
    import ParseMon

DEBUG = False
DATA_MODE = MyEnum.MonNode.DATA_GEN_AUTO.value

IP_SERVER  = 'localhost'
PORT_NODE = 9407
DELTA_TIME = 2.0
SAMPLE_ON_CIRCLE = 10
numberNode = 10
ORDER_NODE = 0
ORDER_VALUE = 1

eventStartMon = threading.Event()
eventStartMon.clear()
eventUpdate = threading.Event()
bStop = False
session = -1

lockData = threading.Lock()

#array to store current data monitored
currentData = []
#array to store snap of current data
dataToProcess = []
#array of data sorted from dataToProcess
dataSorted = []
#array to mark nodes sent to server
nodeSent = []
#mark the last current position sent to server
lastPosGet = 0

#socket to connect to server
sock = socket.socket()

################################################################################
#read from file config to get information
def readConfig():
    global myName, numberNode, addName, fileData, DEBUG, DATA_MODE, IP_SERVER, PORT_NODE, DELTA_TIME, SAMPLE_ON_CIRCLE
    addName = ''
    fName = 'monConfig_'
    try:
        addName = sys.argv[1]
    except Exception:
        pass
    myName = addName

    if (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        fileData = open('data_' + str(addName) + '.dat', 'r')

    fName += str(addName) + '.cfg'

    arg = ParseMon.readConfig(fName)
    if (arg == None):
        return
    myName = arg.NAME
    DEBUG = arg.DEBUG
    DATA_MODE = arg.DATA_MODE
    IP_SERVER = arg.IP_SERVER
    PORT_NODE = arg.PORT_NODE
    DELTA_TIME = arg.DELTA_TIME
    SAMPLE_ON_CIRCLE = arg.SAMPLE_ON_CIRCLE
    numberNode = arg.NUMBER_NODE
    print('Hello, myname is :' + str(myName))

def init():
    global lastPosGet, nodeSent
    readConfig()
    lastPosGet = 0
    for i in range(numberNode):
        nodeSent.append(False)

#create message to send to server
def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

def getData():

    if (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        global fileData
        line = fileData.readline().replace('\n','')
        if (line == ''):
            fileData.close()
            fileData = open('data_' + str(addName) + '.dat', 'r')
            line = fileData.readline().replace('\n', '')

        strData = line.split(' ')
        iData = []
        for i in range(numberNode):
            iData.append([i,int(strData[i])])

        return iData

def getNodeOrder(arg):
    global eventUpdate, lockData, dataToProcess, dataSorted, lastPosGet, sock, currentData
    numRow = arg.n[0]
    if (lastPosGet == 0):
        eventUpdate.clear()
        lockData.acquire()
        dataToProcess = copy.deepcopy(currentData)
        lockData.release()
        dataSorted = sorted(dataToProcess, key=(operator.itemgetter(ORDER_VALUE, ORDER_NODE)), reverse=True)

    i = 0
    dataSend = ''
    nodeOrderSend = []
    maxValue = 0
    while i < numRow:
        if lastPosGet >= numberNode:
            break
        index = dataSorted[lastPosGet][ORDER_NODE]
        if (nodeSent[index] == False):
            nodeSent[index] = True
            nodeOrderSend.append(index)
            if (i == 0):
                maxValue = dataSorted[lastPosGet][ORDER_VALUE]
            i += 1
            lastPosGet += 1
        else:
            lastPosGet += 1

    dataSend = createMessage(dataSend,{'-type':MyEnum.MonNode.NODE_SET_NODE_ORDER.value})
    nodeOrderSend = json.dumps(nodeOrderSend).replace(' ','')
    dataSend = createMessage(dataSend,{'-data':nodeOrderSend})
    dataSend = createMessage(dataSend, {'-min':maxValue})
    sock.sendall(bytes(dataSend.encode()))

def getNodeValue(arg):
    global nodeSent, sock
    nodeOrder = arg.data[0]
    nodeOrder = json.loads(nodeOrder)

    arrSend = []
    for i in nodeOrder:
        arrSend.append(dataToProcess[i])
        nodeSent[i] = True

    arrSend.sort(key=operator.itemgetter(ORDER_NODE))
    arrSend = json.dumps(arrSend).replace(' ','')
    dataSend = ''
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.NODE_SET_DATA.value})
    dataSend = createMessage(dataSend, {'-data':arrSend})
    sock.sendall(bytes(dataSend.encode()))

def setFinishSession():
    global lastPosGet
    lastPosGet = 0
    for i in range(numberNode):
        nodeSent[i] = False
    eventUpdate.set()

################################################################################
#communication with server
def workWithServer():
    global bStop, sock, currentData, numberNode, eventStartMon

    try:
        # send name
        dataSend = createMessage('', {'-type': MyEnum.MonNode.NODE_SET_NAME.value})
        dataSend = createMessage(dataSend, {'-name': myName})
        dataSend = createMessage(dataSend, {'-num_node':numberNode})
        sock.sendall(bytes(dataSend.encode('utf-8')))
        currentData = getData()
        #listen command from server
        while 1:
            try:
                dataRecv = sock.recv(1024).decode()
                if (dataRecv == ''):
                    return
                eventStartMon.set()
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                type = arg.type[0]
            except socket.error:
                return
            except Exception:
                continue

            #server update argument
            if type == MyEnum.MonNode.SERVER_GET_NODE_ORDER.value:
                getNodeOrder(arg)
            elif type == MyEnum.MonNode.SERVER_GET_DATA.value:
                getNodeValue(arg)
            elif type == MyEnum.MonNode.SERVER_SET_FINISH_SESSION:
                setFinishSession()
    except socket.error:
        pass

    finally:
        bStop = True
        sock.close()

#monitor data
def monData():
    global sock, currentData

    eventStartMon.wait()

    while (bStop == False):
        lockData.acquire()
        currentData = getData()
        lockData.release()
        time.sleep(DELTA_TIME)

################################################################################
################################################################################
#init connection
init()
#monData(None)
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (IP_SERVER, PORT_NODE)
    sock.connect(server_address)

    #init parser
    parser = MyParser.createParser()

    #init thread
    thMon = threading.Thread(target=monData, args=())
    thWork = threading.Thread(target=workWithServer, args=())

    thMon.start()
    thWork.start()

    #wait for all thread running
    thWork.join()
    thMon.join()

except socket.error as e:
    print(e)
    pass
