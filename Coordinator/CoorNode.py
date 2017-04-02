import json
import threading
import socket
import time
import operator
import copy
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Coordinator.ParseCor as ParseCor
except ImportError:
    import MyEnum
    import MyParser
    import ParseCor


DEBUG = True

ext = str('')
# init arguments
k = 4      # number elements in top
session = 0 # the number of session
band = 30  # limit bandwidth
ROW = 1

currentBand = 0
currentK = 0
netIn  = 0
netOut = 0
countAtt = 0 #the number of monitor node equal to the number of attributes

#value and name of top elements
topK = []
nameTop = []

lstSock = []
lstName = []

dataPart = {}
sumData = {}
numNode = 0
#used to mark the nodes received
nodeRecv = []
dataAtt = {} #data receive from node monitor i
currentBound = 0

#to check whether an user connnects to
bUserConnect = False

#event to notify that initialization completed
evnInitComplete = threading.Event()
evnInitComplete.clear()

#event to notify that waiting to receive data completed
evnWaitRcv = threading.Event()
evnWaitRcv.set()
numberWait = 0


IP_SERVER  = 'localhost'
PORT_NODE = 9407
PORT_USER = 7021
NUMBER_NODE = 1
DELTA_BAND = int(band / 10)
DELTA_EPS = 1
FILE_MON_NET = 'NetWorkLoad_'+ ext+'.dat'
FILE_MON_TOP = 'Top_' + ext + '.dat'

ORDER_NODE = 0
ORDER_VALUE = 1


NUM_MONITOR = 120

#interval to update network load
TIME_CAL_NETWORK = 3.0

################################################################################
def addNetworkIn(value:int):
    global netIn
    global lockNetIn
    lockNetIn.acquire()
    netIn += value
    lockNetIn.release()

def addNetworkOut(value:int):
    global netOut
    global lockNetOut
    lockNetOut.acquire()
    netOut += value
    lockNetOut.release()

def saveNetworkLoad(currentBand):
    with open(FILE_MON_NET, 'a') as f:
        f.write(str(int(currentBand))+ '\n')

def monNetwork():
    global lockNetIn
    global lockNetOut
    global netIn
    global netOut
    global countTime, DEBUG, evnInitComplete

    countTime = 0

    evnInitComplete.wait()
    while 1:
        time.sleep(TIME_CAL_NETWORK)
        lockNetIn.acquire()
        nIn = netIn / TIME_CAL_NETWORK
        netIn = 0
        lockNetIn.release()

        lockNetOut.acquire()
        nOut = netOut / TIME_CAL_NETWORK
        netOut = 0
        lockNetOut.release()

        countTime += 1
        if countTime > NUM_MONITOR:
            return
        saveNetworkLoad(nOut + nIn)
        if DEBUG:
            print('countTime; %d' %(countTime) )

################################################################################
def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

#read file config
def readConfig(fName : str):
    global DEBUG, MODE_EPS, k, IP_SERVER, PORT_NODE, FILE_MON_TOP, ROW
    global PORT_USER, NUMBER_NODE, FILE_MON_NET, NUM_MONITOR, TIME_CAL_NETWORK

    arg = ParseCor.readConfig(fName)
    if (arg == None):
        return

    DEBUG = arg.DEBUG
    MODE_EPS = arg.MODE_EPS
    ext = arg.ext
    k = arg.k
    IP_SERVER = arg.IP_SERVER
    PORT_NODE = arg.PORT_NODE
    PORT_USER = arg.PORT_USER
    NUMBER_NODE = arg.NUMBER_NODE
    ROW = arg.ROW
    FILE_MON_NET = 'NetWorkLoad_'+ ext+'.dat'
    FILE_MON_TOP = 'Top_' + ext + '.dat'
    NUM_MONITOR = arg.NUM_MONITOR
    TIME_CAL_NETWORK = arg.TIME_CAL_NETWORK

#init to run coordinator
def init():
    global serverForNode, serverForUser
    global lockCount, lockLst, lockTop, lockNetIn, lockNetOut, lockAddData
    global parser, k, numNode, lBound

    try:
        readConfig('corConfig.cfg')
    except Exception:
        pass

    for i in range(k):
        topK.append(0)
        nameTop.append(0)

    #init server to listen monitor node
    serverForNode = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForNode.bind((IP_SERVER, PORT_NODE))
    serverForNode.listen(NUMBER_NODE)

    #init server to listen user node
    serverForUser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForUser.bind((IP_SERVER, PORT_USER))
    serverForUser.listen(1)

    #init synchronize variable
    lockCount = threading.Lock()
    lockLst = threading.Lock()
    lockTop = threading.Lock()
    lockNetIn = threading.Lock()
    lockNetOut = threading.Lock()
    lockAddData  = threading.Lock()

    #init argument parser
    parser = MyParser.createParser()

    #delete old file
    f = open(FILE_MON_NET, 'w')
    f.close()

def getNodeOrder():
    global dataAtt
    dataSend = ''
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.SERVER_GET_NODE_ORDER.value})
    dataSend = createMessage(dataSend, {'-n':ROW})
    for i in range(countAtt):
        s = lstSock[i]
        s.sendall(bytes(dataSend.encode()))

    for i in range(countAtt):
        s = lstSock[i]
        dataRcv = s.recv(1024).decode()
        if (dataRcv != ''):
            arg = parser.parse_args(dataRcv.lstrip().split(' '))
            dataAtt[i] = copy.deepcopy(arg)

def getNodeValue(nodeNeed):
    dataSend = ''
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.SERVER_GET_DATA.value})
    dataSend = createMessage(dataSend, {'-data':nodeNeed})
    dataSend = createMessage(dataSend, {'-n':ROW})
    for i in range(countAtt):
        s = lstSock[i]
        s.sendall(bytes(dataSend.encode()))

    for i in range(countAtt):
        s = lstSock[i]
        dataRcv = s.recv(1024).decode()
        if (dataRcv != ''):
            arg = parser.parse_args(dataRcv.lstrip().split(' '))
            dataAtt[i] = copy.deepcopy(arg)

def beginProcess():
    global nodeRecv
    while(True):
        getNodeOrder()

        currentBound = 0
        nodeNeed = []
        for i in range(countAtt):
            value = dataAtt[i].min[0]
            currentBound += value
            arrNode = dataAtt[i].data[0]
            arrNode = json.loads(arrNode)
            for j in arrNode:
                if nodeRecv[j] == False:
                    nodeRecv[j] = True
                    nodeNeed.append(j)

        nodeNeed = json.dumps(nodeNeed).replace(' ','')

        if (topK[k-1] > currentBound):
            pass
        else:
            getNodeValue(nodeNeed)
            data = {}
            for i in range(countAtt):
                data[i] = json.loads(dataAtt[i].data[0])
            value = []
            for i in range(len(data)):
                value.append(data[0][i][ORDER_NODE], 0)
            for i in range(countAtt):
                for j in range(len(data)):
                    value[j][ORDER_VALUE] += data[i][j][ORDER_VALUE]


################################################################################
def addNewNode(s : socket.socket, orderNode):
    global lockCount
    global lockLst, eps, numNode, k, evnValidate, nodeRecv

    try:
        #receive name of the node
        dataRecv = s.recv(1024).decode()
        addNetworkIn(len(dataRecv))
        try:
            if (dataRecv != ''):
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                nameNode = arg.name[0]
                if (numNode == 0):
                    numNode = arg.num_node[0]
                    for i in range(numNode):
                        nodeRecv.append(False)
        except socket.error as e:
            print('Error: ' + str(e))
            return

        lockLst.acquire()
        lstSock.append(s)
        lstName.append(nameNode)
        lockLst.release()

        if (orderNode == NUMBER_NODE):
            evnInitComplete.set()

    except socket.error:
        pass

def acceptNode(server):
    global countAtt
    global lockCount
    countAtt = 0
    while (countAtt < NUMBER_NODE):
        print('%d\n' % (countAtt))
        (nodeSock, addNode) = server.accept()
        lockCount.acquire()
        countAtt += 1
        threading.Thread(target=addNewNode, args=(nodeSock, countAtt,)).start()
        lockCount.release()
    evnInitComplete.wait()
    beginProcess()
################################################################################
def acceptUser(server : socket.socket):
    global  userSock
    while (1):
        (userSock, addressUser) = server.accept()
        workWithUser(userSock)

def workWithUser(s : socket.socket):
    global parser
    global bUserConnect
    bUserConnect = True
    try:
        while 1:
            dataRecv = s.recv(1024).decode()
            if (dataRecv == ''):
                return
    except socket.error:
        return
    finally:
        bUserConnect = False
        s.close()

################################################################################
################################################################################

init()

# create thread for each server
thNode = threading.Thread(target=acceptNode, args=(serverForNode,))
thNode.start()

thMon = threading.Thread(target=monNetwork, args=())
thMon.start()

thUser = threading.Thread(target=acceptUser, args=(serverForUser,))
thUser.start()

try:
    #wait for all thread terminate
    thNode.join()
    thMon.join()
    thUser.join()
except KeyboardInterrupt:
    serverForNode.close()
    serverForUser.close()