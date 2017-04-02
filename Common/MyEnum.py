import enum

class User(enum.Enum):
    USER_SET_K = 1
    USER_SET_ARG =  2
    USER_SET_BANDWIDTH = 3
    USER_GET_TOP = 4

class MonNode(enum.Enum):
    NODE_SET_NAME = 1
    NODE_SET_DATA = 2
    NODE_SET_NODE_ORDER = 3
    SERVER_SET_ARG = 4
    SERVER_GET_DATA = 5
    SERVER_GET_NODE_ORDER = 6
    SERVER_SET_FINISH_SESSION = 7
    DATA_GEN_AUTO = 8
