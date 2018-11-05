def makebold(fn):

    def wrapped(world):
        return "<b>" + fn(world) + "</b>"
    return wrapped


def makeitalic(fn):

    def wrapped(world):
        return "<i>" + fn(world) + "</i>"

    return wrapped

import time
 
def timeit(func):

    def wrapped(world):
        start = time.clock()
        ss = func(world)
        end = time.clock()
        print(end - start)
        return ss

    return wrapped

@makebold
@makeitalic
@timeit
def hello(world):
    return world


import base64

if __name__ == '__main__':
    # 包含字节的字符串转成utf-8格式
    ss = '\xe8\xb0\xa3\xe8\xa8\x80'
    print(bytes(ss, 'l1').decode())
    ss = hello('sdfsadf')
    print(ss)

    ss = base64.b64decode('aHR0cDovL29wZW50dXRvcmlhbC5pbmZvL3B5dGhvbi9weXRob24lRTclODglQUMlRTglOTklQUIvcHl0aG9uLXNwaWRlcjAyLw==');
    print(ss)

