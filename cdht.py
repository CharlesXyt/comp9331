import socket
import sys
import threading
import time


host ='127.0.0.1'
predecessor =[]
successor =[]
bufsize =1024

for i in range(2,len(sys.argv)):
    successor.append(int(sys.argv[i]))
myid = sys.argv[1]
if len(successor)!=2 or sys.argv[0] !='cdht.py':
    sys.exit()

class udpclient(threading.Thread):
    def __init__(self, name):
        super(udpclient,self).__init__()
        self.name = name
        self._running = True

    def terminate(self):
        self._running = False

    def run(self):
        global successor,myid,bufsize,predecessor
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        l = []
        while True:
            i = 0
            if not self._running:
                break
            successor = clean(successor)
            for e in successor:
                time.sleep(5)
                addr = (host, 5000 + e)
                s.sendto('{} {}'.format(myid,i).encode(encoding='utf-8'), addr)
                i+=1
                try:
                    data, addr = s.recvfrom(bufsize)
                    data = data.decode(encoding='utf-8').split()
                    print('A ping response message was received from Peer {}.'.format(data[1]))
                    l.append(int(data[0]))
                except socket.error:
                    continue
            nb_0 = 0
            nb_1 = 0
            for e in l:
                if e == 0:
                    nb_0+=1
                if e==1:
                    nb_1+=1
            if nb_0 - nb_1 >4 and len(successor) == 2:
                m = successor[1]
                t_0 = tcpclient(name ='tcp_leave',id=successor[0],file = 0,flag='leave')
                t_0.start()
                while True:
                    if int(m) in successor:
                        successor.remove(int(m))
                        continue
                    if int(myid) in successor:
                        successor.remove(int(myid))
                        continue
                    break
                time.sleep(5)
                print('Peer {} is no longer alive.'.format(m))
                if len(successor) ==2:
                    print('My first successor is now peer {}.\nMy second successor is now peer {}.'.format(successor[0],successor[1]))
                else:
                    print('My first successor is now peer {}.'.format(successor[0]))
                l = []
            elif nb_1- nb_0>4 and len(successor) == 2:
                m = successor[0]
                t_0 = tcpclient(name='tcp_leave',id =successor[1],file = 1,flag='leave')
                t_0.start()
                while True:
                    if int(m) in successor:
                        successor.remove(int(m))
                        continue
                    if int(myid) in successor:
                        successor.remove(int(myid))
                        continue
                    break
                time.sleep(5)
                print('Peer {} is no longer alive.'.format(m))
                if len(successor) ==2:
                    print('My first successor is now peer {}.\nMy second successor is now peer {}.'.format(successor[0],successor[1]))
                else:
                    print('My first successor is now peer {}.'.format(successor[0]))
                l = []
            else:
                pass

class udpserver(threading.Thread):
    def __init__(self, name):
        super(udpserver,self).__init__()
        self.name = name
        self._running = True

    def terminate(self):
        self._running = False

    def run(self):
        global successor, myid, bufsize, predecessor
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        addr = (host, 5000 + int(myid))
        s.bind(addr)
        l = []
        while True:
            if not self._running:
                break
            try:
                data, addr = s.recvfrom(bufsize)
                data = data.decode(encoding='utf-8').split()
                seq = data[1]
                print('A ping request message was received from Peer {}'.format(data[0]))
                if int(data[0]) in predecessor:
                    pass
                else:
                    predecessor.append(int(data[0]))
                time.sleep(5)
                data = '{} {}'.format(seq,myid)
                s.sendto(data.encode(encoding='utf-8'), addr)
            except socket.error:
                continue


class tcpclient(threading.Thread):
    def __init__(self, name, id = 0, file = 0,flag = 'file',first = 0):
        super(tcpclient,self).__init__()
        self.name = name
        self.id = id
        self.file = file
        self.flag =flag
        self._running = True
        self.first = first
    def terminate(self):
        self._running = False
    def run(self):
        global successor, myid, bufsize, predecessor
        try:
            successor = clean(successor)
            if self.flag =='file':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addr = (host, 5000 + self.id)
                s.connect(addr)
                s.send('my id is {} request {} {}'.format(self.first, self.file,myid).encode(encoding='utf-8'))
                s.close()
            if self.flag =='quit':
                for e in predecessor:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    addr =(host, 5000 + e)
                    s.connect(addr)
                    s.send('quit {} {} {}'.format(myid, successor[0], successor[1]).encode(encoding='utf-8'))
                    s.close()
                for i in range(len(successor)):
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    if i == 0:
                        addr =(host, 5000 + successor[i])
                        s.connect(addr)
                        s.send('change {} {} {}'.format(myid,predecessor[1],i).encode(encoding='utf-8'))
                        s.close()
                    else:
                        addr = (host, 5000 + successor[i])
                        s.connect(addr)
                        s.send('change {} {} {}'.format(myid, predecessor[1], i).encode(encoding='utf-8'))
                        s.close()
            if self.flag =='leave':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addr = (host, 5000 + int(self.id))
                s.connect(addr)
                s.send('ask {}'.format(myid).encode(encoding='utf-8'))
                s_1 = s.recv(bufsize).decode(encoding='utf-8').split()
                if self.file:
                    successor.append(int(s_1[3]))
                else:
                    successor.append(int(s_1[4]))
                s.close()
            if self.flag =='find':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addr = (host, 5000 + int(self.id))
                s.connect(addr)
                s.send('find {} {}'.format(myid,self.file).encode(encoding='utf-8'))
                s.close()
        except socket.error:
            pass

class tcpserver(threading.Thread):
    def __init__(self, name):
        super(tcpserver,self).__init__()
        self.name = name
        self.id = id
        self._running = True
    def terminate(self):
        self._running = False
    def run(self):
        global successor, myid, bufsize, predecessor
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = (host, 5000 + int(myid))
        s.bind(addr)
        s.listen(5)
        while True:
            successor = clean(successor)
            if not self._running:
                break
            conn, addr = s.accept()
            request = conn.recv(bufsize)
            request = request.decode(encoding='utf-8').split()
            if request[0] =='quit':
                print('Peer {} will depart from the network.'.format(request[1]))
                if int(request[1]) == successor[0]:
                    successor = [int(request[2]),int(request[3])]
                else:
                    successor[1] = int(request[2])
                if int(myid) in successor:
                    successor.remove(int(myid))
                if len(successor) ==2:
                    print('My first successor is now peer {}.'.format(successor[0]))
                    print('My second successor is now peer {}.'.format(successor[1]))
                else:
                    print('My first successor is now peer {}.'.format(successor[0]))
            elif request[0] =='change':
                while True:
                    if int(request[1]) in predecessor:
                        predecessor.remove(int(request[1]))
                        break
                if int(request[3]) == 0:
                    predecessor.append(int(request[2]))
                else:
                    predecessor = [int(request[2])] + predecessor
            elif request[0] =='ask':
                conn.send('my successors are {} {}'.format(successor[0],successor[1]).encode(encoding='utf-8'))
            elif request[0] =='find':
                print('Received a response message from peer {}, which has the file {}'.format(request[1],request[2]))
            else:
                file = int(request[5])
                src = request[3]
                pre = int(request[6])
                a = file%256
                if a == int(myid):
                    result=1
                elif a<int(myid) and a>pre:
                    result = 1
                elif a<int(myid) and pre>int(myid):
                    result = 1
                elif a>pre and pre>int(myid):
                    result = 1
                else:
                    result = 0
                if result:
                    print('File {} is here.A response message, destined for peer {}, has been sent.'.format(file, src))
                    p_1 = tcpclient('tcpclient',src,file,'find')
                    p_1.start()
                else:
                    t3 = tcpclient('tcpclient', successor[0], file,'file',src)
                    t3.start()
                    print(
                        'File {} is not stored here.\n'
                        'File request message has been forwarded to my successor.'.format(file))


def clean(l):
    a = []
    for e in l:
        if e in a:
            continue
        else:
            a.append(e)
    return a

t = udpclient('udpclient')
t1 = udpserver('udpserver')
t2 = tcpserver('tcpserver')
t.start()
t1.start()
t2.start()

while True:
    try:
        successor = clean(successor)
        req = input(':').split()
        if req[0].lower() == 'request':
            a = int(req[1])
            if a < 0 or a > 9999:
                raise ValueError
            else:
                if a%256 == int(myid):
                    print('File {} is here.'.format(a))
                else:
                    t3 = tcpclient('tcpclient', successor[0], req[1],'file',myid)
                    t3.start()
                    print('File request message for {} has been sent to my successor.'.format(a))
        if req[0].lower() == 'quit':
            t4 = tcpclient(name='tcpclient_quit', flag='quit')
            t4.start()
            time.sleep(1)
            t.terminate()
            t1.terminate()
            t2.terminate()
            sys.exit()
    except ValueError:
        print('wrong input,please try again')
