# This script is for running a proxy that makes CULPA.info work

import socket


def get_culpa(url):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("67.207.80.130", 80))
    # s.connect(("culpa.info", 80))

    msg = """
GET {0} HTTP/1.1
Host: culpa.info
User-Agent: Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.5)
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Connection: close

"""

    sent = s.send(msg.format(url).encode('ascii'))
    if sent == 0:
        raise RuntimeError("socket connection broken")

    MSGLEN = 10000000
    def myreceive(sock):
        bytes_recd = 0
        msg = b""
        dummy_culpa = False
        while bytes_recd < MSGLEN:
            chunk = sock.recv(min(MSGLEN - bytes_recd, 2048))
            if chunk == b'':
                return msg
            bytes_recd = bytes_recd + len(chunk)
            msg += chunk
            if msg.startswith(b'Started GET'):
                dummy_culpa = True
            if dummy_culpa:
                # remove shit
                msgs = msg.split(b"\n")
                msg = b''
                for line in msgs[:-1]:
                    if line == b'HTTP/1.1 200 OK \r':
                        dummy_culpa = False
                    if not dummy_culpa:
                        msg += line + b"\n"
                msg += msgs[-1]
        return msg

    response = myreceive(s)
    return response


# server
class Proxy:
    def __init__(self,mhost,mport):
        self.mhost = mhost
        self.mport = mport

        self.middle_man = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.middle_man.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def start_listener(self):
        middle_man_recv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        middle_man_recv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        middle_man_recv.bind((self.mhost, self.mport))
        middle_man_recv.listen()
        print("Listening on " + self.mhost + ':' + str(self.mport)
              + ' ...\n---------------------------------')

        middle_man_forw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        middle_man_forw.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #middle_man_forw.connect_ex((self.dhost, self.dport))

        while True:
            #try:
            # Receive data
            client_socket,client_addr = middle_man_recv.accept()
            data = client_socket.recv(1024)
            print("Data received from client " + client_addr[0] + ':' + str(client_addr[1]))
            request_text = data.decode()
            gettxt = request_text.split("\n")[0]
            url = gettxt.split(" ")[1]
            print("Requested:", url)
            # print(data.decode())

            response = get_culpa(url)

            client_socket.sendall(response)
            client_socket.send(response)
            client_socket.close()


px = Proxy('localhost', 80)
px.start_listener()

print("Done")
