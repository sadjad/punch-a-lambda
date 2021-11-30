#!/usr/bin/env python3

import sys
import socket
import selectors
import types

sel = selectors.DefaultSelector()
public_ips = {}
talk_to = set()
dones = set()

BD = int(sys.argv[2])

def accept_wrapper(sock):
    global public_ips

    conn, addr = sock.accept()  # Should be ready to read
    print(f"[{len(public_ips)}] accepted connection from", addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"", name = "")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)


def service_connection(key, mask):
    global talk_to, public_ips, msg, dones
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        #print("read socket:" , sock)
        recv_data = None
        try:
            recv_data = sock.recv(1024)  # Should be ready to read
        except:
            pass
        if recv_data:
            #data.outb += recv_data
            if "name" in str(recv_data,"utf-8"):
                data.name = str(recv_data,"utf-8").split(":")[1]
                public_ips[data.name] = sock.getpeername()
                if len(public_ips) == BD:
                    msg = "addresses" 
                    for name in sorted(public_ips.keys()):
                        talk_to.add(name)
                        msg += ";" + name + ":" + public_ips[name][0]
                    msg += ";END"
            if "done" in str(recv_data,"utf-8"):
                name = str(recv_data,"utf-8").split(":")[1]
                if name not in dones:
                    print("done ",name)
                dones.add(name)
        else:
            pass
    if mask & selectors.EVENT_WRITE:
        if data.name in talk_to:
            data.outb += bytes(msg,"utf-8")
            print("writing to " + data.name + " 's port")
            talk_to.remove(data.name)
        if len(dones) == BD:
            data.outb += bytes("all done","utf-8")
            #print("all done here")
        if data.outb:
            #print("echoing", repr(data.outb), "to", data.addr)
            try:
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
            except:
                pass

host = ''
port = int(sys.argv[1])
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
lsock.bind((host, port))
lsock.listen()
print("listening on", (host, port))
lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=None)
import time
try:
    while True:
        #time.sleep(1)
        events = sel.select(timeout=None)
#        print(events)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)
except KeyboardInterrupt:
    print("caught keyboard interrupt, exiting")
finally:
    sel.close()
