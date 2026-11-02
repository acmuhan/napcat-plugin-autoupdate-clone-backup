import json,sys,time,threading,base64,gzip,zlib
import websocket
try:
    import brotli
except Exception:
    brotli=None
OUT=[]
def log(*a):
    s=" ".join(str(x) for x in a); print(s,flush=True); OUT.append(s)
def dec(x):
    if not x: return ""
    try: raw=base64.b64decode(x)
    except Exception: return ""
    for fn in (lambda b: brotli.decompress(b) if brotli else (_ for _ in ()).throw(Exception()),
               gzip.decompress, zlib.decompress):
        try: return fn(raw).decode("utf8","replace")
        except Exception: pass
    return raw.decode("utf8","replace")
ws=None
for a in range(4):
    try:
        ws=websocket.create_connection("ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket",timeout=25); break
    except Exception as e:
        log("CONN_ERR",a,str(e)[:120]); time.sleep(4)
if ws is None:
    open("r733d.txt","w").write("\n".join(OUT)); sys.exit(0)
acks={}; lock=threading.Lock(); pushes=[]
def reader():
    while True:
        try: m=ws.recv()
        except Exception: return
        if not isinstance(m,str): continue
        if m=="2":
            try: ws.send("3")
            except Exception: return
            continue
        if m.startswith("43"):
            j=m[2:]; i=0
            while i<len(j) and j[i].isdigit(): i+=1
            try: acks[int(j[:i])]=json.loads(j[i:])
            except Exception: continue
            continue
        pushes.append((time.time(),m))
threading.Thread(target=reader,daemon=True).start()
seq=[1]
def emit(ev,data=None,wait=15):
    a=seq[0]; seq[0]+=1
    msg="42%d%s"%(a,json.dumps([ev,data])) if data is not None else "42%d%s"%(a,json.dumps([ev]))
    try: ws.send(msg)
    except Exception as e: log("SEND_ERR",ev,str(e)[:80]); return None
    end=time.time()+wait
    while time.time()<end:
        with lock:
            if a in acks: return acks.pop(a)
        time.sleep(0.05)
    return None
ws.send("40"); time.sleep(1.5)
r=emit("login",{"username":"muhan","password":"Liu310013.","token":""},20)
try: tok=r[0].get("token")
except Exception: tok=None
log("LOGIN","OK" if tok else "FAIL")
if not tok:
    open("r733d.txt","w").write("\n".join(OUT)); sys.exit(0)
def monmap():
    emit("getMonitorList",None,12); time.sleep(2.2)
    for ts,m in pushes[::-1]:
        if m.startswith("42") and '"monitorList"' in m:
            try:
                arr=json.loads(m[2:])
                if isinstance(arr,list) and arr and arr[0]=="monitorList" and isinstance(arr[1],dict):
                    return {int(k):v for k,v in arr[1].items()}
            except Exception: pass
    return {}
m0=monmap(); log("MON_BEFORE",json.dumps({k:v.get("name") for k,v in m0.items()})[:400])
def add(mon):
    rr=emit("add",mon,12)
    try: return rr[0].get("monitorID")
    except Exception: return None
def P(nm,h,p,to=6):
    return {"name":nm,"type":"port","hostname":h,"port":int(p),"interval":60,"maxretries":0,"timeout":to,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"],"saveResponse":False}
tests=[]
for net in range(19,31):
    for p in (32311,):
        nm="zz733-b%d-%d"%(net,p); tests.append((nm,"172.%d.0.1"%net,p,add(P(nm,"172.%d.0.1"%net,p))))
    nm="zz733-b%d-80"%net; tests.append((nm,"172.%d.0.1"%net,80,add(P(nm,"172.%d.0.1"%net,80))))
for h in ("10.0.0.1","192.168.0.1","172.16.0.1","192.168.2.1"):
    nm="zz733-p-%s"%h.replace(".","_"); tests.append((nm,h,22,add(P(nm,h,22))))
log("ADDED",len([t for t in tests if t[3]]),"of",len(tests))
time.sleep(80)
hbs={}
for ts,m in list(pushes):
    if m.startswith("42") and '"heartbeat"' in m:
        try:
            arr=json.loads(m[2:])
            if isinstance(arr,list) and len(arr)>1 and isinstance(arr[1],dict):
                b=arr[1]
                if b.get("monitorID") is not None: hbs.setdefault(int(b["monitorID"]),[]).append(b)
        except Exception: pass
for nm,h,p,mid in tests:
    if not mid: continue
    bs=hbs.get(mid) or []
    if not bs:
        rr=emit("getMonitorBeats",[mid,1],8)
        try: bs=rr[0].get("data") or []
        except Exception: bs=[]
    if not bs: log("R %s NOBEAT"%nm); continue
    b=bs[-1]; log("R %s st=%s msg=%s"%(nm,b.get("status"),str(b.get("msg"))[:70]))
d=0
for mid,mv in monmap().items():
    if str(mv.get("name","")).startswith("zz733"):
        emit("deleteMonitor",mid,4); d+=1
log("DELETED",d); log("MON_AFTER",json.dumps({k:v.get("name") for k,v in monmap().items()})[:400])
open("r733d.txt","w").write("\n".join(OUT))
