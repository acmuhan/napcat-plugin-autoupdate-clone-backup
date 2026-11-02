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
    except Exception: return "B64ERR"
    for fn in (lambda b: brotli.decompress(b) if brotli else (_ for _ in ()).throw(Exception()),
               gzip.decompress, zlib.decompress):
        try: return fn(raw).decode("utf8","replace")
        except Exception: pass
    return raw.decode("utf8","replace")
URL="ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket"
ws=None
for a in range(4):
    try:
        ws=websocket.create_connection(URL,timeout=25); break
    except Exception as e:
        log("CONN_ERR",a,type(e).__name__,str(e)[:160]); time.sleep(4)
if ws is None:
    open("r733b.txt","w").write("\n".join(OUT)); sys.exit(0)
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
log("LOGIN","OK" if tok else "FAIL",str(r)[:150])
if not tok:
    open("r733b.txt","w").write("\n".join(OUT)); sys.exit(0)
def monmap():
    emit("getMonitorList",None,12); time.sleep(2.2)
    out={}
    for ts,m in pushes[::-1]:
        if m.startswith("42") and '"monitorList"' in m:
            try:
                arr=json.loads(m[2:])
                if isinstance(arr,list) and arr and arr[0]=="monitorList" and isinstance(arr[1],dict):
                    for k,v in arr[1].items(): out[int(k)]=v
                    break
            except Exception: pass
    return out
PRE=("zz733","zz714","zzp-","ctl-","d17-","d18-","pub-")
m0=monmap()
log("MON_BEFORE",json.dumps({k:(v.get("name"),v.get("type")) for k,v in m0.items()})[:1200])
n=0
for mid,mv in list(m0.items()):
    if str(mv.get("name","")).startswith(PRE):
        emit("deleteMonitor",mid,5); n+=1
log("CLEANED",n)
def add(mon):
    rr=emit("add",mon,15)
    try: return rr[0].get("monitorID")
    except Exception: return None
def P(n,h,p,to=10):
    return {"name":n,"type":"port","hostname":h,"port":int(p),"interval":60,"maxretries":0,"timeout":to,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"],"saveResponse":False}
def H(n,u,to=15):
    return {"name":n,"type":"http","url":u,"method":"GET","interval":60,"maxretries":0,
            "accepted_statuscodes":["100-599"],"timeout":to,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":800,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":False,"headers":"{}","maxredirects":3}
def PING(n,h,to=10):
    return {"name":n,"type":"ping","hostname":h,"interval":60,"maxretries":0,"timeout":to,
            "active":True,"notificationIDList":{},"conditions":[],"packetSize":56}
tests=[]
def tp(nm,fn,*a):
    mid=add(fn(nm,*a)); tests.append((nm,mid))
# --- controls ---
tp("zz733-ctl-blackhole-10_255",P,"10.255.255.1",80)
tp("zz733-ctl-testnet-192_0_2_1",P,"192.0.2.1",80)
tp("zz733-ctl-host-pub-24678",P,"103.40.14.130",24678)
tp("zz733-ctl-host-pub-13142",P,"103.40.14.130",13142)
tp("zz733-ctl-br17217-5278",P,"172.17.0.1",5278)
tp("zz733-ctl-br17218-24678",P,"172.18.0.1",24678)
# --- host itself via LAN alias ---
tp("zz733-lan-self-91-24678",P,"192.168.1.91",24678)
tp("zz733-lan-self-91-13142",P,"192.168.1.91",13142)
tp("zz733-lan-self-91-3101",P,"192.168.1.91",3101)
tp("zz733-lan-self-91-5215",P,"192.168.1.91",5215)
# --- LAN hosts ---
for h,tag in [("192.168.1.1","gw"),("192.168.1.5","op5"),("192.168.1.93","s93"),("192.168.1.111","s111"),("192.168.1.158","op158")]:
    for p in (22,80,443):
        tp("zz733-lan-%s-%d"%(tag,p),P,h,p)
tp("zz733-lan-op5-3000",P,"192.168.1.5",3000)
tp("zz733-lan-op5-11688",P,"192.168.1.5",11688)
tp("zz733-lan-op158-11688",P,"192.168.1.158",11688)
tp("zz733-lan-op158-3000",P,"192.168.1.158",3000)
tp("zz733-lan-s93-7897",P,"192.168.1.93",7897)
tp("zz733-lan-s93-7898",P,"192.168.1.93",7898)
tp("zz733-lan-s93-1080",P,"192.168.1.93",1080)
# --- http controls ---
tp("zz733-http-blackhole",H,"http://10.255.255.1/")
tp("zz733-http-testnet",H,"http://192.0.2.1/")
tp("zz733-http-host24678",H,"http://103.40.14.130:24678/")
tp("zz733-http-self91-24678",H,"http://192.168.1.91:24678/")
tp("zz733-http-op158-80",H,"http://192.168.1.158:80/",10)
tp("zz733-http-op5-80",H,"http://192.168.1.5:80/",10)
tp("zz733-http-gw-80",H,"http://192.168.1.1:80/",10)
# --- ping ---
tp("zz733-ping-gw",PING,"192.168.1.1")
tp("zz733-ping-host",PING,"103.40.14.130")
tp("zz733-ping-blackhole",PING,"10.255.255.1")
log("ADDED",len([t for t in tests if t[1]]),"of",len(tests))
time.sleep(150)
hbs={}
for ts,m in list(pushes):
    if m.startswith("42") and '"heartbeat"' in m:
        try:
            arr=json.loads(m[2:])
            if isinstance(arr,list) and len(arr)>1 and isinstance(arr[1],dict):
                b=arr[1]
                if b.get("monitorID") is not None:
                    hbs.setdefault(int(b["monitorID"]),[]).append(b)
        except Exception: pass
log("HB_MONITORS",len(hbs))
for nm,mid in tests:
    if not mid: continue
    bs=hbs.get(mid) or []
    if not bs:
        rr=emit("getMonitorBeats",[mid,1],10)
        try: bs=rr[0].get("data") or []
        except Exception: bs=[]
    if not bs:
        log("R %s - NOBEAT"%nm); continue
    b=bs[-1]
    body=dec(b.get("response")).replace("\r"," ").replace("\n"," ")[:200]
    extra={k:v for k,v in b.items() if k not in ("response","monitorID","important")}
    log("R %s st=%s %s body=%s"%(nm,b.get("status"),json.dumps(extra)[:260],body))
d=0
for mid,mv in monmap().items():
    if str(mv.get("name","")).startswith("zz733"):
        emit("deleteMonitor",mid,4); d+=1
log("DELETED",d)
log("MON_AFTER",json.dumps({k:v.get("name") for k,v in monmap().items()})[:600])
open("r733b.txt","w").write("\n".join(OUT))
