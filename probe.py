import json,sys,time,threading,base64,gzip,zlib,io
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
    for name,fn in (("brotli", lambda b: brotli.decompress(b) if brotli else (_ for _ in ()).throw(Exception("nobrotli"))),
                    ("gzip", lambda b: gzip.decompress(b)),
                    ("zlib", lambda b: zlib.decompress(b))):
        try:
            return fn(raw).decode("utf8","replace")
        except Exception:
            pass
    try: return raw.decode("utf8","replace")
    except Exception: return repr(raw[:80])

URL="ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket"
ws=None
for attempt in range(3):
    try:
        ws=websocket.create_connection(URL,timeout=30,header=["Origin: http://103.40.14.130:3101"])
        break
    except Exception as e:
        log("CONN_ERR",attempt,type(e).__name__,str(e)[:200]); time.sleep(3)
if ws is None:
    log("NO_CONN"); open("r733.txt","w").write("\n".join(OUT)); sys.exit(0)

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
    except Exception as e: log("SEND_ERR",ev,str(e)[:100]); return None
    end=time.time()+wait
    while time.time()<end:
        with lock:
            if a in acks: return acks.pop(a)
        time.sleep(0.05)
    return None

log("OPEN", str(pushes[0][1])[:80] if pushes else None)
ws.send("40"); time.sleep(1.5)
r=emit("login",{"username":"muhan","password":"Liu310013.","token":""},20)
try: tok=r[0].get("token")
except Exception: tok=None
log("LOGIN", "OK" if tok else "FAIL", str(r)[:200])
if not tok:
    open("r733.txt","w").write("\n".join(OUT)); sys.exit(0)

inf=emit("info",None,10); log("INFO", str(inf)[:300])

def monmap():
    emit("getMonitorList",None,10); time.sleep(2.0)
    out={}
    for ts,m in pushes[::-1]:
        if m.startswith("42") and '"monitorList"' in m:
            try:
                arr=json.loads(m[2:])
                if isinstance(arr,list) and arr and arr[0]=="monitorList":
                    data=arr[1] or {}
                    if isinstance(data,dict):
                        for k,v in data.items(): out[int(k)]=v
                    break
            except Exception: pass
    return out

PRE=("zz733","zz714","zzp-","ctl-","d17-","d18-","pub-")
before=monmap()
log("MON_BEFORE", json.dumps({k:(v.get("name"),v.get("type"),v.get("active")) for k,v in before.items()})[:1500])
deleted=0
for mid,mv in list(before.items()):
    if str(mv.get("name","")).startswith(PRE):
        emit("deleteMonitor",mid,6); deleted+=1
log("CLEANED_LEFTOVER",deleted)

def add(mon):
    rr=emit("add",mon,15); mid=None
    try: mid=rr[0].get("monitorID")
    except Exception: pass
    if mid is None:
        log("ADD_FAIL",mon["name"],str(rr)[:200])
    return mid

def P(n,h,p):
    return {"name":n,"type":"port","hostname":h,"port":int(p),"interval":20,"maxretries":0,"timeout":6,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"],
            "saveResponse":False}
def H(n,u,resp=4000):
    return {"name":n,"type":"http","url":u,"method":"GET","interval":20,"maxretries":0,
            "accepted_statuscodes":["100-599"],"timeout":8,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":resp,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":True,"headers":"{}","maxredirects":5}

batch=[]
WEB=[80,443,3000,3030,5215,8088,8089,8686,9000,9443,11688,13021,23334,24678,5140,8080,9090]
for h in ["192.168.1.158","192.168.1.5","192.168.1.91","192.168.1.93"]:
    tag=h.split(".")[-1]
    for p in WEB:
        nm="zz733-h%s-%d"%(tag,p)
        batch.append((nm,h,p,add(H(nm,"http://%s:%d/"%(h,p)))))
for h in ["192.168.1.1","192.168.1.111"]:
    tag=h.split(".")[-1]
    for p in [80,443]:
        nm="zz733-h%s-%d"%(tag,p)
        batch.append((nm,h,p,add(H(nm,"http://%s:%d/"%(h,p)))))
for h in ["192.168.1.158","192.168.1.5","192.168.1.91","192.168.1.93","192.168.1.1","192.168.1.111"]:
    tag=h.split(".")[-1]
    nm="zz733-t%s-22"%tag
    batch.append((nm,h,22,add(P(nm,h,22))))
for h in ["192.168.1.158","192.168.1.5","192.168.1.91","192.168.1.93"]:
    tag=h.split(".")[-1]
    for p in [3306,6379,3389,5900]:
        nm="zz733-t%s-%d"%(tag,p)
        batch.append((nm,h,p,add(P(nm,h,p))))
for p in [7897,7898,1080,1081]:
    nm="zz733-t93-%d"%p
    batch.append((nm,"192.168.1.93",p,add(P(nm,"192.168.1.93",p))))
log("ADDED", len([b for b in batch if b[3]]), "of", len(batch))

# wait for beats
time.sleep(80)
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
log("HB_PUSH_COUNT",len(hbs))

res=[]
for nm,h,p,mid in batch:
    if not mid: continue
    bs=hbs.get(mid) or []
    if not bs:
        rr=emit("getMonitorBeats",[mid,1],10)
        try: bs=rr[0].get("data") or []
        except Exception: bs=[]
    if not bs:
        res.append((nm,"NOBEAT","","")); continue
    b=bs[-1]
    body=dec(b.get("response"))
    res.append((nm, "UP" if b.get("status")==1 else ("DOWN" if b.get("status")==0 else str(b.get("status"))),
                str(b.get("msg") or ""), body.replace("\r"," ").replace("\n"," ")[:600]))
for nm,st,msg,body in res:
    log("R %s %s msg=%s body=%s"%(nm,st,msg[:120],body[:400]))

log("=== OPEN SUMMARY ===")
for nm,st,msg,body in res:
    if st=="UP":
        log("OPEN",nm,msg[:100],body[:200])

# cleanup
left=monmap()
d=0
for mid,mv in left.items():
    if str(mv.get("name","")).startswith("zz733"):
        emit("deleteMonitor",mid,5); d+=1
log("DELETED",d)
after=monmap()
log("MON_AFTER", json.dumps({k:(v.get("name"),v.get("type")) for k,v in after.items()})[:800])
open("r733.txt","w").write("\n".join(OUT))
