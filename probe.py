import json,sys,time,threading,base64
import websocket, brotli
OUT=[]
def log(*a):
    s=" ".join(str(x) for x in a); print(s,flush=True); OUT.append(s)
ws=websocket.create_connection("ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket",timeout=30)
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
seq=1
def emit(ev,data=None,wait=12):
    global seq
    a=seq; seq+=1
    ws.send("42%d%s"%(a,json.dumps([ev,data])) if data is not None else "42%d%s"%(a,json.dumps([ev])))
    end=time.time()+wait
    while time.time()<end:
        with lock:
            if a in acks: return acks.pop(a)
        time.sleep(0.1)
    return None
ws.send("40"); time.sleep(1.5)
r=emit("login",{"username":"muhan","password":"Liu310013.","token":""},15)
try: tok=r[0].get("token")
except Exception: tok=None
log("LOGIN_OK" if tok else "LOGIN_FAIL", str(r)[:100])
if not tok: open("r714d.txt","w").write("\n".join(OUT)); sys.exit(0)
def monlist():
    emit("getMonitorList",None,10); time.sleep(2.5)
    out={}
    for ts,m in pushes[::-1]:
        if m.startswith("42") and '"monitorList"' in m:
            try:
                arr=json.loads(m[2:])
                if arr[0]=="monitorList":
                    for k,v in (arr[1] or {}).items(): out[int(k)]=v
                    break
            except Exception: pass
    return out
PREF=("zz714","ctl-","d17-","d18-","pub-","zzp-")
def sweep(tag):
    ml=monlist(); n=0
    for mid,mv in ml.items():
        nm=str(mv.get("name",""))
        if nm.startswith(PREF):
            emit("deleteMonitor",mid,8); n+=1
    log("SWEEP",tag,"deleted",n)
    return monlist()
left=sweep("pre")
log("REST_AFTER_PRE",json.dumps({k:(v.get("name"),v.get("type")) for k,v in left.items()})[:600])
def add(mon):
    rr=emit("add",mon,12); mid=None
    try: mid=rr[0].get("monitorID")
    except Exception: pass
    log("ADD",mon["name"],mid,str(rr)[:100] if not mid else "")
    return mid
def P(n,h,p):
    return {"name":n,"type":"port","hostname":h,"port":p,"interval":20,"maxretries":0,"timeout":8,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"]}
def H(n,u,resp=40000):
    return {"name":n,"type":"http","url":u,"method":"GET","interval":20,"maxretries":0,
            "accepted_statuscodes":["200-499"],"timeout":12,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":resp,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":True,"headers":"{}","maxredirects":5}
batch=[]
tests=[]
for h,p in [("127.0.0.1",3001),("127.0.0.1",3101),("127.0.0.1",32311),("127.0.0.1",5278),
            ("172.17.0.1",32311),("172.18.0.1",32311),("172.17.0.1",5278),("172.17.0.1",24678),
            ("172.18.0.1",24678),("172.17.0.1",13142),("103.40.14.130",32311),("103.40.14.130",24678),
            ("103.40.14.130",13142),("103.40.14.130",9443),("103.40.14.130",5215),("103.40.14.130",7500),
            ("192.168.1.1",80),("192.168.1.1",443),("192.168.1.5",80),("192.168.1.5",443),
            ("192.168.1.5",3000),("192.168.1.158",80),("192.168.1.158",443),("192.168.1.158",11688),
            ("192.168.1.91",5140),("192.168.1.93",7898),("192.168.1.111",22)]:
    nm="zz714p-%s-%d"%(h.replace(".","_"),p); tests.append((nm,h,p)); batch.append((nm,add(P(nm,h,p))))
for nm,u in [("zz714h-lo3001","http://127.0.0.1:3001/"),("zz714h-pub32311","http://103.40.14.130:32311/"),
             ("zz714h-pub32311api","http://103.40.14.130:32311/api/status"),
             ("zz714h-act5278","http://103.40.14.130:5278/actuator/health"),
             ("zz714h-env5278","http://103.40.14.130:5278/actuator/env"),
             ("zz714h-1p24678","http://103.40.14.130:24678/"),
             ("zz714h-13142","http://103.40.14.130:13142/"),
             ("zz714h-vibe","https://vibecode.muhan.wiki/")]:
    batch.append((nm,add(H(nm,u))))
time.sleep(75)
hbs={}
for ts,m in pushes:
    if m.startswith("42") and '"heartbeat"' in m:
        try:
            arr=json.loads(m[2:])
            if isinstance(arr,list) and len(arr)>1 and isinstance(arr[1],dict):
                hbs.setdefault(arr[1].get("monitorID"),[]).append(arr[1])
        except Exception: pass
def dec(x):
    try: return brotli.decompress(base64.b64decode(x)).decode("utf8","replace")
    except Exception: return ""
seen=set()
for nm,mid in batch:
    if not mid: continue
    bs=hbs.get(mid) or []
    if not bs: log("NOBEAT",nm,mid); continue
    for b in bs[-1:]:
        body=dec(b.get("response"))[:1200] if b.get("response") else ""
        log("R",nm,"st=",b.get("status"),"ping=",b.get("ping"),"msg=",str(b.get("msg"))[:150],"body=",body.replace("\n"," ")[:900])
log("KEYS_SEEN",len(hbs))
sweep("post")
open("r714d.txt","w").write("\n".join(OUT))
