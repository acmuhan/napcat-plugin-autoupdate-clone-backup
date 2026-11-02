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
            try: aid=int(j[:i]); acks[aid]=json.loads(j[i:])
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
log("LOGIN_OK" if tok else "LOGIN_FAIL", str(r)[:120])
if not tok: open("r714c.txt","w").write("\n".join(OUT)); sys.exit(0)
emit("getMonitorList",None,6); time.sleep(3)
def dec(x):
    try: return brotli.decompress(base64.b64decode(x)).decode("utf8","replace")
    except Exception: return "RAWB64:"+str(x)[:80]
mons={}
for ts,m in list(pushes):
    if m.startswith("42") and '"monitorList"' in m:
        try:
            arr=json.loads(m[2:])
            if isinstance(arr,list) and arr and arr[0]=="monitorList":
                for mid,mv in (arr[1] or {}).items(): mons[int(mid)]=mv
        except Exception: pass
log("EXISTING_MONITORS", json.dumps({k:(v.get("name"),v.get("type"),v.get("url") or v.get("hostname"),v.get("active")) for k,v in mons.items()})[:1500])
def mine(nm): return nm.startswith(("zz714","ctl-","d17-","d18-","pub-","zzp-"))
for mid,mv in list(mons.items()):
    if mine(str(mv.get("name",""))):
        log("CLEAN",mid,mv.get("name"),str(emit("deleteMonitor",mid,8))[:60])
def add(mon):
    rr=emit("add",mon,12); mid=None
    try: mid=rr[0].get("monitorID")
    except Exception: pass
    log("ADD",mon["name"],mid); return mid
def tcp(n,h,p):
    return {"name":n,"type":"tcp","hostname":h,"port":p,"interval":20,"maxretries":0,"timeout":8,
            "active":True,"notificationIDList":{},"conditions":[],"accepted_statuscodes":["200-299"]}
def http(n,u,resp=30000):
    return {"name":n,"type":"http","url":u,"method":"GET","interval":20,"maxretries":0,
            "accepted_statuscodes":["200-499"],"timeout":10,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":resp,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":True,"headers":"{}","maxredirects":5}
batch=[]
for n,h,p in [("zzp-lo32311","127.0.0.1",32311),("zzp-lo5278","127.0.0.1",5278),
              ("zzp-17217","172.17.0.1",32311),("zzp-17218","172.18.0.1",32311),
              ("zzp-pub32311","103.40.14.130",32311),
              ("zzp-192168-1","192.168.1.1",80),("zzp-192168-5","192.168.1.5",80),
              ("zzp-192168-5b","192.168.1.5",3000),("zzp-192168-158","192.168.1.158",80),
              ("zzp-192168-91","192.168.1.91",5140),("zzp-192168-93","192.168.1.93",7898),
              ("zzp-192168-111","192.168.1.111",22)]:
    batch.append((n,add(tcp(n,h,p))))
batch.append(("zzp-http32311",add(http("zzp-http32311","http://127.0.0.1:32311/"))))
batch.append(("zzp-http5278",add(http("zzp-http5278","http://127.0.0.1:5278/actuator/health"))))
log("BATCH",json.dumps(batch))
time.sleep(60)
# collect heartbeat pushes
hbs={}
for ts,m in list(pushes):
    if m.startswith("42") and '"heartbeat"' in m:
        try:
            arr=json.loads(m[2:])
            if isinstance(arr,list) and len(arr)>1 and isinstance(arr[1],dict):
                b=arr[1]; hbs.setdefault(b.get("monitorID"),[]).append(b)
        except Exception: pass
log("PUSHES_TOTAL",len(pushes),"HB_MONITORS",sorted(hbs.keys()))
for mid,bs in hbs.items():
    for b in bs[-1:]:
        r=b.get("response")
        log("HB mid=%s status=%s ping=%s msg=%r resp=%s"%(mid,b.get("status"),b.get("ping"),str(b.get("msg"))[:200], dec(r)[:900] if r else ""))
for k,mid in batch:
    if not mid or mid in hbs: continue
    rr=emit("getMonitorBeats",[mid,1],15)
    log("BEATS",k,mid,str(rr)[:400])
for k,mid in batch:
    if mid: log("DEL",k,str(emit("deleteMonitor",mid,8))[:50])
emit("getMonitorList",None,6); time.sleep(3)
left={}
for ts,m in list(pushes):
    if m.startswith("42") and '"monitorList"' in m:
        try:
            arr=json.loads(m[2:])
            if arr[0]=="monitorList": left={k:v.get("name") for k,v in (arr[1] or {}).items()}
        except Exception: pass
log("LEFT",json.dumps(left)[:600])
open("r714c.txt","w").write("\n".join(OUT))
