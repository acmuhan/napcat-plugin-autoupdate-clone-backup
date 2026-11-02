import json,sys,time,threading,base64
import websocket
try:
    import brotli
except Exception:
    brotli=None
OUT=[]
def log(*a):
    s=" ".join(str(x) for x in a); print(s,flush=True); OUT.append(s)
URL="ws://103.40.14.130:3101/socket.io/?EIO=4&transport=websocket"
ws=websocket.create_connection(URL,timeout=30)
acks={}; lock=threading.Lock(); pushes=[]
def reader():
    while True:
        try: m=ws.recv()
        except Exception: return
        if not isinstance(m,str):
            continue
        if m=="2":
            try: ws.send("3")
            except Exception: return
            continue
        if m.startswith("43"):
            j=m[2:]
            i=0
            while i<len(j) and j[i].isdigit(): i+=1
            try: aid=int(j[:i]); payload=json.loads(j[i:])
            except Exception: continue
            with lock: acks[aid]=payload
            continue
        pushes.append(m)
t=threading.Thread(target=reader,daemon=True); t.start()
def recv_wait(tmo=25):
    end=time.time()+tmo
    while time.time()<end:
        try: return ws.recv()
        except Exception: time.sleep(0.2)
    return None
def emit(ev,data=None,wait=12):
    global seq
    a=seq; seq+=1
    msg="42%d%s"%(a,json.dumps([ev,data])) if data is not None else "42%d%s"%(a,json.dumps([ev]))
    ws.send(msg)
    end=time.time()+wait
    while time.time()<end:
        with lock:
            if a in acks: return acks.pop(a)
        time.sleep(0.15)
    return None
seq=1
# connect namespace 40
ws.send("40"); time.sleep(1.5)
r=emit("login",{"username":"muhan","password":"Liu310013.","token":""},15)
log("LOGIN",str(r)[:180])
try: tok=r[0].get("token")
except Exception: tok=None
if not tok:
    log("LOGIN_FAILED",str(r)[:300]); open("r714b.txt","w").write("\n".join(OUT)); sys.exit(0)
log("SETTINGS",str(emit("getSettings",None,10))[:600])
ml=emit("getMonitorList",None,12)
log("MONLIST",str(ml)[:1200])
# cleanup any leftover monitors we created
try:
    for mid,mv in (ml[0].get("data") or {}).items() if isinstance(ml[0].get("data"),dict) else []:
        nm=str(mv.get("name",""))
        if nm.startswith("zz714") or nm.startswith("ctl-"):
            log("CLEAN",mid,nm,str(emit("deleteMonitor",int(mid),10))[:100])
except Exception as e:
    log("CLEANERR",e)
def add(mon):
    rr=emit("add",mon,15)
    mid=None
    try: mid=rr[0].get("monitorID")
    except Exception: pass
    log("ADD",mon["name"],"mid=",mid,"raw=",str(rr)[:160])
    return mid
def tcp(name,host,port):
    return {"name":name,"type":"tcp","hostname":host,"port":port,"interval":20,"maxretries":0,
            "timeout":8,"active":True,"notificationIDList":{},"conditions":[],
            "accepted_statuscodes":["200-299"],"saveResponse":False}
def http(name,url,resp=20000):
    return {"name":name,"type":"http","url":url,"method":"GET","interval":20,"maxretries":0,
            "accepted_statuscodes":["200-499"],"timeout":10,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":resp,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":True,"headers":"{}","maxredirects":5}
ids={}
targets=[("ctl-3101","127.0.0.1",3101),("ctl-5278","127.0.0.1",5278),("ctl-32311","127.0.0.1",32311),
         ("d17-32311","172.17.0.1",32311),("d18-32311","172.18.0.1",32311),("pub-32311","103.40.14.130",32311)]
for nm,h,p in targets:
    ids["tcp:"+nm]=add(tcp(nm,h,p))
ids["http:32311"]=add(http("zz714h-32311","http://127.0.0.1:32311/"))
ids["http:5278"]=add(http("zz714h-5278","http://127.0.0.1:5278/actuator/health"))
log("IDS",json.dumps(ids))
time.sleep(50)
for k,mid in ids.items():
    if not mid: continue
    rr=emit("getMonitorBeats",[mid,1],15)
    if not rr: log("BEATS",k,"NONE"); continue
    try: beats=rr[0].get("data") or []
    except Exception: log("BEATS",k,str(rr)[:300]); continue
    if not beats: log("BEATS",k,"EMPTY")
    for b in beats[-2:]:
        resp=b.get("response"); dec=""
        if resp and brotli:
            try: dec=brotli.decompress(base64.b64decode(resp)).decode("utf8","replace")[:1200]
            except Exception as e: dec="DECERR "+str(e)
        log("BEAT",k,"status=",b.get("status"),"msg=",str(b.get("msg"))[:200],"resp=",dec)
for k,mid in ids.items():
    if mid: log("DEL",k,str(emit("deleteMonitor",mid,10))[:80])
log("MONLEFT",str(emit("getMonitorList",None,12))[:800])
open("r714b.txt","w").write("\n".join(OUT))
