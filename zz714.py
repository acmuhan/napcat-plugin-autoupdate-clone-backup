import json,sys,time
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
def rd(t=3):
    try:
        ws.settimeout(t); return ws.recv()
    except Exception: return None
log("OPEN",rd())
ws.send("40"); log("NS",rd())
n=[0]
def emit(ev,data=None,wait=10):
    n[0]+=1; a=n[0]
    msg="42%d%s"%(a,json.dumps([ev,data])) if data is not None else "42%d%s"%(a,json.dumps([ev]))
    ws.send(msg)
    end=time.time()+wait
    while time.time()<end:
        m=rd(max(0.3,end-time.time()))
        if m is None: break
        if isinstance(m,str) and m.startswith("43%d"%a):
            return m[3:]
    return None
r=emit("login",{"username":"muhan","password":"Liu310013.","token":""},12)
log("LOGIN",str(r)[:200])
try: tok=json.loads(r)[0].get("token")
except Exception: tok=None
if not tok:
    log("LOGIN_FAILED"); open("r714.txt","w").write("\n".join(OUT)); sys.exit(0)
for ev in ["info","getSettings","getMonitorList","getProxyList","getNotificationList"]:
    log("EV",ev,str(emit(ev,None,10))[:900])
ids={}
def add(mon):
    rr=emit("add",mon,12); log("ADD",mon["name"],str(rr)[:200])
    try: return (json.loads(rr) or [{}])[0].get("monitorID")
    except Exception: return None
def tcp(name,host,port):
    return {"name":name,"type":"tcp","hostname":host,"port":port,"interval":20,"maxretries":0,
            "timeout":8,"active":True,"notificationIDList":{},"conditions":[],
            "accepted_statuscodes":["200-299"],"saveResponse":False}
def http(name,url,resp=20000):
    return {"name":name,"type":"http","url":url,"method":"GET","interval":20,"maxretries":0,
            "accepted_statuscodes":["200-499"],"timeout":10,"active":True,"saveResponse":True,
            "saveErrorResponse":True,"responseMaxLength":resp,"notificationIDList":{},
            "conditions":[],"ignoreTls":True,"cacheBust":True,"headers":"{}","maxredirects":5}
targets=[("ctl-3101","127.0.0.1",3101),("ctl-5278","127.0.0.1",5278),("ctl-32311","127.0.0.1",32311),
         ("d17-32311","172.17.0.1",32311),("d18-32311","172.18.0.1",32311),("pub-32311","103.40.14.130",32311)]
for nm,h,p in targets:
    ids["tcp:"+nm]=add(tcp(nm,h,p))
ids["http:32311"]=add(http("zz714h32311","http://127.0.0.1:32311/"))
ids["http:5278"]=add(http("zz714h5278","http://127.0.0.1:5278/actuator/health"))
log("IDS",json.dumps(ids))
time.sleep(40)
for k,mid in ids.items():
    if not mid: continue
    rr=emit("getMonitorBeats",[mid,1],12)
    if not rr: log("BEATS",k,"NONE"); continue
    try: beats=json.loads(rr)[0].get("data",[])
    except Exception: log("BEATS",k,str(rr)[:400]); continue
    for b in beats[-2:]:
        resp=b.get("response")
        dec=""
        if resp and brotli:
            try: dec=brotli.decompress(__import__("base64").b64decode(resp)).decode("utf8","replace")[:1500]
            except Exception as e: dec="DECERR "+str(e)
        log("BEAT",k,"status=",b.get("status"),"msg=",str(b.get("msg"))[:300],"resp=",dec)
for k,mid in ids.items():
    if mid: log("DEL",k,str(emit("deleteMonitor",[mid]) or emit("deleteMonitor",mid,10))[:120])
log("MONLEFT",str(emit("getMonitorList",None,10))[:600])
open("r714.txt","w").write("\n".join(OUT))
