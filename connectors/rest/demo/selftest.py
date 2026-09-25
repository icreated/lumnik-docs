#!/usr/bin/env python3
"""Proof that acme-api.py pages — run it before trusting a screen captured against the bench.

Every dialect is walked to its last page the way the hub's own strategy stops (a total, a
has_more flag, a next link, an empty page, a QueryPage flag), and the ids are counted: anything
but 2 400 distinct ids means the bench, not the connector, is what changed. The misbehaving
dialects and SOAP services are checked for the status — or the Fault — they are there to
produce.

    python3 docs/connectors/rest/demo/selftest.py     # exits non-zero on any failure
"""
import base64, json, re, sys, urllib.error, urllib.request as U

B = "http://127.0.0.1:8099"
BASIC = {"Authorization": "Basic " + base64.b64encode(b"demo-user:demo-pass").decode()}
BEARER = {"Authorization": "Bearer demo-token"}
KEY = {"X-API-Key": "demo-key"}

def get(url, h):
    r = U.urlopen(U.Request(url, headers=h)); return r.status, dict(r.headers), json.loads(r.read())

def walk(name, fn):
    ids = fn()
    ok = len(ids) == 2400 and len(set(ids)) == 2400
    print(f"{'ok ' if ok else 'FAIL'} {name:12} {len(ids)} rows, {len(set(ids))} distinct")
    return ok

def spring():
    ids, page = [], 0
    while True:
        _, _, b = get(f"{B}/spring/partners?page={page}&size=200", BASIC)
        ids += [r["id"] for r in b["content"]]
        page += 1
        if page >= b["totalPages"]: return ids

def drf():
    ids, url = [], f"{B}/drf/partners?page_size=200"
    while url:
        _, _, b = get(url, BEARER); ids += [r["id"] for r in b["results"]]; url = b["next"]
    return ids

def v1():
    ids, after = [], None
    while True:
        u = f"{B}/v1/partners?limit=200" + (f"&starting_after={after}" if after else "")
        _, _, b = get(u, BEARER); ids += [r["id"] for r in b["data"]]
        if not b["has_more"]: return ids
        after = b["data"][-1]["id"]

def link():
    ids, url = [], f"{B}/link/partners?limit=200"
    while url:
        _, h, b = get(url, KEY); ids += [r["id"] for r in b["data"]]
        nxt = h.get("Link", ""); url = nxt.split(">")[0][1:] if 'rel="next"' in nxt else None
    return ids

def offset(dialect, h, extra=""):
    def f():
        ids, off = [], 0
        while True:
            _, _, b = get(f"{B}/{dialect}/partners?offset={off}&limit=200{extra}", h)
            ids += [r["id"] for r in b["data"]]
            off += 200
            if off >= b["total"]: return ids
    return f

# ── SOAP: the same walk, in XML, with the offset in the body ──────────────────────────────
SOAP_BODY = """<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<CustomerQueryPage_Input><PageSize>{size}</PageSize><StartRowNum>{row}</StartRowNum>
</CustomerQueryPage_Input></soap:Body></soap:Envelope>"""

def soap_post(service, row, size=200, headers=None):
    r = U.urlopen(U.Request(f"{B}/{service}/start.swe",
                            data=SOAP_BODY.format(size=size, row=row).encode(),
                            headers={**BASIC, "Content-Type": "text/xml",
                                     "SOAPAction": '"document/urn:acme:CustomerQueryPage"',
                                     **(headers or {})}))
    return r.status, r.read().decode()

def soap_walk(service, flag, size=200):
    """Stop the way SoapPaginationStrategy stops: on the flag if the service publishes one,
    on a short page otherwise."""
    def f():
        ids, row = [], 0
        while True:
            _, body = soap_post(service, row, size)
            page = re.findall(r"<ns:Id>(\d+)</ns:Id>", body)
            ids += page
            if flag == "last_page" and "<ns:LastPage>true</ns:LastPage>" in body: return ids
            if flag == "more_records" and "<ns:MoreRecords>false</ns:MoreRecords>" in body: return ids
            if flag is None and len(page) < size: return ids
            row += size
    return f

def graphql():
    ids, after = [], None
    while True:
        q = {"query": "{ partners(first: 200) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } }",
             "variables": {"after": after} if after else {}}
        r = U.urlopen(U.Request(f"{B}/graphql", data=json.dumps(q).encode(),
                                headers={**KEY, "Content-Type": "application/json"}))
        b = json.loads(r.read())["data"]["partners"]
        ids += [e["node"]["id"] for e in b["edges"]]
        if not b["pageInfo"]["hasNextPage"]: return ids
        after = b["pageInfo"]["endCursor"]

results = [walk("spring", spring), walk("drf", drf), walk("v1-cursor", v1), walk("link", link),
           walk("offset", offset("offset", BASIC)), walk("open", offset("open", {})),
           walk("hdr", offset("hdr", {**BASIC, "X-Api-Version": "2026-01"})),
           walk("graphql", graphql),
           walk("soap-siebel", soap_walk("siebel", "last_page")),
           walk("soap-sap", soap_walk("sap", "more_records")),
           walk("soap-mute", soap_walk("mute", None))]

# The misbehaving dialects
try:
    get(f"{B}/down/partners", BASIC); print("FAIL down    answered 200")
except urllib.error.HTTPError as e: print(f"ok  down         HTTP {e.code}")
try:
    get(f"{B}/hdr/partners", BASIC); print("FAIL hdr     answered without the header")
except urllib.error.HTTPError as e: print(f"ok  hdr-nohdr    HTTP {e.code}")
U.urlopen(f"{B}/admin/reset")
codes = []
for _ in range(3):
    try: codes.append(get(f"{B}/flaky/partners?offset=0&limit=200", BASIC)[0])
    except urllib.error.HTTPError as e: codes.append(e.code)
print(f"{'ok ' if codes == [429, 503, 200] else 'FAIL'} flaky        {codes}")
# The SOAP services that answer with something other than a page
tail = soap_walk("tail", "last_page", size=100)()
print(f"{'ok ' if len(tail) == 201 else 'FAIL'} soap-tail    {len(tail)} rows (201 = the single-element last page survived)")
soap_codes = []
for svc in ("fault", "fault200"):
    try:
        st, body = soap_post(svc, 0)
    except urllib.error.HTTPError as e:
        st, body = e.code, e.read().decode()
    soap_codes.append((st, "<soap:Fault>" in body))
print(f"{'ok ' if soap_codes == [(500, True), (200, True)] else 'FAIL'} soap-faults  {soap_codes} (status, carries a Fault)")
try:
    soap_post("session", 0)
    session_code = 200
except urllib.error.HTTPError as e:
    session_code = e.code
print(f"{'ok ' if session_code == 401 else 'FAIL'} soap-session HTTP {session_code} without the token in <soap:Header>")
U.urlopen(f"{B}/admin/reset")
soap_flaky = []
for _ in range(3):
    try: soap_flaky.append(soap_post("flaky", 0)[0])
    except urllib.error.HTTPError as e: soap_flaky.append(e.code)
print(f"{'ok ' if soap_flaky == [500, 500, 200] else 'FAIL'} soap-flaky   {soap_flaky}")

q = {"query": "{ boom }"}
r = U.urlopen(U.Request(f"{B}/graphql", data=json.dumps(q).encode(), headers={**KEY, "Content-Type": "application/json"}))
print("ok  gql-errors   HTTP 200 carrying", list(json.loads(r.read()).keys()))
ok = (all(results) and codes == [429, 503, 200] and len(tail) == 201
      and soap_codes == [(500, True), (200, True)] and session_code == 401
      and soap_flaky == [500, 500, 200])
print("ALL OK" if ok else "SOMETHING FAILED")
sys.exit(0 if ok else 1)
