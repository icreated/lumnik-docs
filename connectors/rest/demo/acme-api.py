#!/usr/bin/env python3
"""ACME Partners API — the source every screen on docs/connectors/rest.md reads.

One HTTP server, no dependencies beyond the Python standard library, serving the SAME 2 400
partners under ten JSON dialects, a GraphQL door and eight SOAP services. The first path segment
picks the envelope; it never changes the data. That is the whole point: a manifest that works
here differs from the one you will write for your own ERP by a base URL, a credential and a path.

    python3 docs/connectors/rest/demo/acme-api.py        # foreground, one log line per request
    curl localhost:8099/open/partners                    # the one dialect with no auth wall
    curl -u demo-user localhost:8099/spring/partners     # the others: curl asks for the password

It listens on every interface, because the hub runs in Docker and reaches it at
http://host.docker.internal:8099 — `localhost` inside that container is the container itself,
and a server bound to loopback refuses the bridge. Run it on a machine you trust, or pass an
address to bind: `acme-api.py 8099 127.0.0.1` serves the shell you are in and nothing else.

ACME Components SA is fictional and so is every row: names, cities and credit limits are
generated from the row number, so two people running this file see identical data and can
compare screens. Nothing is read from disk.

The credentials are constants in a file anyone can read. They are here to make the auth
blocks of the manifests real, not to protect anything.
"""

import base64
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
HOST = sys.argv[2] if len(sys.argv) > 2 else "0.0.0.0"
BASIC = ("demo-user", "demo-pass")   # /spring, /offset, /hdr, /down, /flaky
BEARER = "demo-token"                # /drf, /v1
API_KEY = "demo-key"                 # /link, /graphql
API_VERSION = "2026-01"              # /hdr answers 400 without this header

# ── The data ──────────────────────────────────────────────────────────────────────────────
# Deterministic: partner N always has the same name, city and timestamp.
CITIES = [("Lyon", "FR"), ("Bruxelles", "BE"), ("Milano", "IT"), ("Porto", "PT"),
          ("Kraków", "PL"), ("Aarhus", "DK"), ("Valencia", "ES"), ("Bristol", "GB")]
TRADES = ["Hydraulique", "Roulements", "Fixations", "Pneumatique", "Étanchéité",
          "Transmission", "Outillage", "Levage", "Filtration", "Soudure"]
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def partner(n):
    city, country = CITIES[n % len(CITIES)]
    return {
        "id": 1000 + n,
        "name": f"{TRADES[n % len(TRADES)]} {city} {1000 + n}",
        "city": city,
        "country": country,
        "credit_limit": 500 + (n * 137) % 45_000,
        # Every partner has its own minute, so a watermark run reads a bounded, obvious slice.
        "updated_at": (EPOCH + timedelta(minutes=n)).isoformat().replace("+00:00", "Z"),
    }


PARTNERS = [partner(n) for n in range(2400)]


def slice_of(rows, start, size):
    return rows[start:start + size]


def since(rows, iso):
    """?updated_since= — the filter a `watermark` endpoint sends on its second run.

    Inclusive (>=), like most real APIs: an exclusive filter drops every row written in the
    same instant as the stored mark. The cost is that the boundary row comes back on every
    run, which is exactly what the connector's content hash is there to absorb.
    """
    return [r for r in rows if r["updated_at"] >= iso] if iso else rows


# ── The dialects ──────────────────────────────────────────────────────────────────────────
# Each returns (status, headers, body). `q` is the parsed query string, `rows` the (possibly
# watermark-filtered) collection, and `base` the absolute URL this request arrived on — read
# from the Host header, so the next-page links a source hands out are the ones the caller can
# actually follow (the hub sees `host.docker.internal`, a curl on the host sees `127.0.0.1`).

def d_spring(q, rows, url, base):
    """Spring Data `Page`: $.content + $.totalPages, page numbers start at 0."""
    size = int(q.get("size", ["50"])[0])
    page = int(q.get("page", ["0"])[0])
    total_pages = (len(rows) + size - 1) // size
    return 200, {}, {"content": slice_of(rows, page * size, size),
                     "totalPages": total_pages, "totalElements": len(rows),
                     "number": page, "last": page >= total_pages - 1}


def d_drf(q, rows, url, base):
    """Django REST `PageNumberPagination`: $.results + a whole next URL in $.next."""
    size = int(q.get("page_size", ["50"])[0])
    page = int(q.get("page", ["1"])[0])
    items = slice_of(rows, (page - 1) * size, size)
    more = page * size < len(rows)
    nxt = f"{base}{url.path}?page={page + 1}&page_size={size}" if more else None
    return 200, {}, {"count": len(rows), "next": nxt, "previous": None, "results": items}


def d_v1(q, rows, url, base):
    """Stripe-style opaque cursor: ?starting_after=<id>, $.data + $.has_more."""
    limit = int(q.get("limit", ["50"])[0])
    after = q.get("starting_after", [None])[0]
    start = 0
    if after is not None:
        start = next((i + 1 for i, r in enumerate(rows) if str(r["id"]) == after), 0)
    items = slice_of(rows, start, limit)
    return 200, {}, {"data": items, "has_more": start + limit < len(rows)}


def d_link(q, rows, url, base):
    """RFC 5988: the next page is a Link header, the body carries nothing about paging."""
    limit = int(q.get("limit", ["50"])[0])
    page = int(q.get("page", ["1"])[0])
    items = slice_of(rows, (page - 1) * limit, limit)
    headers = {}
    if page * limit < len(rows):
        nxt = f"{base}{url.path}?page={page + 1}&limit={limit}"
        headers["Link"] = f'<{nxt}>; rel="next"'
    return 200, headers, {"data": items}


def d_offset(q, rows, url, base):
    """Offset/limit with a published row count — the shape most legacy APIs export."""
    limit = int(q.get("limit", ["100"])[0])
    offset = int(q.get("offset", ["0"])[0])
    return 200, {}, {"data": slice_of(rows, offset, limit), "total": len(rows)}


def d_liar(q, rows, url, base):
    """Offset again, but the last 100 partners never come back while `$.total` still counts
    them. A source that loses rows this way ends its pagination like a complete one — no error,
    no short page the strategy can see — which is what `total_path` exists to catch."""
    status, headers, body = d_offset(q, rows[:-100], url, base)
    body["total"] = len(rows)
    return status, headers, body


DIALECTS = {"spring": d_spring, "drf": d_drf, "v1": d_v1, "link": d_link,
            "offset": d_offset, "open": d_offset, "hdr": d_offset,
            "down": d_offset, "flaky": d_offset, "liar": d_liar}
AUTH = {"spring": "basic", "offset": "basic", "hdr": "basic", "down": "basic",
        "flaky": "basic", "liar": "basic", "drf": "bearer", "v1": "bearer",
        "link": "api-key", "open": None}

# /flaky and its SOAP twin burn two attempts per page before answering. Reset them with
# /admin/reset — without that, a second run finds the pages already paid for and goes green
# having retried nothing.
FLAKY_SEEN = {}


# ── The SOAP services ─────────────────────────────────────────────────────────────────────
# SOAP is not an eleventh dialect: it is one POST door per service, the operation lives in the
# XML body, and the cursor is a ROW OFFSET woven into that body rather than a query parameter.
# Same 2 400 partners, in XML, under `POST /<service>/start.swe` — the path shape Siebel's EAI
# listener actually uses.
#
# Element names are TitleCase and every payload element is namespaced (`ns:`) on purpose: the
# connector strips prefixes before matching paths, and that strip is what these services test.

SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP_ACTION_PREFIX = "document/urn:acme:"   # /siebel answers 400 to any other SOAPAction
SESSION_TOKEN = "demo-session-42"           # what /session wants inside <soap:Header>
TAIL_ROWS = 201                             # /tail: 100 + 100 + ONE, so its last page holds one


def xml_escape(value):
    return (str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&apos;"))


def envelope(inner):
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            f'<soap:Envelope xmlns:soap="{SOAP_NS}">'
            f"<soap:Body>{inner}</soap:Body>"
            "</soap:Envelope>")


def fault(code, message):
    """A SOAP 1.1 Fault — the BUSINESS error channel, which must never be retried."""
    return envelope(f"<soap:Fault><faultcode>{code}</faultcode>"
                    f"<faultstring>{xml_escape(message)}</faultstring>"
                    "<detail><ErrorCode>SBL-DAT-00500</ErrorCode></detail></soap:Fault>")


def not_a_fault(message):
    """An infrastructure error dressed as XML but carrying NO Fault — status-based retry applies."""
    return envelope(f'<ns:Error xmlns:ns="urn:acme">{xml_escape(message)}</ns:Error>')


class SoapCtx:
    """One SOAP request, already resolved to a page window over the partners."""

    def __init__(self, handler, service, body, q):
        self.handler = handler
        self.service = service
        self.body = body
        self.page_size = int(self.tag("PageSize") or 100)
        self.start_row = int(self.tag("StartRowNum") or 0)
        self.rows = since(PARTNERS, q.get("updated_since", [None])[0])

    def tag(self, name):
        """Read one element out of the request body. As naive as the GraphQL parser next door:
        what is under test is the connector's request, not this server's XML skills."""
        m = re.search(rf"<(?:\w+:)?{name}>([^<]*)</(?:\w+:)?{name}>", self.body)
        return m.group(1) if m else None

    def query_page(self, rows=None, flag=None):
        """The QueryPage answer: this window's customers, plus at most one stop flag."""
        rows = self.rows if rows is None else rows
        window = slice_of(rows, self.start_row, self.page_size)
        last = self.start_row + len(window) >= len(rows)
        items = "".join(
            "<ns:Customer>"
            + "".join(f"<ns:{tag}>{xml_escape(value)}</ns:{tag}>" for tag, value in (
                ("Id", r["id"]), ("Name", r["name"]), ("City", r["city"]),
                # No types on the wire: every leaf is a string, and the promote ladder is what
                # turns these two into a decimal and a timestamptz.
                ("CreditLimit", r["credit_limit"]), ("UpdatedAt", r["updated_at"])))
            + "</ns:Customer>"
            for r in window)
        flags = ""
        if flag == "last_page":
            flags = f"<ns:LastPage>{'true' if last else 'false'}</ns:LastPage>"
        elif flag == "more_records":
            flags = f"<ns:MoreRecords>{'false' if last else 'true'}</ns:MoreRecords>"
        return 200, envelope(
            f'<ns:CustomerQueryPage_Output xmlns:ns="urn:acme">'
            f"<ns:ListOfCustomer>{items}</ns:ListOfCustomer>{flags}"
            f"<ns:StartRowNum>{self.start_row}</ns:StartRowNum>"
            f"</ns:CustomerQueryPage_Output>")


def s_siebel(c):
    """The canonical Siebel QueryPage: LastPage as the stop signal, SOAPAction demanded.

    The SOAPAction check is not decoration — `soap_action:` is a manifest field, and a service
    that never looked at it would let a manifest omitting it pass here and fail at the customer."""
    action = (c.handler.headers.get("SOAPAction") or "").strip('"')
    if not action.startswith(SOAP_ACTION_PREFIX):
        return 400, not_a_fault(f"SOAPAction must start with {SOAP_ACTION_PREFIX}, got: {action!r}")
    return c.query_page(flag="last_page")


def s_sap(c):
    """SAP PI style: a MoreRecords flag instead of LastPage — the has_more_path branch."""
    return c.query_page(flag="more_records")


def s_mute(c):
    """Announces nothing at all. The only way out is the short-page fallback."""
    return c.query_page()


def s_tail(c):
    """`siebel`'s shape over a TRUNCATED dataset whose last page holds exactly ONE element. XML
    has no arrays: one <Customer> is an object, fifty are a list. Without the connector's
    force-list that single partner is silently lost, and the row count is the proof."""
    return c.query_page(rows=c.rows[:TAIL_ROWS], flag="last_page")


def s_fault(c):
    """A Fault on HTTP 500 — the shape SOAP 1.1 prescribes. Must fail the run, unretried."""
    return 500, fault("soap:Server", "SBL-DAT-00500: no such Business Component 'Customer'")


def s_fault200(c):
    """The same Fault on HTTP 200 — plenty of gateways do this. An engine that trusts the status
    reads a body with no rows in it and reports a clean, empty, successful sync."""
    return 200, fault("soap:Client", "SBL-EXL-00151: the query cannot be parsed")


def s_flaky(c):
    """Two plain 500s per page window, carrying XML but NO Fault. The other error channel: this
    one is an infrastructure hiccup, and it must be retried to completion."""
    key = ("soap", c.service, c.start_row)
    n = FLAKY_SEEN.get(key, 0)
    FLAKY_SEEN[key] = n + 1
    if n < 2:
        return 500, not_a_fault(f"synthetic failure {n + 1}/2 at row {c.start_row}")
    return c.query_page(flag="last_page")


def s_session(c):
    """Wants a session token inside <soap:Header>, the way legacy EAI services do — which is what
    the endpoint's `header:` template and its {{env:NAME}} placeholder exist for."""
    if c.tag("SessionToken") != SESSION_TOKEN:
        return 401, fault("soap:Client", "SBL-SEC-00001: invalid or missing session token")
    return c.query_page(flag="last_page")


SOAP_SERVICES = {"siebel": s_siebel, "sap": s_sap, "mute": s_mute, "tail": s_tail,
                 "fault": s_fault, "fault200": s_fault200, "flaky": s_flaky,
                 "session": s_session}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # one line per request, on stdout
        sys.stdout.write("%s  %s\n" % (self.log_date_time_string(), fmt % args))
        sys.stdout.flush()

    # ── plumbing ──────────────────────────────────────────────────────────────────────────
    def send(self, status, body, headers=None):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def send_xml(self, status, body):
        payload = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def authorised(self, kind):
        got = self.headers.get("Authorization", "")
        if kind is None:
            return True
        if kind == "basic":
            want = base64.b64encode(":".join(BASIC).encode()).decode()
            return got == f"Basic {want}"
        if kind == "bearer":
            return got == f"Bearer {BEARER}"
        return self.headers.get("X-API-Key") == API_KEY

    # ── GET ───────────────────────────────────────────────────────────────────────────────
    def do_GET(self):
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]
        if parts == ["admin", "reset"]:
            FLAKY_SEEN.clear()
            return self.send(200, {"reset": "ok"})
        if len(parts) != 2 or parts[0] not in DIALECTS or parts[1] != "partners":
            return self.send(404, {"error": "unknown resource",
                                   "known": sorted(DIALECTS) , "resource": url.path})
        dialect = parts[0]
        if not self.authorised(AUTH[dialect]):
            return self.send(401, {"error": "unauthorized", "expected_auth": AUTH[dialect]})
        q = parse_qs(url.query)
        if dialect == "hdr" and self.headers.get("X-Api-Version") != API_VERSION:
            return self.send(400, {"error": "missing or wrong X-Api-Version",
                                   "expected": API_VERSION})
        if dialect == "down":
            return self.send(500, {"error": "this endpoint is down and stays down"})
        if dialect == "flaky":
            n = FLAKY_SEEN.get(url.query, 0)
            FLAKY_SEEN[url.query] = n + 1
            if n == 0:
                return self.send(429, {"error": "slow down"}, {"Retry-After": "1"})
            if n == 1:
                return self.send(503, {"error": "upstream hiccup"})
        rows = since(PARTNERS, q.get("updated_since", [None])[0])
        base = f"http://{self.headers.get('Host', f'127.0.0.1:{PORT}')}"
        status, headers, body = DIALECTS[dialect](q, rows, url, base)
        self.send(status, body, headers)

    # ── POST /graphql ─────────────────────────────────────────────────────────────────────
    # Not a GraphQL engine: it reads `first:` and the cursor out of the query text with a
    # regular expression. What is under test is the connector's Relay paging, not this parser.
    def do_POST(self):
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]
        if len(parts) == 2 and parts[0] in SOAP_SERVICES and parts[1] == "start.swe":
            return self.do_soap(parts[0], url)
        if url.path != "/graphql":
            return self.send(404, {"error": "POST is /graphql or /<service>/start.swe",
                                   "soap_services": sorted(SOAP_SERVICES)})
        if not self.authorised("api-key"):
            return self.send(401, {"error": "unauthorized", "expected_auth": "api-key"})
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")) or 0)
        try:
            req = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return self.send(400, {"error": "body is not JSON"})
        query, variables = req.get("query", ""), req.get("variables") or {}
        # Two deliberate misbehaviours the connector must refuse — both on HTTP 200.
        if "boom" in query:
            return self.send(200, {"errors": [{"message": "Cannot query field 'boom' on type 'Query'"}]})
        if "nullData" in query:
            return self.send(200, {"data": None})
        first = int(next(iter(re.findall(r"first:\s*(\d+)", query)), variables.get("first", 50)))
        after = variables.get("after")
        # Relay cursors are opaque by contract: base64 here, so a manifest that tries to build
        # one instead of echoing back what pageInfo handed it fails where a real API would.
        start = int(base64.b64decode(after)) + 1 if after else 0
        items = slice_of(PARTNERS, start, first)
        edges = [{"cursor": base64.b64encode(str(start + i).encode()).decode(), "node": r}
                 for i, r in enumerate(items)]
        return self.send(200, {"data": {"partners": {
            "edges": edges,
            "pageInfo": {"hasNextPage": start + first < len(PARTNERS),
                         "endCursor": edges[-1]["cursor"] if edges else None}}}})

    # ── POST /<service>/start.swe — the SOAP door ──────────────────────────────────────────
    # Transport auth is HTTP Basic, like the GET dialects: a session token inside <soap:Header>
    # is a SECOND credential, and only the `session` service asks for one.
    def do_soap(self, service, url):
        if not self.authorised("basic"):
            return self.send_xml(401, fault("soap:Client", "SBL-SEC-00001: unauthorized"))
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")) or 0).decode()
        status, payload = SOAP_SERVICES[service](
            SoapCtx(self, service, body, parse_qs(url.query)))
        self.send_xml(status, payload)


if __name__ == "__main__":
    print(f"ACME Partners API on http://{HOST}:{PORT} — {len(PARTNERS)} partners, "
          f"dialects: {', '.join(sorted(DIALECTS))} + POST /graphql\n"
          f"SOAP services (POST /<service>/start.swe): {', '.join(sorted(SOAP_SERVICES))}",
          flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
