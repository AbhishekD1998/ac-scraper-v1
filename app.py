# app.py — AC Scraper (Bing-first search, Google fallback) + History + Diagnostics

import os, re, asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin

import streamlit as st
import pandas as pd
import aiohttp
import phonenumbers
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ------------------ App header ------------------
st.set_page_config(page_title="AC Scraper", layout="wide", page_icon="🅰️")
BRAND = "Abhishek Creations"
st.markdown("<h2 style='text-align:center;margin-bottom:0'>AC Scraper</h2>", unsafe_allow_html=True)
st.markdown(f"<div style='text-align:center;color:#666;margin-top:4px;'>Powered by {BRAND}</div>", unsafe_allow_html=True)
st.markdown("---")

# ------------------ Secrets ------------------
USERNAME = st.secrets.get("USERNAME", "abhishekcreations")
PASSWORD = st.secrets.get("PASSWORD", "ac2006")
BING_API_KEY   = (st.secrets.get("BING_API_KEY")   or "").strip()
GOOGLE_API_KEY = (st.secrets.get("GOOGLE_API_KEY") or "").strip()
GOOGLE_CX      = (st.secrets.get("GOOGLE_CX")      or "").strip()
SERPAPI_KEY    = (st.secrets.get("SERPAPI_KEY")    or "").strip()
DATABASE_URL   = (st.secrets.get("DATABASE_URL")   or "").strip()

# ------------------ Login ------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if not st.session_state.logged_in:
    st.title("🔒 Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Login", type="primary"):
        if u.strip() == USERNAME and p.strip() == PASSWORD:
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Invalid credentials")
    st.stop()

# ------------------ Nav ------------------
tab = st.sidebar.radio("Navigation", ["Scraper (Websites CSV)", "Company → Contact Finder", "History", "🧪 Diagnostics"], index=0)
st.sidebar.markdown("---")
st.sidebar.caption("Abhishek Creations © 2025")

# ------------------ DB ------------------
def get_engine() -> Engine:
    if DATABASE_URL:
        return create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
    return create_engine("sqlite:///ac_scraper.db", future=True)

engine = get_engine()

def init_db():
    with engine.begin() as c:
        c.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS scrapes(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TIMESTAMP NOT NULL,
              year INTEGER NOT NULL,
              month INTEGER NOT NULL,
              description TEXT NOT NULL,
              tool_type TEXT NOT NULL
            )
        """)
        c.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS records(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scrape_id INTEGER NOT NULL,
              company TEXT, website TEXT, email TEXT,
              phone TEXT, address TEXT, source TEXT,
              FOREIGN KEY (scrape_id) REFERENCES scrapes(id)
            )
        """)

init_db()

def create_scrape_batch(description: str, tool_type: str) -> int:
    now = datetime.utcnow()
    with engine.begin() as c:
        c.execute(text("""
            INSERT INTO scrapes(created_at,year,month,description,tool_type)
            VALUES(:ts,:y,:m,:d,:t)
        """), {"ts": now, "y": now.year, "m": now.month, "d": description.strip(), "t": tool_type})
        if engine.url.get_backend_name().startswith("sqlite"):
            sid = c.execute(text("SELECT last_insert_rowid()")).scalar()
        else:
            sid = c.execute(text("SELECT LASTVAL()")).scalar()
    return int(sid)

def save_records(scrape_id: int, df: pd.DataFrame):
    cols = ["company","website","email","phone","address","source"]
    for x in cols:
        if x not in df.columns:
            df[x] = ""
    rows = df[cols].to_dict(orient="records")
    if not rows: return
    with engine.begin() as c:
        c.execute(text("""
            INSERT INTO records(scrape_id,company,website,email,phone,address,source)
            VALUES(:scrape_id,:company,:website,:email,:phone,:address,:source)
        """), [{"scrape_id": scrape_id, **r} for r in rows])

# ------------------ Helpers ------------------
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
OBFUSCATED_RE = re.compile(
    r"([A-Za-z0-9._%+-]+)\s*(?:\[at\]|\(at\)| at |\s@\s|@)\s*([A-Za-z0-9.-]+)\s*(?:\[dot\]|\(dot\)| dot |\s\.\s|\.)\s*([A-Za-z]{2,})",
    re.I
)
CONTACT_KEYS = ["contact","kontakt","impressum","about","contact-us","get-in-touch","kontak","contato"]
PHONE_HINT = re.compile(r"(\+?\d[\d\-\s().]{6,}\d)")

def normalize_url(url: str):
    if not url: return None
    u = url.strip()
    if u.startswith("//"): u = "http:" + u
    if not urlparse(u).scheme: u = "http://" + u
    return u if urlparse(u).netloc else None

def clean_visible_text(html: str):
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(["script","style","noscript","iframe"]): t.decompose()
    return soup.get_text(separator="\n", strip=True)

def extract_emails(text: str):
    found = set(EMAIL_RE.findall(text or ""))
    for m in OBFUSCATED_RE.findall(text or ""):
        found.add(f"{m[0]}@{m[1]}.{m[2]}")
    return sorted(found)

def extract_phones(text: str):
    out = set()
    for raw in PHONE_HINT.findall(text or ""):
        try:
            for region in ("US","IN","GB","DE","AE"):
                p = phonenumbers.parse(raw, region)
                if phonenumbers.is_possible_number(p) and phonenumbers.is_valid_number(p):
                    out.add(phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
                    break
        except Exception:
            pass
    return sorted(out)

def extract_address_snippets(text: str):
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    keys = ("address","head office","registered office","location","office","impressum")
    seen, out = set(), []
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k in low for k in keys) and ("," in ln or any(ch.isdigit() for ch in ln)):
            snip = ln
            if i+1 < len(lines) and ("," in lines[i+1] or any(ch.isdigit() for ch in lines[i+1])):
                snip += " " + lines[i+1]
            snip = " ".join(snip.split())[:300]
            if snip and snip not in seen:
                seen.add(snip)
                out.append(snip)
    return out[:3]

async def fetch(session: aiohttp.ClientSession, url: str, timeout_s: int, tries: int):
    for attempt in range(1, tries+1):
        try:
            async with asyncio.timeout(timeout_s):
                async with session.get(url, allow_redirects=True) as r:
                    txt = await r.text(errors="ignore")
                    return txt, str(r.url), r.status
        except Exception:
            await asyncio.sleep(0.5 * attempt)
    return None, url, None

def find_contact_links(base: str, html: str):
    soup = BeautifulSoup(html or "", "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text() or "").lower()
        if any(k in href.lower() for k in CONTACT_KEYS) or any(k in text for k in CONTACT_KEYS):
            links.append(urljoin(base, href))
    return list(dict.fromkeys(links))[:6]

def is_probably_homepage(url: str) -> bool:
    try:
        p = urlparse(url)
        if not p.netloc: return False
        bad = ("linkedin.com","facebook.com","twitter.com","instagram.com","crunchbase.com","wikipedia.org")
        if any(b in p.netloc for b in bad): return False
        depth = p.path.strip("/").count("/")
        return depth <= 1
    except Exception:
        return False

# ------------------ Search (Bing → Google → SerpAPI) ------------------
async def search_official_site(session, company: str, timeout_s=10):
    # 1) Bing first (much better free quota)
    if BING_API_KEY:
        try:
            async with asyncio.timeout(timeout_s):
                r = await session.get(
                    "https://api.bing.microsoft.com/v7.0/search",
                    headers={"Ocp-Apim-Subscription-Key": BING_API_KEY},
                    params={"q": company, "count": 5, "responseFilter": "Webpages"}
                )
                if r.status == 200:
                    data = await r.json()
                    web = (data or {}).get("webPages", {}).get("value", [])
                    for it in web:
                        link = it.get("url")
                        if link and is_probably_homepage(link): return link
                    if web: return web[0].get("url")
        except Exception:
            pass  # fall through

    # 2) Google CSE (may be 429 if over quota)
    if GOOGLE_API_KEY and GOOGLE_CX:
        try:
            async with asyncio.timeout(timeout_s):
                r = await session.get(
                    "https://www.googleapis.com/customsearch/v1",
                    params={"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": company, "num": 3}
                )
                if r.status == 200:
                    data = await r.json()
                    items = (data or {}).get("items", [])
                    for it in items:
                        link = it.get("link")
                        if link and is_probably_homepage(link): return link
                    if items: return items[0].get("link")
        except Exception:
            pass

    # 3) SerpAPI (optional)
    if SERPAPI_KEY:
        try:
            async with asyncio.timeout(timeout_s):
                r = await session.get(
                    "https://serpapi.com/search.json",
                    params={"engine": "google", "q": company, "api_key": SERPAPI_KEY}
                )
                if r.status == 200:
                    data = await r.json()
                    org = (data or {}).get("organic_results", [])
                    for it in org:
                        link = it.get("link")
                        if link and is_probably_homepage(link): return link
                    if org: return org[0].get("link")
        except Exception:
            pass

    return None

# ------------------ Pipelines ------------------
async def process_row_website(session, sem, row, opts):
    async with sem:
        company = (row.get("company") or row.get("Company") or row.get("name") or "").strip()
        site = (row.get("website") or row.get("Website") or row.get("site") or "").strip()
        res, fail = [], {"company": company, "website": site, "notes": ""}

        if not site:
            fail["notes"] = "no-website"; return res, fail
        url = normalize_url(site)
        if not url:
            fail["notes"] = "invalid-url"; return res, fail

        html, final, status = await fetch(session, url, opts["timeout"], opts["tries"])
        if not html or (status and status >= 400):
            fail["notes"] = f"fetch-failed({status})"; return res, fail

        visible = clean_visible_text(html)
        emails = extract_emails(visible)
        sources = {}

        if not emails and opts.get("follow_contacts"):
            for c in find_contact_links(final, html):
                ctext, cfinal, stc = await fetch(session, c, opts["timeout"], opts["tries"])
                if not ctext or (stc and stc >= 400): continue
                for e in extract_emails(clean_visible_text(ctext)):
                    sources.setdefault(e, set()).add(cfinal)
        for e in emails:
            sources.setdefault(e, set()).add(final)

        all_emails = sorted(sources.keys())
        if not all_emails:
            fail["notes"] = "no-emails-found"; return res, fail

        for e in all_emails:
            res.append({"company": company, "website": final, "email": e, "source": ",".join(sorted(sources.get(e, {final})))})
        return res, None

async def run_all_website(rows, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 6)
    connector = aiohttp.TCPConnector(limit_per_host=max(4, opts["concurrency"]), ssl=False)
    sem = asyncio.Semaphore(opts["concurrency"])
    results, failed = [], []
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True,
                                     headers={"User-Agent":"Mozilla/5.0 AC-Scraper/1.0"}) as session:
        tasks = [process_row_website(session, sem, r, opts) for r in rows]
        total, done = len(tasks), 0
        for fut in asyncio.as_completed(tasks):
            r, f = await fut
            if r: results.extend(r)
            if f: failed.append(f)
            done += 1
            if cb: cb(done, total)
    return results, failed

async def company_to_contacts(session, company, opts):
    homepage = await search_official_site(session, company, timeout_s=opts["timeout"])
    if not homepage:
        return [], {"company": company, "website": "", "notes": "no-search-result"}

    html, final, status = await fetch(session, homepage, opts["timeout"], opts["tries"])
    if not html or (status and status >= 400):
        return [], {"company": company, "website": homepage, "notes": f"homepage-fetch-failed({status})"}

    visible = clean_visible_text(html)
    emails = extract_emails(visible)
    phones = extract_phones(visible)
    addrs  = extract_address_snippets(visible)

    sources = {}
    if not emails and opts.get("follow_contacts"):
        for c in find_contact_links(final, html):
            ctext, cfinal, stc = await fetch(session, c, opts["timeout"], opts["tries"])
            if not ctext or (stc and stc >= 400): continue
            v = clean_visible_text(ctext)
            for e in extract_emails(v): sources.setdefault(e, set()).add(cfinal)
            for p in extract_phones(v): sources.setdefault(f"PHONE::{p}", set()).add(cfinal)
            for a in extract_address_snippets(v): sources.setdefault(f"ADDR::{a}", set()).add(cfinal)

    for e in emails: sources.setdefault(e, set()).add(final)
    for p in phones: sources.setdefault(f"PHONE::{p}", set()).add(final)
    for a in addrs:  sources.setdefault(f"ADDR::{a}", set()).add(final)

    email_list   = sorted([k for k in sources if not k.startswith(("PHONE::","ADDR::"))])
    phone_list   = sorted([k.split("::",1)[1] for k in sources if k.startswith("PHONE::")])
    address_list = sorted([k.split("::",1)[1] for k in sources if k.startswith("ADDR::")])

    rows = []
    if email_list:
        for e in email_list:
            rows.append({
                "company": company, "website": final, "email": e,
                "phone": "; ".join(phone_list)[:200],
                "address": "; ".join(address_list)[:300],
                "source": ",".join(sorted(sources.get(e, {final})))
            })
        return rows, None
    else:
        return [], {"company": company, "website": final, "notes": "no-emails-found"}

async def run_all_company(names, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 6)
    connector = aiohttp.TCPConnector(limit_per_host=max(4, opts["concurrency"]), ssl=False)
    results, failed = [], []
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True,
                                     headers={"User-Agent":"Mozilla/5.0 AC-Scraper/1.0"}) as session:
        tasks = [company_to_contacts(session, nm, opts) for nm in names]
        total, done = len(tasks), 0
        for fut in asyncio.as_completed(tasks):
            r, f = await fut
            if r: results.extend(r)
            if f: failed.append(f)
            done += 1
            if cb: cb(done, total)
    return results, failed

# ------------------ UI: Websites CSV ------------------
if tab.startswith("Scraper"):
    st.title("📧 Email Scraper (Websites CSV)")
    description = st.text_input("Batch description (required)", placeholder="e.g., SMM Hamburg exhibitors Aug 2025")
    uploaded = st.file_uploader("Upload CSV / XLSX (columns: company, website)", type=["csv","xlsx","xls"])
    colA, colB, colC = st.columns(3)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages (fallback)", value=True)
    with colB: concurrency     = st.slider("Concurrency", 2, 40, value=12)
    with colC: timeout         = st.slider("Timeout (sec)", 6, 25, value=12)
    tries = st.slider("Retries", 1, 4, value=2)

    if uploaded:
        try:
            if uploaded.name.lower().endswith((".xls",".xlsx")):
                df = pd.read_excel(uploaded).fillna("")
            else:
                df = pd.read_csv(uploaded).fillna("")
        except Exception as e:
            st.error(f"Failed to read file: {e}")
            st.stop()

        st.info(f"Loaded {len(df)} rows. Add a description and click Start.")
        start = st.button("Start Scraping", type="primary", disabled=not description.strip())
        progress_bar = st.progress(0); log = st.empty(); logs = []

        if start:
            scrape_id = create_scrape_batch(description.strip(), "websites_csv")
            rows = [{k:str(v) for k,v in r.items()} for _, r in df.iterrows()]
            opts = {"follow_contacts": follow_contacts, "concurrency": concurrency, "timeout": timeout, "tries": tries}

            def cb(done, total):
                progress_bar.progress(int(done/total*100))
                logs.append(f"Processed {done}/{total}")
                log.code("\n".join(logs[-12:]))

            results, failed = asyncio.run(run_all_website(rows, opts, cb=cb))

            if results:
                out = pd.DataFrame(results).drop_duplicates(subset=["company","website","email"])
                st.success(f"Found {len(out)} email rows.")
                st.dataframe(out.head(50), use_container_width=True)
                save_records(scrape_id, out)
                st.download_button("Download results CSV", out.to_csv(index=False).encode("utf-8"),
                                   file_name=f"emails_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
            if failed:
                miss = pd.DataFrame(failed)
                st.markdown("### No-email / failed")
                st.dataframe(miss.head(50), use_container_width=True)
                st.download_button("Download failed CSV", miss.to_csv(index=False).encode("utf-8"),
                                   file_name=f"failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")

# ------------------ UI: Company → Contact Finder ------------------
elif tab.startswith("Company"):
    st.title("🏢 Company → Contact Finder")
    has_provider = bool(BING_API_KEY or (GOOGLE_API_KEY and GOOGLE_CX) or SERPAPI_KEY)
    if not has_provider:
        st.warning("Add a search provider in Secrets (Bing recommended; Google CSE or SerpAPI also supported).")
        st.stop()

    description = st.text_input("Batch description (required)", placeholder="e.g., Marine OEMs shortlist Sept 2025")
    txt = st.text_area("Companies (one per line)", height=220, placeholder="Amphenol\nMurata Electronics\nBosch Rexroth\nKeysight Technologies India\nHoltek Semiconductor India")

    colA, colB, colC = st.columns(3)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages", value=True)
    with colB: concurrency     = st.slider("Concurrency", 2, 30, value=8)
    with colC: timeout         = st.slider("Timeout (sec)", 6, 25, value=12)
    tries = st.slider("Retries", 1, 4, value=2)

    if st.button("Start Finding", type="primary", disabled=not (txt.strip() and description.strip())):
        companies = [c.strip() for c in txt.splitlines() if c.strip()]
        st.info(f"Got {len(companies)} companies. Starting…")
        progress_bar = st.progress(0); log = st.empty(); logs = []
        scrape_id = create_scrape_batch(description.strip(), "company_search")

        def cb(done, total):
            progress_bar.progress(int(done/total*100))
            logs.append(f"Processed {done}/{total}")
            log.code("\n".join(logs[-12:]))

        results, failed = asyncio.run(
            run_all_company(companies,
                            {"timeout": timeout, "tries": tries, "follow_contacts": follow_contacts, "concurrency": concurrency},
                            cb=cb)
        )

        if results:
            df_out = pd.DataFrame(results).drop_duplicates(subset=["company","website","email"])
            st.success(f"Found {len(df_out)} email rows.")
            st.dataframe(df_out.head(50), use_container_width=True)
            save_records(scrape_id, df_out)
            st.download_button("Download contacts CSV", df_out.to_csv(index=False).encode("utf-8"),
                               file_name=f"contacts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
        if failed:
            df_fail = pd.DataFrame(failed)
            st.markdown("### Not found / failed")
            st.dataframe(df_fail.head(50), use_container_width=True)
            st.download_button("Download failed CSV", df_fail.to_csv(index=False).encode("utf-8"),
                               file_name=f"company_failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")

# ------------------ UI: History ------------------
elif tab == "History":
    st.title("🗂️ Scrape History")
    with engine.begin() as c:
        batches = c.execute(text("""
            SELECT id, created_at, year, month, description, tool_type
            FROM scrapes ORDER BY created_at DESC
        """)).mappings().all()

    if not batches:
        st.info("No batches yet.")
        st.stop()

    dfb = pd.DataFrame(batches)
    dfb["created_at"] = pd.to_datetime(dfb["created_at"])
    col1, col2 = st.columns([2,1])
    with col1:
        st.dataframe(dfb[["id","created_at","year","month","tool_type","description"]], use_container_width=True)
    with col2:
        years = ["All"] + sorted(dfb["year"].unique().tolist(), reverse=True)
        ysel  = st.selectbox("Year", years, index=0)
        months = ["All"] + sorted(dfb["month"].unique().tolist(), reverse=True)
        msel  = st.selectbox("Month", months, index=0)

    fdf = dfb.copy()
    if ysel != "All": fdf = fdf[fdf["year"] == ysel]
    if msel != "All": fdf = fdf[fdf["month"] == msel]

    st.markdown("### Download a batch")
    bids = fdf["id"].tolist()
    if not bids:
        st.info("No batches match filters.")
    else:
        bid = st.selectbox("Choose batch ID", bids, index=0)
        if st.button("Prepare CSV"):
            with engine.begin() as c:
                rows = c.execute(text("""
                    SELECT company, website, email, phone, address, source
                    FROM records WHERE scrape_id = :sid ORDER BY company
                """), {"sid": bid}).mappings().all()
            if rows:
                dfr = pd.DataFrame(rows)
                st.success(f"{len(dfr)} rows.")
                st.dataframe(dfr.head(50), use_container_width=True)
                st.download_button("Download batch CSV", dfr.to_csv(index=False).encode("utf-8"),
                                   file_name=f"batch_{bid}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
            else:
                st.info("No rows in this batch.")

# ------------------ UI: Diagnostics ------------------
elif tab == "🧪 Diagnostics":
    st.title("🧪 Diagnostics")
    st.write({
        "GOOGLE_API_KEY set?": bool(GOOGLE_API_KEY),
        "GOOGLE_CX set?": bool(GOOGLE_CX),
        "BING_API_KEY set?": bool(BING_API_KEY),
        "SERPAPI_KEY set?": bool(SERPAPI_KEY),
        "DATABASE_URL set?": bool(DATABASE_URL),
    })

    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("Test Bing"):
            async def t():
                if not BING_API_KEY:
                    st.error("Missing BING_API_KEY"); return
                async with aiohttp.ClientSession() as s:
                    r = await s.get("https://api.bing.microsoft.com/v7.0/search",
                                    headers={"Ocp-Apim-Subscription-Key": BING_API_KEY},
                                    params={"q":"Amphenol","count":1,"responseFilter":"Webpages"})
                    st.write("HTTP", r.status)
                    try: st.json(await r.json())
                    except Exception: st.code(await r.text())
            asyncio.run(t())

    with c2:
        if st.button("Test Google CSE"):
            async def t():
                if not (GOOGLE_API_KEY and GOOGLE_CX):
                    st.error("Missing GOOGLE_API_KEY or GOOGLE_CX"); return
                async with aiohttp.ClientSession() as s:
                    r = await s.get("https://www.googleapis.com/customsearch/v1",
                                    params={"key":GOOGLE_API_KEY,"cx":GOOGLE_CX,"q":"Amphenol","num":1})
                    st.write("HTTP", r.status)
                    try: st.json(await r.json())
                    except Exception: st.code(await r.text())
                    if r.status == 429:
                        st.warning("Google quota exceeded (429). Bing will be used automatically by the app.")
            asyncio.run(t())

    with c3:
        if st.button("Plain fetch test"):
            async def t():
                async with aiohttp.ClientSession() as s:
                    r = await s.get("https://www.murata.com",
                                    headers={"User-Agent":"Mozilla/5.0 AC-Scraper/1.0"})
                    st.write("HTTP", r.status, "Final URL:", str(r.url))
                    st.code((await r.text(errors="ignore"))[:1200])
            asyncio.run(t())

    st.markdown("---")
    err = st.text_area("Paste error here → click Explain", height=180)
    if st.button("Explain"):
        if not err.strip():
            st.info("Paste an error first.")
        else:
            hints = []
            if "429" in err or "rateLimitExceeded" in err:
                hints.append("• Google quota exceeded — the app already prefers Bing. Ensure BING_API_KEY is set.")
            if "No module named" in err:
                hints.append("• Missing dependency. Confirm it's in requirements.txt.")
            if "OperationalError" in err and "sqlalchemy" in err.lower():
                hints.append("• DB connection/permissions issue. SQLite is default; Postgres needs a valid DATABASE_URL.")
            if not hints:
                hints.append("• Try lower concurrency, confirm Secrets, and use the three test buttons above to isolate the problem.")
            st.markdown("\n".join(hints))
