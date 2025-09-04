import os, re, asyncio, time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import streamlit as st
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
import phonenumbers
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ──────────────────────────── APP CONFIG ────────────────────────────
st.set_page_config(page_title="AC Scraper", layout="wide", page_icon="🅰️")

# ──────────────────────────── AUTH ────────────────────────────
USERNAME = st.secrets.get("USERNAME", "abhishekcreations")
PASSWORD = st.secrets.get("PASSWORD", "ac2006")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def auth_gate():
    if not st.session_state.logged_in:
        st.title("🔒 Login")
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Login"):
            if u.strip() == USERNAME and p.strip() == PASSWORD:
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.stop()

auth_gate()

# ──────────────────────────── HEADER / BRAND ────────────────────────────
st.markdown("<h2 style='text-align:center;margin:0'>AC Scraper</h2>", unsafe_allow_html=True)
st.markdown("<div style='text-align:center;color:#666;margin:4px 0 10px'>Powered by Abhishek Creations</div>", unsafe_allow_html=True)
st.markdown("---")

# ──────────────────────────── DB (SQLite by default) ────────────────────────────
def get_engine() -> Engine:
    db_url = st.secrets.get("DATABASE_URL", "").strip()
    if db_url:
        return create_engine(db_url, pool_pre_ping=True, future=True)
    return create_engine("sqlite:///ac_scraper.db", future=True)

engine = get_engine()

def init_db():
    with engine.begin() as conn:
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS scrapes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            description TEXT NOT NULL,
            tool_type TEXT NOT NULL
        )
        """)
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_id INTEGER NOT NULL,
            company TEXT,
            website TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            source TEXT,
            FOREIGN KEY (scrape_id) REFERENCES scrapes(id)
        )
        """)
init_db()

def create_scrape_batch(description: str, tool_type: str) -> int:
    now = datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO scrapes (created_at, year, month, description, tool_type) VALUES (:ts,:y,:m,:d,:t)"),
            {"ts": now, "y": now.year, "m": now.month, "d": description.strip(), "t": tool_type}
        )
        # Portable last inserted id
        if engine.url.get_backend_name().startswith("sqlite"):
            return int(conn.execute(text("SELECT last_insert_rowid()")).scalar())
        else:
            return int(conn.execute(text("SELECT MAX(id) FROM scrapes")).scalar())

def save_records(scrape_id: int, df: pd.DataFrame):
    cols = ["company","website","email","phone","address","source"]
    for c in cols:
        if c not in df.columns: df[c] = ""
    rows = df[cols].to_dict(orient="records")
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO records (scrape_id, company, website, email, phone, address, source)
            VALUES (:scrape_id, :company, :website, :email, :phone, :address, :source)
        """), [{"scrape_id": scrape_id, **r} for r in rows])

# ──────────────────────────── HELPERS ────────────────────────────
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
OBFUSCATED_RE = re.compile(
    r"([A-Za-z0-9._%+-]+)\s*(?:\[at\]|\(at\)| at |\s@\s|@)\s*([A-Za-z0-9.-]+)\s*(?:\[dot\]|\(dot\)| dot |\s\.\s|\.)\s*([A-Za-z]{2,})",
    re.I
)
PHONE_RE = re.compile(r"(\+?\d[\d\-\s().]{6,}\d)")
CONTACT_HINTS = ["contact", "kontakt", "impressum", "about", "contact-us", "get-in-touch"]

def normalize_url(url: str):
    if not url: return None
    url = url.strip()
    if url.startswith("//"): url = "http:" + url
    if not urlparse(url).scheme: url = "http://" + url
    return url if urlparse(url).netloc else None

def clean_visible_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe"]): tag.decompose()
    return soup.get_text(separator="\n", strip=True)

def extract_emails(text: str):
    if not text: return []
    found = set(m.strip() for m in EMAIL_RE.findall(text))
    for m in OBFUSCATED_RE.findall(text):
        local, domain, tld = m
        found.add(f"{local}@{domain}.{tld}")
    # prefer non no-reply first later when we sort/emit
    return sorted(found)

def extract_phones(text: str):
    out = set()
    for raw in PHONE_RE.findall(text or ""):
        for region in ("IN", "US", "GB", "DE", "AE"):
            try:
                parsed = phonenumbers.parse(raw, region)
                if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed):
                    out.add(phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
                    break
            except Exception:
                pass
    return sorted(out)

def extract_address_lines(text: str):
    if not text: return []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    keys = ("address", "head office", "registered office", "location", "office", "impressum")
    out = []
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k in low for k in keys) and ("," in ln or any(ch.isdigit() for ch in ln)):
            snippet = ln
            if i+1 < len(lines) and ("," in lines[i+1] or any(ch.isdigit() for ch in lines[i+1])):
                snippet += " " + lines[i+1]
            out.append(" ".join(snippet.split()))
    # keep at most 3, unique, clipped
    uniq, seen = [], set()
    for s in out:
        s2 = s[:300]
        if s2 not in seen:
            uniq.append(s2); seen.add(s2)
        if len(uniq) >= 3: break
    return uniq

def find_contact_links(base: str, html: str):
    soup = BeautifulSoup(html or "", "html.parser")
    links = []
    # obvious links in page
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text() or "").lower()
        if any(k in href.lower() for k in CONTACT_HINTS) or any(k in text for k in CONTACT_HINTS):
            links.append(urljoin(base, href))
    # heuristic direct guesses
    for path in ("/contact", "/contact-us", "/about", "/impressum", "/kontakt"):
        links.append(urljoin(base, path))
    # unique
    seen, uniq = set(), []
    for u in links:
        if u not in seen:
            uniq.append(u); seen.add(u)
        if len(uniq) >= 6: break
    return uniq

async def fetch(session: aiohttp.ClientSession, url: str, timeout_s: int, tries: int):
    last_url = url
    for attempt in range(1, tries+1):
        try:
            async with asyncio.timeout(timeout_s):
                async with session.get(url, allow_redirects=True) as resp:
                    txt = await resp.text(errors="ignore")
                    last_url = str(resp.url)
                    return txt, last_url
        except Exception:
            await asyncio.sleep(0.4 * attempt)
    return None, last_url

def prefer_non_noreply(emails):
    # order: non-noreply first
    return sorted(emails, key=lambda e: ("noreply" in e.lower() or "no-reply" in e.lower(), e))

# ──────────────────────────── COMPANY→SITE SEARCH (via APIs) ────────────────────────────
async def search_official_site(session, company: str, timeout_s=10):
    gkey = st.secrets.get("GOOGLE_API_KEY"); gcx = st.secrets.get("GOOGLE_CX")
    bkey = st.secrets.get("BING_API_KEY");  skey = st.secrets.get("SERPAPI_KEY")

    # Google CSE
    if gkey and gcx:
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {"key": gkey, "cx": gcx, "q": company, "num": 5}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params) as r:
                    data = await r.json()
                    items = (data or {}).get("items", [])
                    for it in items:
                        link = it.get("link")
                        if link and is_homeish(link): return link
                    if items: return items[0].get("link")
        except Exception:
            pass

    # Bing Web
    if bkey:
        try:
            url = "https://api.bing.microsoft.com/v7.0/search"
            headers = {"Ocp-Apim-Subscription-Key": bkey}
            params = {"q": company, "count": 5, "responseFilter": "Webpages"}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params, headers=headers) as r:
                    data = await r.json()
                    web = (data or {}).get("webPages", {}).get("value", [])
                    for it in web:
                        link = it.get("url")
                        if link and is_homeish(link): return link
                    if web: return web[0].get("url")
        except Exception:
            pass

    # SerpAPI Google
    if skey:
        try:
            url = "https://serpapi.com/search.json"
            params = {"engine":"google", "q": company, "api_key": skey}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params) as r:
                    data = await r.json()
                    org = (data or {}).get("organic_results", [])
                    for it in org:
                        link = it.get("link")
                        if link and is_homeish(link): return link
                    if org: return org[0].get("link")
        except Exception:
            pass

    return None

def is_homeish(url: str) -> bool:
    try:
        p = urlparse(url)
        if not p.netloc: return False
        bad = ("linkedin.com","facebook.com","twitter.com","instagram.com","crunchbase.com","wikipedia.org","youtube.com")
        if any(b in p.netloc for b in bad): return False
        depth = p.path.strip("/").count("/")
        return depth <= 1
    except Exception:
        return False

# ──────────────────────────── SCRAPE PIPELINES ────────────────────────────
async def scrape_site_for_emails(session, base_url, opts):
    html, final = await fetch(session, base_url, opts["timeout"], opts["tries"])
    if not html: return [], [], [], final

    visible = clean_visible_text(html)
    emails = extract_emails(visible)
    phones = extract_phones(visible)
    addrs  = extract_address_lines(visible)

    # Fallback: contact pages
    sources = {}
    for e in emails: sources.setdefault(e, set()).add(final)
    if (not emails) and opts.get("follow_contacts"):
        for c in find_contact_links(final, html):
            chtml, cfin = await fetch(session, c, opts["timeout"], opts["tries"])
            if not chtml: continue
            cvis = clean_visible_text(chtml)
            for e in extract_emails(cvis): sources.setdefault(e, set()).add(cfin)
            for p in extract_phones(cvis): sources.setdefault(f"PHONE::{p}", set()).add(cfin)
            for a in extract_address_lines(cvis): sources.setdefault(f"ADDR::{a}", set()).add(cfin)

    # Consolidate
    emails2 = sorted([k for k in sources if not k.startswith(("PHONE::","ADDR::"))], key=lambda e: ("noreply" in e.lower(), e))
    phones2 = sorted([k.split("::",1)[1] for k in sources if k.startswith("PHONE::")]) or phones
    addrs2  = sorted([k.split("::",1)[1] for k in sources if k.startswith("ADDR::")])  or addrs

    return emails2, phones2, addrs2, final

async def process_row_website(session, sem, row, opts):
    async with sem:
        company = (row.get("company") or row.get("Company") or row.get("name") or "").strip()
        raw_site = (row.get("website") or row.get("Website") or row.get("site") or "").strip()
        failed = {"company": company, "website": raw_site, "notes": ""}

        url = normalize_url(raw_site)
        if not url:
            failed["notes"] = "invalid-url"
            return [], failed

        emails, phones, addrs, final = await scrape_site_for_emails(session, url, opts)
        if not emails:
            failed["notes"] = "no-emails-found"
            return [], failed

        out = []
        for e in prefer_non_noreply(emails):
            out.append({
                "company": company, "website": final, "email": e,
                "phone": "; ".join(phones)[:200],
                "address": "; ".join(addrs)[:300],
                "source": final
            })
        return out, None

async def process_company(session, sem, company, opts):
    async with sem:
        homepage = await search_official_site(session, company, timeout_s=opts["timeout"])
        if not homepage:
            return [], {"company": company, "website": "", "notes": "no-search-result"}
        emails, phones, addrs, final = await scrape_site_for_emails(session, homepage, opts)
        if not emails:
            return [], {"company": company, "website": final, "notes": "no-emails-found"}
        out = []
        for e in prefer_non_noreply(emails):
            out.append({
                "company": company, "website": final, "email": e,
                "phone": "; ".join(phones)[:200],
                "address": "; ".join(addrs)[:300],
                "source": final
            })
        return out, None

async def run_websites(rows, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 8)
    connector = aiohttp.TCPConnector(limit_per_host=opts["concurrency"], ssl=False)
    sem = asyncio.Semaphore(opts["concurrency"])
    results, failed = [], []
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True, headers={"User-Agent": "AC-Scraper/1.0"}) as session:
        tasks = [process_row_website(session, sem, r, opts) for r in rows]
        total, done = len(tasks), 0
        for fut in asyncio.as_completed(tasks):
            res, fail = await fut
            if res: results.extend(res)
            if fail: failed.append(fail)
            done += 1
            if cb: cb(done, total)
    return results, failed

async def run_companies(names, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 8)
    connector = aiohttp.TCPConnector(limit_per_host=opts["concurrency"], ssl=False)
    sem = asyncio.Semaphore(opts["concurrency"])
    results, failed = [], []
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True, headers={"User-Agent": "AC-Scraper/1.0"}) as session:
        tasks = [process_company(session, sem, nm, opts) for nm in names]
        total, done = len(tasks), 0
        for fut in asyncio.as_completed(tasks):
            res, fail = await fut
            if res: results.extend(res)
            if fail: failed.append(fail)
            done += 1
            if cb: cb(done, total)
    return results, failed

# Chunked runner (to handle 700+ lists without API throttling)
def chunk(items, size):
    for i in range(0, len(items), size):
        yield items[i:i+size], i, min(i+size, len(items))

def run_chunked_websites(df_rows, opts, chunk_size, sleep_s, logger, pbar):
    all_results, all_failed = [], []
    total = len(df_rows); processed = 0
    for block, start, end in chunk(df_rows, chunk_size):
        logger.info(f"Chunk {start+1}-{end} / {total}…")
        def cb(done, tot):
            # progress inside chunk
            p = int(((processed + done) / total) * 100)
            pbar.progress(min(p, 100))
        res, fail = asyncio.run(run_websites(block, opts, cb=cb))
        all_results.extend(res); all_failed.extend(fail)
        processed += len(block)
        pbar.progress(int(processed / total * 100))
        time.sleep(sleep_s)
    return all_results, all_failed

def run_chunked_companies(names, opts, chunk_size, sleep_s, logger, pbar):
    all_results, all_failed = [], []
    total = len(names); processed = 0
    for block, start, end in chunk(names, chunk_size):
        logger.info(f"Chunk {start+1}-{end} / {total}…")
        def cb(done, tot):
            p = int(((processed + done) / total) * 100)
            pbar.progress(min(p, 100))
        res, fail = asyncio.run(run_companies(block, opts, cb=cb))
        all_results.extend(res); all_failed.extend(fail)
        processed += len(block)
        pbar.progress(int(processed / total * 100))
        time.sleep(sleep_s)
    return all_results, all_failed

# ──────────────────────────── UI: SIDEBAR NAV + KEY CHECK ────────────────────────────
tab = st.sidebar.radio("Navigation", ["Scraper (Websites CSV)", "Company → Contact Finder", "History"], index=0)
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔑 Key Check")
st.sidebar.write(f"Google CSE: {'✅' if st.secrets.get('GOOGLE_API_KEY') and st.secrets.get('GOOGLE_CX') else '—'}")
st.sidebar.write(f"Bing Search: {'✅' if st.secrets.get('BING_API_KEY') else '—'}")
st.sidebar.write(f"DB: {'✅' if st.secrets.get('DATABASE_URL','').strip() else 'SQLite (local)'}")
st.sidebar.markdown("---")
st.sidebar.caption("Abhishek Creations © 2025")

# ──────────────────────────── UI: WEBSITES CSV ────────────────────────────
if tab.startswith("Scraper"):
    st.title("📧 Email Scraper (Websites CSV)")
    description = st.text_input("Batch description (required before scraping)", placeholder="e.g., SMM Hamburg exhibitors Aug 2025")
    uploaded = st.file_uploader("Upload CSV/XLSX (columns: company, website)", type=["csv","xlsx","xls"])

    colA, colB, colC, colD = st.columns(4)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages", value=True)
    with colB: concurrency = st.slider("Concurrency", 2, 60, 12)
    with colC: timeout = st.slider("Timeout (sec)", 6, 30, 12)
    with colD: tries = st.slider("Retries", 1, 4, 2)

    colE, colF = st.columns(2)
    with colE: chunk_size = st.slider("Chunk size (large lists)", 10, 200, 40, step=10)
    with colF: pause_between = st.slider("Pause between chunks (sec)", 0, 15, 3)

    if uploaded:
        try:
            if uploaded.name.lower().endswith((".xls", ".xlsx")):
                df = pd.read_excel(uploaded).fillna("")
            else:
                df = pd.read_csv(uploaded).fillna("")
        except Exception as e:
            st.error(f"Failed to read file: {e}"); st.stop()

        if not {"company","website"}.intersection(set(map(str.lower, df.columns))):
            st.warning("Your file must have columns for company and website (case-insensitive).")
            st.stop()

        st.info(f"Loaded {len(df)} rows. Add a description and click **Start**.")
        start = st.button("Start", type="primary", disabled=not description.strip())

        if start:
            scrape_id = create_scrape_batch(description.strip(), "websites_csv")
            rows = [{k:str(v) for k,v in r.items()} for _, r in df.iterrows()]
            progress_bar = st.progress(0)
            log_box = st.empty(); logs = []

            class Logger:
                def info(self, msg): 
                    logs.append(msg); log_box.code("\n".join(logs[-12:]))

            opts = {"follow_contacts": follow_contacts, "concurrency": concurrency, "timeout": timeout, "tries": tries}

            results, failed = run_chunked_websites(rows, opts, chunk_size, pause_between, Logger(), progress_bar)

            if results:
                out = pd.DataFrame(results).drop_duplicates(subset=["company","website","email"])
                st.success(f"Found {len(out)} email rows.")
                st.dataframe(out.head(25))
                save_records(scrape_id, out)
                st.download_button("Download results CSV", out.to_csv(index=False).encode("utf-8"),
                                   file_name=f"emails_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
            if failed:
                miss = pd.DataFrame(failed)
                st.markdown("### No-email / failed")
                st.dataframe(miss.head(25))
                st.download_button("Download failed CSV", miss.to_csv(index=False).encode("utf-8"),
                                   file_name=f"failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")

# ──────────────────────────── UI: COMPANY → CONTACT FINDER ────────────────────────────
elif tab.startswith("Company"):
    st.title("🏢 Company → Contact Finder")

    has_provider = bool(st.secrets.get("GOOGLE_API_KEY") and st.secrets.get("GOOGLE_CX")) \
                   or bool(st.secrets.get("BING_API_KEY")) \
                   or bool(st.secrets.get("SERPAPI_KEY"))
    if not has_provider:
        st.warning("Add a search provider key in Secrets (Google CSE or Bing or SerpAPI) to enable this tool.")
        st.stop()

    description = st.text_input("Batch description (required)", placeholder="e.g., Marine OEMs shortlist Sept 2025")
    txt = st.text_area("Companies (one per line)", height=200, placeholder="Bosch\nSony\nApple")

    colA, colB, colC = st.columns(3)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages", value=True)
    with colB: concurrency = st.slider("Concurrency", 2, 40, 10)
    with colC: timeout = st.slider("Timeout (sec)", 6, 30, 12)
    tries = st.slider("Retries", 1, 4, 2)

    colE, colF = st.columns(2)
    with colE: chunk_size = st.slider("Chunk size (large lists)", 10, 200, 30, step=10)
    with colF: pause_between = st.slider("Pause between chunks (sec)", 0, 15, 3)

    if st.button("Start", type="primary", disabled=not (txt.strip() and description.strip())):
        companies = [c.strip() for c in txt.splitlines() if c.strip()]
        progress_bar = st.progress(0)
        log_box = st.empty(); logs = []

        class Logger:
            def info(self, msg):
                logs.append(msg); log_box.code("\n".join(logs[-12:]))

        scrape_id = create_scrape_batch(description.strip(), "company_search")
        opts = {"follow_contacts": follow_contacts, "concurrency": concurrency, "timeout": timeout, "tries": tries}

        results, failed = run_chunked_companies(companies, opts, chunk_size, pause_between, Logger(), progress_bar)

        if results:
            df_out = pd.DataFrame(results).drop_duplicates(subset=["company","website","email"])
            st.success(f"Found {len(df_out)} email rows.")
            st.dataframe(df_out.head(25))
            save_records(scrape_id, df_out)
            st.download_button("Download contacts CSV", df_out.to_csv(index=False).encode("utf-8"),
                               file_name=f"contacts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
        if failed:
            df_fail = pd.DataFrame(failed)
            st.markdown("### Not found / failed")
            st.dataframe(df_fail.head(25))
            st.download_button("Download failed CSV", df_fail.to_csv(index=False).encode("utf-8"),
                               file_name=f"company_failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")

# ──────────────────────────── UI: HISTORY ────────────────────────────
elif tab == "History":
    st.title("🗂️ Scrape History")

    with engine.begin() as conn:
        batches = conn.execute(text("""
            SELECT id, created_at, year, month, description, tool_type
            FROM scrapes ORDER BY created_at DESC
        """)).mappings().all()

    if not batches:
        st.info("No batches yet. Run a scrape first."); st.stop()

    dfb = pd.DataFrame(batches)
    dfb["created_at"] = pd.to_datetime(dfb["created_at"])

    st.dataframe(dfb[["id","created_at","year","month","tool_type","description"]], use_container_width=True)

    bid = st.selectbox("Download a batch by ID", dfb["id"].tolist())
    if st.button("Prepare CSV"):
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT company, website, email, phone, address, source
                FROM records WHERE scrape_id = :sid ORDER BY company
            """), {"sid": int(bid)}).mappings().all()
        if rows:
            dfr = pd.DataFrame(rows)
            st.success(f"{len(dfr)} rows.")
            st.dataframe(dfr.head(25))
            st.download_button("Download batch CSV", dfr.to_csv(index=False).encode("utf-8"),
                               file_name=f"batch_{bid}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
        else:
            st.info("No rows in that batch.")
