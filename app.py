# app.py
import os, re, asyncio, json
from datetime import datetime
from urllib.parse import urlparse, urljoin

import streamlit as st
import pandas as pd
import aiohttp
import phonenumbers
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =================== APP CONFIG ===================
st.set_page_config(page_title="AC Scraper", layout="wide", page_icon="🅰️")
st.markdown("<h2 style='text-align:center;margin-bottom:0'>AC Scraper</h2>", unsafe_allow_html=True)
st.markdown("<div style='text-align:center;color:#666;margin-top:4px;'>Powered by Abhishek Creations</div>", unsafe_allow_html=True)
st.markdown("---")

# =================== AUTH ===================
USERNAME = st.secrets.get("USERNAME", "abhishekcreations")
PASSWORD = st.secrets.get("PASSWORD", "ac2006")
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔒 Login")
    col1, col2 = st.columns(2)
    with col1:
        u = st.text_input("Username")
    with col2:
        p = st.text_input("Password", type="password")
    if st.button("Login", type="primary"):
        if u.strip() == USERNAME and p.strip() == PASSWORD:
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Invalid credentials")
    st.stop()

# =================== SIDEBAR NAV ===================
tab = st.sidebar.radio(
    "Navigation",
    ["Scraper (Websites CSV)", "Company → Contact Finder", "History", "Diagnostics"],
    index=0
)
st.sidebar.markdown("---")
st.sidebar.caption("Abhishek Creations © 2025 – All Rights Reserved")

# =================== DB (SQLite by default; Postgres if DATABASE_URL provided) ===================
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
        );
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
        );
        """)

def create_scrape_batch(description: str, tool_type: str) -> int:
    now = datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO scrapes (created_at, year, month, description, tool_type) VALUES (:ts,:y,:m,:d,:t)"),
            {"ts": now, "y": now.year, "m": now.month, "d": description.strip(), "t": tool_type}
        )
        # last id for SQLite vs Postgres
        if engine.url.get_backend_name().startswith("sqlite"):
            sid = conn.execute(text("SELECT last_insert_rowid()")).scalar()
        else:
            sid = conn.execute(text("SELECT CAST(CURRVAL(pg_get_serial_sequence('scrapes','id')) AS INT)")).scalar()
    return int(sid)

def save_records(scrape_id: int, df: pd.DataFrame):
    cols = ["company","website","email","phone","address","source"]
    for c in cols:
        if c not in df.columns: df[c] = ""
    rows = df[cols].to_dict(orient="records")
    if not rows: return
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO records (scrape_id, company, website, email, phone, address, source)
                VALUES (:scrape_id, :company, :website, :email, :phone, :address, :source)
            """),
            [{"scrape_id": scrape_id, **r} for r in rows]
        )

init_db()

# =================== REGEX/HELPERS ===================
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
OBFUSCATED_RE = re.compile(
    r"([A-Za-z0-9._%+-]+)\s*(?:\[at\]|\(at\)| at |\s@\s|@)\s*([A-Za-z0-9.-]+)\s*(?:\[dot\]|\(dot\)| dot |\s\.\s|\.)\s*([A-Za-z]{2,})",
    re.I
)
CONTACT_KEYS = ["contact","kontakt","impressum","about","contact-us","get-in-touch","kontak","contato"]
PHONE_RE = re.compile(r"(\+?\d[\d\-\s().]{6,}\d)")

def normalize_url(url: str):
    if not url: return None
    url = url.strip()
    if url.startswith("//"): url = "http:" + url
    if not urlparse(url).scheme: url = "http://" + url
    return url if urlparse(url).netloc else None

def extract_emails(text: str):
    if not text: return []
    found = set(m.strip() for m in EMAIL_RE.findall(text))
    for m in OBFUSCATED_RE.findall(text):
        local, domain, tld = m
        found.add(f"{local}@{domain}.{tld}")
    # filter obvious junk
    return sorted(e for e in found if "@" in e and len(e) <= 254 and not e.lower().startswith("no-reply"))

def extract_phones(text: str):
    res = set()
    for raw in PHONE_RE.findall(text or ""):
        try:
            for region in ("US","IN","GB","DE","AE"):
                parsed = phonenumbers.parse(raw, region)
                if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed):
                    res.add(phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
                    break
        except Exception:
            continue
    return sorted(res)

def extract_address_snippets(text: str):
    if not text: return []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    keys = ("address","head office","registered office","location","office","impressum")
    out = []
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k in low for k in keys) and ("," in ln or any(ch.isdigit() for ch in ln)):
            snippet = ln
            if i+1 < len(lines) and ("," in lines[i+1] or any(ch.isdigit() for ch in lines[i+1])):
                snippet += " " + lines[i+1]
            out.append(snippet)
    clean, seen = [], set()
    for s in out:
        s = " ".join(s.split())
        if s not in seen:
            seen.add(s)
            clean.append(s[:300])
    return clean[:3]

def clean_visible_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script","style","noscript","iframe"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)

async def fetch(session: aiohttp.ClientSession, url: str, timeout_s: int, tries: int):
    for attempt in range(1, tries+1):
        try:
            async with asyncio.timeout(timeout_s):
                async with session.get(url, allow_redirects=True) as resp:
                    txt = await resp.text(errors="ignore")
                    return txt, str(resp.url)
        except Exception:
            await asyncio.sleep(0.5 * attempt)
    return None, url

def find_contact_links(base: str, html: str):
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
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

# =================== SEARCH (Bing → Google → SerpAPI) ===================
async def search_official_site(session, company: str, timeout_s=10):
    # 1) Azure Bing Search v7 (preferred)
    bkey = st.secrets.get("BING_API_KEY")
    bend = (st.secrets.get("BING_ENDPOINT") or "https://api.bing.microsoft.com").rstrip("/")
    if bkey:
        try:
            url = f"{bend}/bing/v7.0/search"
            headers = {"Ocp-Apim-Subscription-Key": bkey}
            params = {"q": company, "count": 5, "responseFilter": "Webpages"}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params, headers=headers) as r:
                    data = await r.json()
                    web = (data or {}).get("webPages", {}).get("value", [])
                    for it in web:
                        link = it.get("url")
                        if link and is_probably_homepage(link): return link
                    if web: return web[0].get("url")
        except Exception:
            pass

    # 2) Google CSE
    gkey = st.secrets.get("GOOGLE_API_KEY")
    gcx  = st.secrets.get("GOOGLE_CX")
    if gkey and gcx:
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {"key": gkey, "cx": gcx, "q": company, "num": 3}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params) as r:
                    data = await r.json()
                    items = (data or {}).get("items", [])
                    for it in items:
                        link = it.get("link")
                        if link and is_probably_homepage(link): return link
                    if items: return items[0].get("link")
        except Exception:
            pass

    # 3) SerpAPI
    skey = st.secrets.get("SERPAPI_KEY")
    if skey:
        try:
            url = "https://serpapi.com/search.json"
            params = {"engine": "google", "q": company, "api_key": skey}
            async with asyncio.timeout(timeout_s):
                async with session.get(url, params=params) as r:
                    data = await r.json()
                    org = (data or {}).get("organic_results", [])
                    for it in org:
                        link = it.get("link")
                        if link and is_probably_homepage(link): return link
                    if org: return org[0].get("link")
        except Exception:
            pass

    return None

# =================== PIPELINES ===================
async def process_row_website(session, sem, row, opts):
    async with sem:
        company = (row.get("company") or row.get("Company") or row.get("name") or "").strip()
        site = (row.get("website") or row.get("Website") or row.get("site") or "").strip()
        result = []
        failed = {"company": company, "website": site, "notes": ""}

        if not site:
            failed["notes"] = "no-website"
            return result, failed

        url = normalize_url(site)
        if not url:
            failed["notes"] = "invalid-url"
            return result, failed

        html, final = await fetch(session, url, opts["timeout"], opts["tries"])
        if not html:
            failed["notes"] = "fetch-failed"
            return result, failed

        visible = clean_visible_text(html)
        emails = extract_emails(visible)
        sources = {}
        if not emails and opts.get("follow_contacts"):
            for c in find_contact_links(final, html):
                ctext, cfinal = await fetch(session, c, opts["timeout"], opts["tries"])
                if not ctext: continue
                cvis = clean_visible_text(ctext)
                for e in extract_emails(cvis):
                    sources.setdefault(e, set()).add(cfinal)
        for e in emails:
            sources.setdefault(e, set()).add(final)

        all_emails = sorted(sources.keys())
        if all_emails:
            for e in all_emails:
                result.append({
                    "company": company, "website": final, "email": e,
                    "phone": "", "address": "", "source": ",".join(sorted(sources.get(e, {final})))
                })
            return result, None
        else:
            failed["notes"] = "no-emails-found"
            return result, failed

async def run_all_website(rows, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 6)
    connector = aiohttp.TCPConnector(limit_per_host=opts["concurrency"], ssl=False)
    sem = asyncio.Semaphore(opts["concurrency"])
    results, failed = [], []
    headers = {"User-Agent":"AC-Scraper/1.0 (+https://example.com)"}
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True, headers=headers) as session:
        tasks = [process_row_website(session, sem, r, opts) for r in rows]
        total = len(tasks); done = 0
        for fut in asyncio.as_completed(tasks):
            res, fail = await fut
            if res: results.extend(res)
            if fail: failed.append(fail)
            done += 1
            if cb: cb(done, total)
    return results, failed

async def company_to_contacts(session, company, opts):
    homepage = await search_official_site(session, company, timeout_s=opts["timeout"])
    if not homepage:
        return [], {"company": company, "website": "", "notes": "no-search-result"}

    html, final = await fetch(session, homepage, opts["timeout"], opts["tries"])
    if not html:
        return [], {"company": company, "website": homepage, "notes": "homepage-fetch-failed"}

    visible = clean_visible_text(html)
    emails = extract_emails(visible)
    phones = extract_phones(visible)
    addrs  = extract_address_snippets(visible)

    sources = {}
    if not emails and opts.get("follow_contacts"):
        for c in find_contact_links(final, html):
            ctext, cfinal = await fetch(session, c, opts["timeout"], opts["tries"])
            if not ctext: continue
            cvis = clean_visible_text(ctext)
            for e in extract_emails(cvis): sources.setdefault(e, set()).add(cfinal)
            for p in extract_phones(cvis): sources.setdefault(f"PHONE::{p}", set()).add(cfinal)
            for a in extract_address_snippets(cvis): sources.setdefault(f"ADDR::{a}", set()).add(cfinal)

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
                "company": company,
                "website": final,
                "email": e,
                "phone": "; ".join(phone_list)[:200],
                "address": "; ".join(address_list)[:300],
                "source": ",".join(sorted(sources.get(e, {final})))
            })
        return rows, None
    else:
        return [], {"company": company, "website": final, "notes": "no-emails-found"}

async def run_all_company(names, opts, cb=None):
    timeout_cfg = aiohttp.ClientTimeout(total=opts["timeout"] + 6)
    connector = aiohttp.TCPConnector(limit_per_host=opts["concurrency"], ssl=False)
    sem = asyncio.Semaphore(opts["concurrency"])
    results, failed = [], []
    headers = {"User-Agent":"AC-Scraper/1.0 (+https://example.com)"}
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector, trust_env=True, headers=headers) as session:
        tasks = [company_to_contacts(session, nm, opts) for nm in names]
        total = len(tasks); done = 0
        for fut in asyncio.as_completed(tasks):
            rows, fail = await fut
            if rows: results.extend(rows)
            if fail: failed.append(fail)
            done += 1
            if cb: cb(done, total)
    return results, failed

# =================== UI: WEBSITES CSV ===================
if tab.startswith("Scraper"):
    st.title("📧 Email Scraper (Websites CSV)")
    description = st.text_input("Batch description (required before scraping)", placeholder="e.g., SMM Hamburg exhibitors Aug 2025")
    uploaded = st.file_uploader("Upload CSV / XLSX (columns: company, website)", type=["csv","xlsx","xls"])
    colA, colB, colC = st.columns(3)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages (fallback)", value=True)
    with colB: concurrency = st.slider("Concurrency", 2, 60, value=12)
    with colC: timeout = st.slider("Timeout (sec)", 6, 30, value=12)
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
                st.dataframe(out.head(25), use_container_width=True)
                save_records(scrape_id, out)
                st.info("Saved to database. (History tab)")
                st.download_button(
                    "Download results CSV",
                    out.to_csv(index=False).encode("utf-8"),
                    file_name=f"emails_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
                )
            if failed:
                miss = pd.DataFrame(failed)
                st.markdown("### No-email / failed sites (preview)")
                st.dataframe(miss.head(25), use_container_width=True)
                st.download_button(
                    "Download failed CSV",
                    miss.to_csv(index=False).encode("utf-8"),
                    file_name=f"failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
                )

# =================== UI: COMPANY → CONTACT FINDER ===================
elif tab.startswith("Company"):
    st.title("🏢 Company → Contact Finder")

    # At least one provider required
    has_provider = bool(st.secrets.get("BING_API_KEY")) or \
                   bool(st.secrets.get("GOOGLE_API_KEY") and st.secrets.get("GOOGLE_CX")) or \
                   bool(st.secrets.get("SERPAPI_KEY"))
    if not has_provider:
        st.warning("Add a search provider in Secrets (Bing Azure preferred, else Google CSE or SerpAPI).")
        st.stop()

    description = st.text_input("Batch description (required before finding)", placeholder="e.g., Marine OEMs shortlist Sept 2025")
    txt = st.text_area("Companies (one per line)", height=160, placeholder="ACME Corp\nFoo Technologies\nBar Shipping GmbH")

    colA, colB, colC = st.columns(3)
    with colA: follow_contacts = st.checkbox("Follow contact/about pages", value=True)
    with colB: concurrency = st.slider("Concurrency", 2, 40, value=10)
    with colC: timeout = st.slider("Timeout (sec)", 6, 30, value=12)
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

        results, failed = asyncio.run(run_all_company(
            companies,
            {"timeout": timeout, "tries": tries, "follow_contacts": follow_contacts, "concurrency": concurrency},
            cb=cb
        ))

        if results:
            df_out = pd.DataFrame(results).drop_duplicates(subset=["company","website","email"])
            st.success(f"Found {len(df_out)} email rows.")
            st.dataframe(df_out.head(25), use_container_width=True)
            save_records(scrape_id, df_out)
            st.info("Saved to database. (History tab)")
            st.download_button(
                "Download contacts CSV",
                df_out.to_csv(index=False).encode("utf-8"),
                file_name=f"contacts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
            )
        if failed:
            df_fail = pd.DataFrame(failed)
            st.markdown("### Not found / failed")
            st.dataframe(df_fail.head(25), use_container_width=True)
            st.download_button(
                "Download failed CSV",
                df_fail.to_csv(index=False).encode("utf-8"),
                file_name=f"company_failed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
            )

# =================== UI: HISTORY ===================
elif tab == "History":
    st.title("🗂️ Scrape History")

    with engine.begin() as conn:
        batches = conn.execute(text("""
            SELECT id, created_at, year, month, description, tool_type
            FROM scrapes
            ORDER BY created_at DESC
        """)).mappings().all()

    if not batches:
        st.info("No batches found yet. Run a scrape from the other tabs.")
        st.stop()

    dfb = pd.DataFrame(batches)
    dfb["created_at"] = pd.to_datetime(dfb["created_at"])
    col1, col2 = st.columns([2,1])
    with col1:
        st.dataframe(dfb[["id","created_at","year","month","tool_type","description"]], use_container_width=True)
    with col2:
        years = ["All"] + sorted(dfb["year"].unique().tolist(), reverse=True)
        ysel = st.selectbox("Year", years, index=0)
        msel = st.selectbox("Month", ["All"] + sorted(dfb["month"].unique().tolist(), reverse=True), index=0)

    fdf = dfb.copy()
    if ysel != "All": fdf = fdf[fdf["year"] == ysel]
    if msel != "All": fdf = fdf[fdf["month"] == msel]

    st.markdown("### Download a batch")
    batch_ids = fdf["id"].tolist()
    if not batch_ids:
        st.info("No batches match the selected filters.")
    else:
        bid = st.selectbox("Choose a batch ID", batch_ids, index=0)
        if st.button("Prepare CSV"):
            with engine.begin() as conn:
                rows = conn.execute(text("""
                    SELECT company, website, email, phone, address, source
                    FROM records
                    WHERE scrape_id = :sid
                    ORDER BY company
                """), {"sid": bid}).mappings().all()
            if rows:
                dfr = pd.DataFrame(rows)
                st.success(f"{len(dfr)} rows found.")
                st.dataframe(dfr.head(25), use_container_width=True)
                st.download_button(
                    "Download batch CSV",
                    dfr.to_csv(index=False).encode("utf-8"),
                    file_name=f"batch_{bid}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
                )
            else:
                st.info("No rows found for this batch.")

# =================== UI: DIAGNOSTICS ===================
elif tab == "Diagnostics":
    st.title("🧪 Diagnostics")

    # Show what keys we have
    st.subheader("Keys present?")
    st.write("BING_API_KEY present:", bool(st.secrets.get("BING_API_KEY")))
    st.write("BING_ENDPOINT:", st.secrets.get("BING_ENDPOINT", "(not set)"))
    st.write("GOOGLE_API_KEY present:", bool(st.secrets.get("GOOGLE_API_KEY")))
    st.write("GOOGLE_CX present:", bool(st.secrets.get("GOOGLE_CX")))
    st.write("SERPAPI_KEY present:", bool(st.secrets.get("SERPAPI_KEY")))

    test_query = st.text_input("Test query", value="Murata")
    if st.button("Run Diagnostics", type="primary"):
        async def run_diag():
            out = {}
            timeout_cfg = aiohttp.ClientTimeout(total=20)
            headers = {"User-Agent":"AC-Scraper/diag"}
            async with aiohttp.ClientSession(timeout=timeout_cfg, headers=headers) as session:
                # Bing test
                bkey = st.secrets.get("BING_API_KEY")
                bend = (st.secrets.get("BING_ENDPOINT") or "https://api.bing.microsoft.com").rstrip("/")
                if bkey:
                    try:
                        url = f"{bend}/bing/v7.0/search"
                        params = {"q": test_query, "count": 2, "responseFilter": "Webpages"}
                        async with session.get(url, params=params, headers={"Ocp-Apim-Subscription-Key": bkey}) as r:
                            out["bing_status"] = r.status
                            out["bing_json"] = await r.json()
                    except Exception as e:
                        out["bing_error"] = str(e)
                else:
                    out["bing_note"] = "No BING_API_KEY"

                # Google CSE test
                gkey = st.secrets.get("GOOGLE_API_KEY")
                gcx  = st.secrets.get("GOOGLE_CX")
                if gkey and gcx:
                    try:
                        url = "https://www.googleapis.com/customsearch/v1"
                        params = {"key": gkey, "cx": gcx, "q": test_query, "num": 2}
                        async with session.get(url, params=params) as r:
                            out["google_status"] = r.status
                            try:
                                out["google_json"] = await r.json()
                            except Exception:
                                out["google_text"] = await r.text()
                    except Exception as e:
                        out["google_error"] = str(e)
                else:
                    out["google_note"] = "No GOOGLE_API_KEY/GOOGLE_CX"

                # Plain fetch test
                try:
                    async with session.get("https://www.murata.com") as r:
                        html = await r.text()
                        out["plain_status"] = r.status
                        out["plain_final_url"] = str(r.url)
                        out["plain_sample"] = html[:800]
                except Exception as e:
                    out["plain_error"] = str(e)
            return out

        res = asyncio.run(run_diag())
        st.subheader("Results")
        st.code(json.dumps(res, indent=2)[:20000])
