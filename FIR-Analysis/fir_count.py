#!/usr/bin/env python3
"""
scrape_bihar_fir_fast.py

Concurrent scraper for SCRB Bihar FIR counts by district & police station.
Outputs bihar_fir_counts.csv with columns:
district_id, ps_id, 2020-01 ... 2025-12
"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
import csv
from tqdm import tqdm

BASE_URL = "https://scrb.bihar.gov.in/FIRiew.aspx"
DISTRICT_IDS = list(range(1, 39))  # 1..38 inclusive
START_YEAR, END_YEAR = 2014, 2025
MAX_WORKERS = 10  # adjust for speed / politeness
OUTPUT_CSV = "bihar_fir_counts.csv"
REQUEST_TIMEOUT = 30  # seconds

# Build month columns
MONTH_COLS = []
for y in range(START_YEAR, END_YEAR + 1):
    for m in range(1, 13):
        MONTH_COLS.append(f"{y}-{m:02d}")


# Utilities: resilient session factory
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Referer": BASE_URL,
    })
    retries = Retry(total=5, backoff_factor=0.8,
                    status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


# Extract ASP.NET hidden fields (safe if some are missing)
def extract_hidden_fields(soup):
    def val(id_):
        el = soup.find("input", {"id": id_})
        return el["value"] if el and el.has_attr("value") else ""
    return {
        "__VIEWSTATE": val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": val("__EVENTVALIDATION"),
        "__LASTFOCUS": val("__LASTFOCUS"),
    }


# Robust date extractor for many formats
DATE_RE = re.compile(r"(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})")

def extract_date_from_text(text):
    m = DATE_RE.search(text)
    if not m:
        return None
    d, mo, y = m.groups()
    if len(y) == 2:
        y = "20" + y
    try:
        return datetime(int(y), int(mo), int(d))
    except ValueError:
        return None


# Parse police station options from HTML soup
def parse_ps_options(soup):
    sel = soup.find("select", {"id": "ctl00_ContentPlaceHolder1_ddlPoliceStation"})
    if not sel:
        return []
    options = []
    for opt in sel.find_all("option"):
        val = opt.get("value", "").strip()
        # skip empty or obviously invalid
        if val and val != "0":
            # Some option text may be purely whitespace; keep ID only
            try:
                options.append(val)
            except Exception:
                continue
    # de-duplicate while preserving order
    seen = set()
    unique = []
    for v in options:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


# Parse FIR rows for FIR Date column from table#example
def parse_fir_rows(soup):
    table = soup.find("table", {"id": "example"})
    if not table:
        return []
    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return []
    for tr in tbody.find_all("tr"):
        cols = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if cols:
            rows.append(cols)
    return rows


# Detect postback links (pagination) and return list of (target, argument)
# We search for javascript:__doPostBack('target','arg') in onclick/href attributes.
POSTBACK_RE = re.compile(r"__doPostBack\('([^']*)'\s*,\s*'([^']*)'\)")

def find_postback_actions(soup):
    actions = []
    for a in soup.find_all(["a", "button"]):
        # check onclick
        onclick = a.get("onclick", "")
        href = a.get("href", "")
        for source in (onclick, href):
            if source:
                m = POSTBACK_RE.search(source)
                if m:
                    tgt, arg = m.groups()
                    actions.append((tgt, arg))
    # Return unique actions
    unique = []
    seen = set()
    for t, a in actions:
        key = (t, a)
        if key not in seen:
            seen.add(key)
            unique.append((t, a))
    return unique


# Post helper: always extract new hidden fields from response
def do_post(session, payload, sleep_min=0.3, sleep_max=1.0):
    # small random sleep to vary request timing
    time.sleep(random.uniform(sleep_min, sleep_max))
    r = session.post(BASE_URL, data=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = extract_hidden_fields(soup)
    return soup, hidden


# For a given district, get PS list by simulating postback on district dropdown
def get_ps_for_district(district_id, session=None):
    s = session or make_session()
    # initial GET
    r = s.get(BASE_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = extract_hidden_fields(soup)

    payload = {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlDistrict",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": hidden.get("__LASTFOCUS", ""),
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        # set district
        "ctl00$ContentPlaceHolder1$ddlDistrict": str(district_id),
        # keep the PS field blank for this postback
        "ctl00$ContentPlaceHolder1$ddlPoliceStation": "0",
        # maintain default search radio and text
        "ctl00$ContentPlaceHolder1$optionsRadios": "radioPetioner",
        "ctl00$ContentPlaceHolder1$txtSearchBy": "",
    }

    soup2, hidden2 = do_post(s, payload)
    ps_list = parse_ps_options(soup2)
    return ps_list


# For a given district & ps_id, fetch counts across all pages (2020-2025)
def fetch_counts_for_ps(district_id, ps_id, session=None):
    s = session or make_session()
    # 1️⃣ Initial GET
    r = s.get(BASE_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = extract_hidden_fields(soup)

    # 2️⃣ Postback to load police stations for this district
    payload1 = {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlDistrict",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": hidden.get("__LASTFOCUS", ""),
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "ctl00$ContentPlaceHolder1$ddlDistrict": str(district_id),
        "ctl00$ContentPlaceHolder1$ddlPoliceStation": "0",
        "ctl00$ContentPlaceHolder1$optionsRadios": "radioFIR",
        "ctl00$ContentPlaceHolder1$txtSearchBy": "",
    }

    soup2, hidden2 = do_post(s, payload1)

    # 3️⃣ Real search postback for district + PS
    payload2 = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": hidden2.get("__LASTFOCUS", ""),
        "__VIEWSTATE": hidden2.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden2.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": hidden2.get("__EVENTVALIDATION", ""),
        "ctl00$ContentPlaceHolder1$ddlDistrict": str(district_id),
        "ctl00$ContentPlaceHolder1$ddlPoliceStation": str(ps_id),
        "ctl00$ContentPlaceHolder1$optionsRadios": "radioFIR",
        "ctl00$ContentPlaceHolder1$txtSearchBy": "",
        "ctl00$ContentPlaceHolder1$btnSearch": "Search",
    }

    try:
        soup_res, hidden_res = do_post(s, payload2)
    except Exception as e:
        return Counter()

    counts = Counter()

    # parse first page
    rows = parse_fir_rows(soup_res)
    for cols in rows:
        # attempt to extract date from known columns; "FIR Date" is 3rd column (index 2) typically,
        # but to be robust search all cells in row
        for cell in cols:
            dt = extract_date_from_text(cell)
            if dt and START_YEAR <= dt.year <= END_YEAR:
                counts[(dt.year, dt.month)] += 1
                break

    # detect pagination postback actions (page links)
    actions = find_postback_actions(soup_res)
    # find page actions that include "Page$" in argument
    page_actions = [(t, a) for (t, a) in actions if "Page$" in a]
    # deduplicate by page number and preserve order
    seen_pages = set()
    ordered_page_actions = []
    for t, a in page_actions:
        if a not in seen_pages:
            seen_pages.add(a)
            ordered_page_actions.append((t, a))

    # For each page action, simulate postback to fetch page data
    for tgt, arg in ordered_page_actions:
        # build payload using latest hidden fields
        payload_page = {
            "__EVENTTARGET": tgt,
            "__EVENTARGUMENT": arg,
            "__LASTFOCUS": hidden_res.get("__LASTFOCUS", ""),
            "__VIEWSTATE": hidden_res.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": hidden_res.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": hidden_res.get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$ddlDistrict": str(district_id),
            "ctl00$ContentPlaceHolder1$ddlPoliceStation": str(ps_id),
            "ctl00$ContentPlaceHolder1$optionsRadios": "radioFIR",
            "ctl00$ContentPlaceHolder1$txtSearchBy": "",
        }
        try:
            soup_page, hidden_page = do_post(s, payload_page)
        except Exception:
            # try to continue to next
            continue
        hidden_res = hidden_page  # update for subsequent page posts
        rows = parse_fir_rows(soup_page)
        for cols in rows:
            for cell in cols:
                dt = extract_date_from_text(cell)
                if dt and START_YEAR <= dt.year <= END_YEAR:
                    counts[(dt.year, dt.month)] += 1
                    break

    return counts


# Worker wrapper to run per PS and return CSV row data
def worker_ps(args):
    district_id, ps_id = args
    session = make_session()
    # small jitter to reduce identical request bursts
    time.sleep(random.random() * 0.3)
    try:
        counts = fetch_counts_for_ps(district_id, ps_id, session=session)
    except Exception as e:
        counts = Counter()
    # Build ordered month values
    row = [district_id, ps_id]
    for col in MONTH_COLS:
        y, m = map(int, col.split("-"))
        row.append(counts.get((y, m), 0))
    return row


def main():
    # Prepare CSV writer and write header
    header = ["district_id", "ps_id"] + MONTH_COLS
    # Use append mode so we can incrementally save; start fresh
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)

    # We'll iterate districts, get PS list, and dispatch PS jobs concurrently
    for district_id in DISTRICT_IDS:
        # get PS list for this district
        try:
            ps_list = get_ps_for_district(district_id)
        except Exception:
            ps_list = []
        # If none found, still write a row with ps_id blank (optional)
        if not ps_list:
            # write row with zeros
            with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                row = [district_id, ""] + [0]*len(MONTH_COLS)
                writer.writerow(row)
            continue

        # Prepare tasks list of (district_id, ps_id)
        tasks = [(district_id, ps_id) for ps_id in ps_list]

        # Run thread pool for this district's PS entries
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(worker_ps, t): t for t in tasks}
            for fut in tqdm(as_completed(futures), total=len(futures),
                            desc=f"District {district_id}", unit="ps"):
                try:
                    row = fut.result()
                except Exception:
                    # on failure write zeros for that ps
                    district_id_failed, ps_failed = futures[fut]
                    row = [district_id_failed, ps_failed] + [0]*len(MONTH_COLS)
                # append to CSV
                with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as fh:
                    writer = csv.writer(fh)
                    writer.writerow(row)

    print(f"\nDone. Results written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
