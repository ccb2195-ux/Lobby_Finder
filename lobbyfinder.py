#!/usr/bin/env python3
"""
lobbyfinder.py — lobbying & campaign-finance data compiler

No API keys needed.  Data sources:
  • Senate Lobbying Disclosure Act (LDA) API  — lda.senate.gov/api/v1
  • Federal Election Commission (FEC) API     — api.open.fec.gov/v1
    (uses public DEMO_KEY; get your own free key at api.data.gov for higher limits)

Usage:
  python lobbyfinder.py "Apple Inc" "Microsoft Corp"
  python lobbyfinder.py companies.txt          # one company name per line
  python lobbyfinder.py                        # interactive prompt

Output — written to ./lobbyfinder_output/ :
  lobbying_filings_<ts>.csv           Senate LDA filings (lobbying spend)
  fec_committees_<ts>.csv             PAC / committee registrations
  fec_committee_totals_<ts>.csv       Per-cycle financial totals per committee
  fec_individual_contribs_<ts>.csv    Sched A — individual contributions by employer
  fec_pac_disbursements_<ts>.csv      Sched B — PAC disbursements to candidates
  summary_<ts>.csv                    Aggregated totals per company
"""

import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ── configuration ─────────────────────────────────────────────────────────────

LDA_BASE      = "https://lda.senate.gov/api/v1"
FEC_BASE      = "https://api.open.fec.gov/v1"
FEC_KEY       = os.getenv("MY_KEY", "DEMO_KEY")
LDA_DELAY     = 5.0          # Senate LDA is strict; ~12 req/min keeps it comfortable
FEC_DELAY     = 1.5          # FEC DEMO_KEY allows ~40 req/hr
LDA_PAGE_SIZE = 100          # larger pages = fewer total requests
FEC_PAGE_SIZE = 100

# ── session ───────────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": "LobbyFinder/1.0 (research)"})

# ── low-level helpers ─────────────────────────────────────────────────────────

def _get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = _session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = min(int(r.headers.get("Retry-After", 60)) + 5, 120)
                print(f"      ↳ rate-limited — sleeping {wait} s …")
                time.sleep(wait)
                continue
            if r.status_code in (400, 401, 403):
                print(f"      ↳ HTTP {r.status_code} — check API key. Response: {r.text[:200]}")
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                print(f"      ↳ request failed: {exc}")
                return None
            time.sleep(5)
    return None


def _lda_pages(endpoint, params):
    """Yield every result from a paginated Senate LDA endpoint."""
    p = dict(params, page=1, page_size=LDA_PAGE_SIZE)
    while True:
        data = _get(f"{LDA_BASE}/{endpoint}/", p)
        if not data:
            break
        yield from data.get("results", [])
        if not data.get("next"):
            break
        p["page"] += 1
        time.sleep(LDA_DELAY)


def _fec_pages(endpoint, params, max_pages=20):
    """Yield every result from a paginated FEC endpoint (page-number style)."""
    p = dict(params, api_key=FEC_KEY, per_page=FEC_PAGE_SIZE, page=1)
    for _ in range(max_pages):
        data = _get(f"{FEC_BASE}{endpoint}", p)
        if not data:
            break
        yield from data.get("results", [])
        pag = data.get("pagination", {})
        if p["page"] >= pag.get("pages", 1):
            break
        p["page"] += 1
        time.sleep(FEC_DELAY)


def _safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

# ── Senate LDA ────────────────────────────────────────────────────────────────

def _lda_row(company, filing, role):
    reg        = filing.get("registrant") or {}
    cli        = filing.get("client") or {}
    activities = filing.get("lobbying_activities") or []
    issues = "; ".join(
        (act.get("general_issue_code_display") or act.get("general_issue_code") or "")
        for act in activities
    )
    return {
        "source":          "Senate LDA",
        "query_company":   company,
        "role":            role,
        "filing_uuid":     filing.get("filing_uuid"),
        "filing_year":     filing.get("filing_year"),
        "period":          filing.get("period_of_report"),
        "filing_type":     filing.get("filing_type_display") or filing.get("filing_type"),
        "dt_posted":       filing.get("dt_posted"),
        "registrant_name": reg.get("name"),
        "registrant_id":   reg.get("id"),
        "client_name":     cli.get("name"),
        "client_id":       cli.get("id"),
        "income":          filing.get("income"),
        "expenses":        filing.get("expenses"),
        "lobbying_issues": issues,
        "url": f"https://lda.senate.gov/filings/public/filing/{filing.get('filing_uuid', '')}/print/",
    }


def _dedup_client_filings(rows):
    """Keep only the latest-posted filing per (registrant, client, year, period).

    The LDA API returns both originals and amendments as separate rows.
    Summing both inflates spend figures; this mirrors OpenSecrets' methodology
    of counting only the most recent version of each period's filing.
    """
    best = {}
    for row in rows:
        key = (row["registrant_id"], row["client_id"], row["filing_year"], row["period"])
        existing = best.get(key)
        if existing is None or (row["dt_posted"] or "") > (existing["dt_posted"] or ""):
            best[key] = row
    return list(best.values())


def fetch_lda(company):
    rows = []

    print("  [LDA] client filings …", end=" ", flush=True)
    raw = []
    for f in _lda_pages("filings", {"client_name": company}):
        raw.append(_lda_row(company, f, "client"))
    deduped = _dedup_client_filings(raw)
    rows.extend(deduped)
    print(f"{len(deduped)} rows (deduped from {len(raw)})")

    print("  [LDA] registrant filings …", end=" ", flush=True)
    before = len(rows)
    for f in _lda_pages("filings", {"registrant_name": company}):
        rows.append(_lda_row(company, f, "registrant"))
    print(f"{len(rows) - before} rows")

    return rows

# ── FEC committees ────────────────────────────────────────────────────────────

def _committee_row(company, c):
    return {
        "source":          "FEC Committees",
        "query_company":   company,
        "committee_id":    c.get("committee_id"),
        "committee_name":  c.get("name"),
        "committee_type":  c.get("committee_type_full"),
        "designation":     c.get("designation_full"),
        "party":           c.get("party_full"),
        "state":           c.get("state"),
        "treasurer":       c.get("treasurer_name"),
        "first_file_date": c.get("first_file_date"),
        "last_file_date":  c.get("last_file_date"),
        "cycles":          "; ".join(str(x) for x in (c.get("cycles") or [])),
    }


def fetch_committees(company):
    rows = []
    print("  [FEC] committee registrations …", end=" ", flush=True)
    keyword = company.lower()
    for c in _fec_pages("/committees/", {"name": company}, max_pages=1):
        if keyword in (c.get("name") or "").lower():
            rows.append(_committee_row(company, c))
    print(f"{len(rows)} found")
    return rows

# ── FEC committee financial totals ────────────────────────────────────────────

def fetch_committee_totals(company, committee_rows):
    rows  = []
    pairs = [
        (r["committee_id"], r["committee_name"])
        for r in committee_rows
        if r["query_company"] == company and r["committee_id"]
    ]
    if not pairs:
        return rows

    print(f"  [FEC] financial totals for {len(pairs)} committee(s) …", end=" ", flush=True)
    for cid, cname in pairs:
        for t in _fec_pages(f"/committee/{cid}/totals/", {}):
            rows.append({
                "source":                      "FEC Committee Totals",
                "query_company":               company,
                "committee_id":                cid,
                "committee_name":              cname,
                "cycle":                       t.get("cycle"),
                "receipts":                    t.get("receipts"),
                "disbursements":               t.get("disbursements"),
                "contributions":               t.get("contributions"),
                "operating_expenditures":      t.get("operating_expenditures"),
                "contributions_to_candidates": (
                    t.get("contributions_to_candidates")
                    or t.get("candidate_contributions")
                ),
                "independent_expenditures":    t.get("independent_expenditures"),
                "coordinated_expenditures":    t.get("coordinated_expenditures_by_party"),
                "cash_on_hand_end":            t.get("cash_on_hand_end_period"),
            })
        time.sleep(FEC_DELAY)
    print(f"{len(rows)} rows")
    return rows

# ── FEC Schedule A — individual contributions by employer ─────────────────────

def fetch_individual_contribs(company):
    rows = []
    print("  [FEC] individual contributions by employer …", end=" ", flush=True)
    params = {
        "contributor_employer": company,
        "is_individual":        "true",
        "sort":                 "-contribution_receipt_date",
    }
    for c in _fec_pages("/schedules/schedule_a/", params, max_pages=10):
        committee = c.get("committee") or {}
        rows.append({
            "source":                 "FEC Schedule A",
            "query_company":          company,
            "contributor_name":       c.get("contributor_name"),
            "contributor_employer":   c.get("contributor_employer"),
            "contributor_occupation": c.get("contributor_occupation"),
            "contributor_city":       c.get("contributor_city"),
            "contributor_state":      c.get("contributor_state"),
            "amount":                 c.get("contribution_receipt_amount"),
            "date":                   c.get("contribution_receipt_date"),
            "recipient_committee":    committee.get("name"),
            "recipient_id":           c.get("committee_id"),
            "memo":                   c.get("memo_text"),
            "cycle":                  c.get("two_year_transaction_period"),
        })
    print(f"{len(rows)} rows (capped at 1 000)")
    return rows

# ── FEC Schedule B — PAC disbursements to candidates ─────────────────────────

def fetch_pac_disbursements(company, committee_rows):
    rows  = []
    pairs = [
        (r["committee_id"], r["committee_name"])
        for r in committee_rows
        if r["query_company"] == company and r["committee_id"]
    ]
    if not pairs:
        return rows

    print(f"  [FEC] PAC disbursements for {len(pairs)} committee(s) …", end=" ", flush=True)
    for cid, cname in pairs:
        for d in _fec_pages("/schedules/schedule_b/", {
            "committee_id": cid,
            "sort":         "-disbursement_date",
        }, max_pages=5):
            rows.append({
                "source":            "FEC Schedule B",
                "query_company":     company,
                "committee_id":      cid,
                "committee_name":    cname,
                "recipient_name":    d.get("recipient_name"),
                "recipient_id":      d.get("recipient_committee_id"),
                "amount":            d.get("disbursement_amount"),
                "date":              d.get("disbursement_date"),
                "description":       d.get("disbursement_description"),
                "category":          d.get("disbursement_type_description"),
                "cycle":             d.get("two_year_transaction_period"),
            })
        time.sleep(FEC_DELAY)
    print(f"{len(rows)} rows")
    return rows

# ── summary ───────────────────────────────────────────────────────────────────

def build_summary(companies, lda_rows, totals_rows, indiv_rows):
    out = []
    for company in companies:
        # Lobbying spend: expenses reported by the company as client
        lda_spend = sum(
            _safe_float(r["expenses"])
            for r in lda_rows
            if r["query_company"] == company and r["role"] == "client"
        )
        lda_filings = sum(1 for r in lda_rows if r["query_company"] == company and r["role"] == "client")

        # PAC totals (sum across all cycles found)
        pac_receipts    = sum(_safe_float(r["receipts"])     for r in totals_rows if r["query_company"] == company)
        pac_disbursements = sum(_safe_float(r["disbursements"]) for r in totals_rows if r["query_company"] == company)
        pac_to_candidates = sum(_safe_float(r["contributions_to_candidates"]) for r in totals_rows if r["query_company"] == company)

        indiv_total = sum(_safe_float(r["amount"]) for r in indiv_rows if r["query_company"] == company)
        num_committees = sum(1 for r in totals_rows if r["query_company"] == company)

        out.append({
            "company":                        company,
            "lda_lobbying_filings_count":     lda_filings,
            "lda_lobbying_spend":             round(lda_spend, 2),
            "pac_committees_found":           num_committees,
            "pac_total_receipts_all_cycles":  round(pac_receipts, 2),
            "pac_total_disbursements_all_cycles": round(pac_disbursements, 2),
            "pac_contributions_to_candidates": round(pac_to_candidates, 2),
            "individual_contribs_sample_total": round(indiv_total, 2),
            "individual_contribs_sample_note": "first 1000 rows only — not a complete total",
            "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
    return out

# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(path, rows):
    if not rows:
        print(f"  [skip]  {os.path.basename(path)} — no data")
        return 0
    # Build fieldnames preserving insertion order across all rows
    seen       = {}
    fieldnames = []
    for row in rows:
        for k in row:
            if k not in seen:
                seen[k] = True
                fieldnames.append(k)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [saved] {os.path.basename(path)}  ({len(rows)} rows)")
    return len(rows)

# ── entry point ───────────────────────────────────────────────────────────────

def main():
    # ── parse company list ────────────────────────────────────────────────────
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if len(sys.argv) == 2 and os.path.isfile(arg):
            with open(arg, encoding="utf-8") as fh:
                companies = [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]
            print(f"Loaded {len(companies)} companies from {arg}")
        else:
            companies = [a.strip() for a in sys.argv[1:] if a.strip()]
    else:
        print("LobbyFinder — lobbying & campaign-finance data compiler")
        print("─" * 52)
        print("Enter company names (one per line; blank line when done):\n")
        companies = []
        while True:
            line = input("  Company: ").strip()
            if not line:
                break
            companies.append(line)

    if not companies:
        print("No companies provided — exiting.")
        sys.exit(1)

    # ── output filename ───────────────────────────────────────────────────────
    ts           = datetime.now().strftime("%Y%m%d_%H%M")
    default_name = ts
    raw          = input(f"\n  Output file prefix (leave blank to use timestamp '{ts}'): ").strip()
    label        = raw if raw else default_name
    label        = "".join(c for c in label if c.isalnum() or c in "_- ")
    label        = label.strip().replace(" ", "_")
    if not label:
        label = default_name

    # ── collect data ──────────────────────────────────────────────────────────
    ts      = label
    out_dir = os.path.join(os.path.expanduser("~"), "Desktop", "lobbyfinder_output")
    os.makedirs(out_dir, exist_ok=True)

    all_lda   = []
    all_cmts  = []
    all_tots  = []
    all_indiv = []
    all_disb  = []

    for company in companies:
        print(f"\n{'─' * 52}")
        print(f"  {company}")
        print(f"{'─' * 52}")

        lda  = fetch_lda(company)
        cmts = fetch_committees(company)
        tots = fetch_committee_totals(company, cmts)
        indv = fetch_individual_contribs(company)
        disb = fetch_pac_disbursements(company, cmts)

        all_lda  .extend(lda)
        all_cmts .extend(cmts)
        all_tots .extend(tots)
        all_indiv.extend(indv)
        all_disb .extend(disb)

    # ── write CSVs ────────────────────────────────────────────────────────────
    print(f"\n{'─' * 52}")
    print("  Writing output …")
    print(f"{'─' * 52}")

    write_csv(f"{out_dir}/lobbying_filings_{ts}.csv",           all_lda)
    write_csv(f"{out_dir}/fec_committees_{ts}.csv",             all_cmts)
    write_csv(f"{out_dir}/fec_committee_totals_{ts}.csv",       all_tots)
    write_csv(f"{out_dir}/fec_individual_contribs_{ts}.csv",    all_indiv)
    write_csv(f"{out_dir}/fec_pac_disbursements_{ts}.csv",      all_disb)

    summary = build_summary(companies, all_lda, all_tots, all_indiv)
    write_csv(f"{out_dir}/summary_{ts}.csv", summary)

    print(f"\nDone. All files are in ./{out_dir}/")


if __name__ == "__main__":
    main()
