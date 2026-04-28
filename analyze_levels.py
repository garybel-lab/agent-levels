#!/usr/bin/env python3
"""Aggregate Q1 2026 agent metrics per level (L0/L1/L2/L3) for index.html update."""
import json
import re
from statistics import mean

DATA_FILE = "/workspaces/agent-levels/agents_data_q1_2026.json"

# Canonical rosters from "Agents par level" table (excluding Mohammed Mazour & Marhuska Ngohiag who lack JSON data)
LEVELS = {
    "L0": [
        "Thomas Dufrene", "Elise Francois", "Florence Touyard", "Claire Lardenois",
        "Charline Verdeaux", "Marine Hecquard", "Laurine Calas", "Margot Bonnet",
        "Zoe Morales", "Edwige Clad", "Eva Ancelin",
    ],
    "L1": [
        "Emy Chomon", "Thibaut Brachet", "Emilie Belguise", "Claire Henry",
        "Thomas Guibert", "Ella Thevenet", "Juliette Cazes", "Bertrand Cazabat",
    ],
    "L2": [
        "Line Desmazures", "Mila Gascoin", "Malek Adjroud", "Antoine Revardeau",
        "Helene Ristic", "Melissa Braun", "Eloise Lebert", "Charline Blas",
        "Margot Casties", "Amandine Godin", "Chloe Solanki", "Fabienne Pottelain",
        "Alexandra Mennechez", "Marion Hucbourg-Hernandez", "Constant Le Dantec",
        "Alexandre Paprocki", "Aurelie Guesnet", "Laury De-Oliveira", "Philippe Tkatchouk",
    ],
    "L3": [
        "Pauline Preynat", "Camille Lecomte", "Guillaume Dupont", "Camille Mollon",
        "Valerie Aleksandrowicz", "Lucie Collado", "Victoria Nouchi",
        "Samantha Philippon-Joubert", "Charlotte Garrido", "Laurine Ghis",
        "Yann Magueur", "Fanny Mangin", "Joumala Elalaoui", "Alice Saint-Jean",
    ],
}

# Aliases — JSON file uses some accented/different forms
ALIASES = {
    "Eloise Lebert": "Eloïse Lebert",
    "Chloe Solanki": "Chloé Solanki",
    "Marion Hucbourg-Hernandez": "Marion Hucbourg Hernandez",
    "Joumala Elalaoui": "Joumala ElAlaoui",
}

def parse_pct(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("%", "")
    try: return float(s)
    except ValueError: return None

def parse_num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("%", "")
    try: return float(s)
    except ValueError: return None

def parse_duration_to_minutes(v):
    """'2h 49m' -> 169"""
    if v is None: return None
    m = re.match(r"(?:(\d+)h)?\s*(?:(\d+)m)?", str(v).strip())
    if not m: return None
    h = int(m.group(1) or 0); mm = int(m.group(2) or 0)
    return h * 60 + mm if (h or mm) else None

def is_pct_str(v):
    return isinstance(v, str) and v.endswith("%")

def is_duration_str(v):
    return isinstance(v, str) and re.match(r"^\d+h\s*\d+m$", v.strip())

def clean_record(r):
    """Detect and fix shifted columns for the call/email block."""
    cpo = r.get("calls_per_opp")
    pco = r.get("pct_calls_over10min")
    adt = r.get("avg_daily_call_time")

    # Pattern: emails ok, but calls_per_opp is a "%", pct_calls is a duration
    # → shift: pct_calls_over10min was put in calls_per_opp, avg_daily_call_time was put in pct_calls_over10min
    if is_pct_str(cpo) and is_duration_str(pco) and adt is None:
        r["pct_calls_over10min"] = cpo
        r["avg_daily_call_time"] = pco
        r["calls_per_opp"] = None  # truly missing

    return r

def sane(v, lo, hi):
    if v is None: return None
    return v if lo <= v <= hi else None

def extract_metrics(rec):
    rec = clean_record(rec)
    raw = {
        "name": rec["agent"],
        "opps": parse_num(rec.get("opps")),
        "cr3_gross": parse_pct(rec.get("gross_cr3")),
        "cr3_net": parse_pct(rec.get("net_cr3")),
        # cycle days
        "sp_to_sc": parse_num(rec.get("supply_pend_to_conf")),
        "fc_to_offer": parse_num(rec.get("fc_to_offer")),
        "fc_to_oc": parse_num(rec.get("fc_to_oc")),
        "oc_to_pending": parse_num(rec.get("oc_to_pending")),
        "oc_to_rc": parse_num(rec.get("oc_to_rc")),
        "pending_to_conf": parse_num(rec.get("pending_to_conf")),
        # funnel %
        "fc_done": parse_pct(rec.get("fc_done_ratio")),
        "offer_made": parse_pct(rec.get("offer_made_ratio")),
        "oc_done": parse_pct(rec.get("oc_done_ratio")),
        "rc_done": parse_pct(rec.get("rc_done_ratio")),
        "first_oc_bkg": parse_pct(rec.get("oc_booking_ratio")),
        "rc_bkg": parse_pct(rec.get("rc_booking_ratio")),
        # pipeline
        "ps": parse_num(rec.get("stage_present_service")),
        "so": parse_num(rec.get("stage_send_offer")),
        "rc_count": parse_num(rec.get("stage_receive_conf")),
        "sp": parse_num(rec.get("stage_supply_pending")),
        # tasks
        "open_cases": parse_num(rec.get("open_cases")),
        "open_tasks": parse_num(rec.get("open_tasks")),
        # email/call (NEW)
        "emails_in": parse_num(rec.get("avg_inbound_emails")),
        "emails_out": parse_num(rec.get("avg_outbound_emails")),
        "emails_per_opp": parse_num(rec.get("emails_per_opp")),
        "calls_per_opp": parse_num(rec.get("calls_per_opp")),
        "pct_calls_over10": parse_pct(rec.get("pct_calls_over10min")),
        "daily_call_min": parse_duration_to_minutes(rec.get("avg_daily_call_time")),
    }
    # Sanitize: drop absurd/shifted values
    raw["opps"]            = sane(raw["opps"], 50, 1000)
    raw["cr3_gross"]       = sane(raw["cr3_gross"], 0, 100)
    raw["cr3_net"]         = sane(raw["cr3_net"], 0, 100)
    raw["fc_done"]         = sane(raw["fc_done"], 0, 100)
    raw["offer_made"]      = sane(raw["offer_made"], 0, 100)
    raw["oc_done"]         = sane(raw["oc_done"], 0, 100)
    raw["rc_done"]         = sane(raw["rc_done"], 0, 100)
    raw["first_oc_bkg"]    = sane(raw["first_oc_bkg"], 0, 100)
    raw["rc_bkg"]          = sane(raw["rc_bkg"], 0, 100)
    raw["emails_per_opp"]  = sane(raw["emails_per_opp"], 0, 50)
    raw["calls_per_opp"]   = sane(raw["calls_per_opp"], 0, 30)
    raw["pct_calls_over10"]= sane(raw["pct_calls_over10"], 0, 100)
    raw["daily_call_min"]  = sane(raw["daily_call_min"], 0, 600)
    raw["emails_in"]       = sane(raw["emails_in"], 0, 30)
    raw["emails_out"]      = sane(raw["emails_out"], 0, 30)
    return raw

def avg(values, default=None):
    vals = [v for v in values if v is not None]
    return mean(vals) if vals else default

def weighted_avg(values, weights):
    pairs = [(v, w) for v, w in zip(values, weights) if v is not None and w]
    if not pairs: return None
    tot_w = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / tot_w if tot_w else None

def summarize_level(level, agents_data):
    print(f"\n{'='*60}\n  LEVEL {level} — {len(agents_data)} agents\n{'='*60}")
    names = [a["name"] for a in agents_data]
    print("Agents:", ", ".join(names))

    opps_list = [a["opps"] for a in agents_data]
    total_opps = sum(o for o in opps_list if o)
    print(f"\nTotal opps: {total_opps:.0f}  |  Avg opps/agent: {total_opps/len(agents_data):.0f}")

    # CR3
    cr3_g = avg([a["cr3_gross"] for a in agents_data])
    cr3_n = avg([a["cr3_net"] for a in agents_data])
    cr3_w = weighted_avg([a["cr3_gross"] for a in agents_data], opps_list)
    print(f"CR3 gross (avg / weighted-by-opps): {cr3_g:.2f}% / {cr3_w:.2f}%")
    print(f"CR3 net   (avg): {cr3_n:.2f}%")

    # Funnel %
    print(f"\n--- FUNNEL ---")
    print(f"FC Done:        {avg([a['fc_done'] for a in agents_data]):.2f}%   (weighted {weighted_avg([a['fc_done'] for a in agents_data], opps_list):.2f}%)")
    print(f"Offer Made:     {avg([a['offer_made'] for a in agents_data]):.2f}%   (weighted {weighted_avg([a['offer_made'] for a in agents_data], opps_list):.2f}%)")
    print(f"OC Done:        {avg([a['oc_done'] for a in agents_data]):.2f}%   (weighted {weighted_avg([a['oc_done'] for a in agents_data], opps_list):.2f}%)")
    print(f"RC Done:        {avg([a['rc_done'] for a in agents_data]):.2f}%   (weighted {weighted_avg([a['rc_done'] for a in agents_data], opps_list):.2f}%)")
    print(f"First OC Bkg:   {avg([a['first_oc_bkg'] for a in agents_data]):.2f}%   (weighted {weighted_avg([a['first_oc_bkg'] for a in agents_data], opps_list):.2f}%)")
    print(f"RC Bkg:         {avg([a['rc_bkg'] for a in agents_data]):.2f}%")

    # Cycle days
    print(f"\n--- CYCLES (jours) ---")
    print(f"Supply Pend→Conf: {avg([a['sp_to_sc'] for a in agents_data]):.2f}j")
    print(f"FC→Offer:         {avg([a['fc_to_offer'] for a in agents_data]):.2f}j")
    print(f"FC→OC:            {avg([a['fc_to_oc'] for a in agents_data]):.2f}j")
    print(f"OC→Pending:       {avg([a['oc_to_pending'] for a in agents_data]):.2f}j")
    print(f"OC→RC:            {avg([a['oc_to_rc'] for a in agents_data]):.2f}j")
    print(f"Pending→Conf:     {avg([a['pending_to_conf'] for a in agents_data]):.2f}j")

    # Pipeline (sums)
    print(f"\n--- PIPELINE (snapshot, sommes) ---")
    print(f"PS  (Present Service):    {sum((a['ps'] or 0) for a in agents_data):.0f}")
    print(f"SO  (Send Offer):         {sum((a['so'] or 0) for a in agents_data):.0f}")
    print(f"RC  (Receive Conf):       {sum((a['rc_count'] or 0) for a in agents_data):.0f}")
    print(f"SP  (Supply Pending):     {sum((a['sp'] or 0) for a in agents_data):.0f}")
    print(f"Open Cases:                {sum((a['open_cases'] or 0) for a in agents_data):.0f}")
    print(f"Open Tasks:                {sum((a['open_tasks'] or 0) for a in agents_data):.0f}")
    # Per-agent averages
    print(f"PS/agent:   {avg([a['ps'] for a in agents_data]):.1f}  |  SO/agent: {avg([a['so'] for a in agents_data]):.1f}  |  RC/agent: {avg([a['rc_count'] for a in agents_data]):.1f}  |  SP/agent: {avg([a['sp'] for a in agents_data]):.1f}")

    # NEW: Email/Call patterns
    print(f"\n--- EMAILS & CALLS (NEW) ---")
    e_in = [a['emails_in'] for a in agents_data if a['emails_in'] is not None]
    e_out = [a['emails_out'] for a in agents_data if a['emails_out'] is not None]
    e_po = [a['emails_per_opp'] for a in agents_data if a['emails_per_opp'] is not None]
    c_po = [a['calls_per_opp'] for a in agents_data if a['calls_per_opp'] is not None]
    pct_short = [a['pct_calls_over10'] for a in agents_data if a['pct_calls_over10'] is not None]
    dcm = [a['daily_call_min'] for a in agents_data if a['daily_call_min'] is not None]

    def fmt(v, suffix="", prec=2): return f"{v:.{prec}f}{suffix}" if v is not None else "—"
    print(f"Avg inbound emails:    {fmt(avg(e_in))}  (n={len(e_in)})")
    print(f"Avg outbound emails:   {fmt(avg(e_out))}  (n={len(e_out)})")
    print(f"Emails / opp:          {fmt(avg(e_po))}  (n={len(e_po)})")
    print(f"Calls / opp:           {fmt(avg(c_po))}  (n={len(c_po)})")
    print(f"% calls >10min:        {fmt(avg(pct_short),'%',1)}  (n={len(pct_short)})")
    dcm_avg = avg(dcm)
    print(f"Daily call time:       {fmt(dcm_avg,' min',0)}  ({fmt(dcm_avg/60 if dcm_avg else None,'h',1)})  (n={len(dcm)})")

    return {
        "level": level,
        "n_agents": len(agents_data),
        "total_opps": total_opps,
        "agents": names,
    }

def main():
    with open(DATA_FILE) as f:
        data = json.load(f)
    by_name = {r["agent"]: r for r in data}

    summaries = {}
    for level, roster in LEVELS.items():
        records = []
        for name in roster:
            actual = ALIASES.get(name, name)
            if actual in by_name:
                records.append(extract_metrics(by_name[actual]))
            else:
                print(f"  ⚠ Missing in JSON: {level} / {name}")
        summaries[level] = summarize_level(level, records)

    # Cross-level patterns table
    print(f"\n\n{'='*60}\n  CROSS-LEVEL EMAIL/CALL PATTERNS\n{'='*60}")
    print(f"{'Level':<6} {'Agents':<8} {'CR3':<8} {'Em/opp':<10} {'Calls/opp':<12} {'%>10min':<10} {'Daily call':<12}")
    for level, roster in LEVELS.items():
        records = [extract_metrics(by_name[ALIASES.get(n, n)]) for n in roster if ALIASES.get(n, n) in by_name]
        cr3 = avg([a['cr3_gross'] for a in records]) or 0
        epo = avg([a['emails_per_opp'] for a in records if a['emails_per_opp'] is not None]) or 0
        cpo = avg([a['calls_per_opp'] for a in records if a['calls_per_opp'] is not None]) or 0
        pct = avg([a['pct_calls_over10'] for a in records if a['pct_calls_over10'] is not None]) or 0
        dcm = avg([a['daily_call_min'] for a in records if a['daily_call_min'] is not None]) or 0
        print(f"{level:<6} {len(records):<8} {cr3:.1f}%   {epo:.1f}      {cpo:.1f}        {pct:.1f}%     {int(dcm)}min ({dcm/60:.1f}h)")

if __name__ == "__main__":
    main()
