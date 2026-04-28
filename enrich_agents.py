#!/usr/bin/env python3
"""Enrich window._agData_* blobs with new Looker fields + compute level/market benchmarks.

Outputs:
- /workspaces/agent-levels/agent_enrichment.json  (mapping agent_id -> new fields)
- /workspaces/agent-levels/level_benchmarks.json  (per-level + market averages)
"""
import json
import re
from statistics import mean

DATA_FILE = "/workspaces/agent-levels/agents_data_q1_2026.json"
OUT_ENRICH = "/workspaces/agent-levels/agent_enrichment.json"
OUT_BENCH  = "/workspaces/agent-levels/level_benchmarks.json"

# Map names from JSON file to agent_id used in window._agData_*
NAME_TO_ID = {
    "Thomas Dufrene": "thomas_dufrene",
    "Elise Francois": "elise_francois",
    "Florence Touyard": "florence_touyard",
    "Claire Lardenois": "claire_lardenois",
    "Charline Verdeaux": "charline_verdeaux",
    "Marine Hecquard": "marine_hecquard",
    "Laurine Calas": "laurine_calas",
    "Margot Bonnet": "margot_bonnet",
    "Zoe Morales": "zoe_morales",
    "Edwige Clad": "edwige_clad",
    "Eva Ancelin": "eva_ancelin",
    "Emy Chomon": "emy_chomon",
    "Thibaut Brachet": "thibaut_brachet",
    "Emilie Belguise": "emilie_belguise",
    "Claire Henry": "claire_henry",
    "Thomas Guibert": "thomas_guibert",
    "Ella Thevenet": "ella_thevenet",
    "Juliette Cazes": "juliette_cazes",
    "Bertrand Cazabat": "bertrand_cazabat",
    "Line Desmazures": "line_desmazures",
    "Mila Gascoin": "mila_gascoin",
    "Malek Adjroud": "malek_adjroud",
    "Antoine Revardeau": "antoine_revardeau",
    "Helene Ristic": "helene_ristic",
    "Melissa Braun": "melissa_braun",
    "Eloïse Lebert": "eloise_lebert",
    "Charline Blas": "charline_blas",
    "Margot Casties": "margot_casties",
    "Amandine Godin": "amandine_godin",
    "Chloé Solanki": "chloe_solanki",
    "Fabienne Pottelain": "fabienne_pottelain",
    "Alexandra Mennechez": "alexandra_mennechez",
    "Marion Hucbourg Hernandez": "marion_hucbourg_hernandez",
    "Constant Le Dantec": "constant_le_dantec",
    "Alexandre Paprocki": "alexandre_paprocki",
    "Aurelie Guesnet": "aurelie_guesnet",
    "Laury De-Oliveira": "laury_de_oliveira",
    "Philippe Tkatchouk": "philippe_tkatchouk",
    "Pauline Preynat": "pauline_preynat",
    "Camille Lecomte": "camille_lecomte",
    "Guillaume Dupont": "guillaume_dupont",
    "Camille Mollon": "camille_mollon",
    "Valerie Aleksandrowicz": "valerie_aleksandrowicz",
    "Lucie Collado": "lucie_collado",
    "Victoria Nouchi": "victoria_nouchi",
    "Samantha Philippon-Joubert": "samantha_philippon_joubert",
    "Charlotte Garrido": "charlotte_garrido",
    "Laurine Ghis": "laurine_ghis",
    "Yann Magueur": "yann_magueur",
    "Fanny Mangin": "fanny_mangin",
    "Joumala ElAlaoui": "joumala_elalaoui",
    "Alice Saint-Jean": "alice_saint_jean",
}

LEVELS = {
    "L0": ["Thomas Dufrene","Elise Francois","Florence Touyard","Claire Lardenois","Charline Verdeaux","Marine Hecquard","Laurine Calas","Margot Bonnet","Zoe Morales","Edwige Clad","Eva Ancelin"],
    "L1": ["Emy Chomon","Thibaut Brachet","Emilie Belguise","Claire Henry","Thomas Guibert","Ella Thevenet","Juliette Cazes","Bertrand Cazabat"],
    "L2": ["Line Desmazures","Mila Gascoin","Malek Adjroud","Antoine Revardeau","Helene Ristic","Melissa Braun","Eloïse Lebert","Charline Blas","Margot Casties","Amandine Godin","Chloé Solanki","Fabienne Pottelain","Alexandra Mennechez","Marion Hucbourg Hernandez","Constant Le Dantec","Alexandre Paprocki","Aurelie Guesnet","Laury De-Oliveira","Philippe Tkatchouk"],
    "L3": ["Pauline Preynat","Camille Lecomte","Guillaume Dupont","Camille Mollon","Valerie Aleksandrowicz","Lucie Collado","Victoria Nouchi","Samantha Philippon-Joubert","Charlotte Garrido","Laurine Ghis","Yann Magueur","Fanny Mangin","Joumala ElAlaoui","Alice Saint-Jean"],
}

def parse_pct(v):
    if v is None: return None
    if isinstance(v,(int,float)): return float(v)
    s = str(v).strip().replace("%","")
    try: return float(s)
    except: return None

def parse_num(v):
    if v is None: return None
    if isinstance(v,(int,float)): return float(v)
    s = str(v).strip().replace("%","")
    try: return float(s)
    except: return None

def parse_dur_min(v):
    if v is None: return None
    m = re.match(r"(?:(\d+)h)?\s*(?:(\d+)m)?", str(v).strip())
    if not m: return None
    h = int(m.group(1) or 0); mm = int(m.group(2) or 0)
    return h*60+mm if (h or mm) else None

def is_pct_str(v): return isinstance(v,str) and v.endswith("%")
def is_dur_str(v): return isinstance(v,str) and re.match(r"^\d+h\s*\d+m$", v.strip())

def fix_shift(r):
    """Detect shifted columns in calls block and reattribute."""
    cpo = r.get("calls_per_opp")
    pco = r.get("pct_calls_over10min")
    adt = r.get("avg_daily_call_time")
    if is_pct_str(cpo) and is_dur_str(pco) and adt is None:
        r["pct_calls_over10min"] = cpo
        r["avg_daily_call_time"] = pco
        r["calls_per_opp"] = None
    return r

def sane(v, lo, hi):
    if v is None: return None
    return v if lo <= v <= hi else None

def extract(rec):
    rec = fix_shift(rec)
    return {
        "cyc_sp_to_sc":     sane(parse_num(rec.get("supply_pend_to_conf")), 0, 60),
        "cyc_fc_to_offer":  sane(parse_num(rec.get("fc_to_offer")), 0, 30),
        "cyc_fc_to_oc":     sane(parse_num(rec.get("fc_to_oc")), 0, 30),
        "cyc_oc_to_pending":sane(parse_num(rec.get("oc_to_pending")), 0, 30),
        "cyc_oc_to_rc":     sane(parse_num(rec.get("oc_to_rc")), 0, 60),
        "cyc_pending_to_conf":sane(parse_num(rec.get("pending_to_conf")), 0, 60),
        "emails_in":   sane(parse_num(rec.get("avg_inbound_emails")),  0, 30),
        "emails_out":  sane(parse_num(rec.get("avg_outbound_emails")), 0, 30),
        "emails_per_opp": sane(parse_num(rec.get("emails_per_opp")), 0, 50),
        "calls_per_opp":  sane(parse_num(rec.get("calls_per_opp")),  0, 30),
        "pct_calls_long": sane(parse_pct(rec.get("pct_calls_over10min")), 0, 100),
        "daily_call_min": sane(parse_dur_min(rec.get("avg_daily_call_time")), 0, 600),
        "open_cases":  sane(parse_num(rec.get("open_cases")), 0, 500),
        "open_tasks":  sane(parse_num(rec.get("open_tasks")), 0, 500),
        "looker_opps": sane(parse_num(rec.get("opps")), 30, 1000),
        "looker_cr3":  sane(parse_pct(rec.get("gross_cr3")), 0, 100),
        "stage_ps":    sane(parse_num(rec.get("stage_present_service")), 0, 200),
        "stage_so":    sane(parse_num(rec.get("stage_send_offer")), 0, 200),
        "stage_rc":    sane(parse_num(rec.get("stage_receive_conf")), 0, 200),
        "stage_sp":    sane(parse_num(rec.get("stage_supply_pending")), 0, 200),
        "fc_done_ratio": sane(parse_pct(rec.get("fc_done_ratio")), 0, 100),
        "offer_made_ratio": sane(parse_pct(rec.get("offer_made_ratio")), 0, 100),
        "oc_done_ratio": sane(parse_pct(rec.get("oc_done_ratio")), 0, 100),
        "rc_done_ratio": sane(parse_pct(rec.get("rc_done_ratio")), 0, 100),
        "oc_booking_ratio": sane(parse_pct(rec.get("oc_booking_ratio")), 0, 100),
        "rc_booking_ratio": sane(parse_pct(rec.get("rc_booking_ratio")), 0, 100),
    }

def avg(vals):
    v = [x for x in vals if x is not None]
    return sum(v)/len(v) if v else None

def main():
    with open(DATA_FILE) as f: raw = json.load(f)
    by_name = {r["agent"]: r for r in raw}

    enrich = {}
    for name, agent_id in NAME_TO_ID.items():
        if name in by_name:
            enrich[agent_id] = extract(by_name[name])

    # Per-level + market benchmarks
    bench = {}
    all_recs = list(enrich.values())
    fields = ["cyc_sp_to_sc","cyc_fc_to_offer","cyc_fc_to_oc","cyc_oc_to_pending","cyc_oc_to_rc","cyc_pending_to_conf",
              "emails_in","emails_out","emails_per_opp","calls_per_opp","pct_calls_long","daily_call_min",
              "fc_done_ratio","offer_made_ratio","oc_done_ratio","rc_done_ratio","oc_booking_ratio","rc_booking_ratio"]

    bench["MARKET"] = {f: round(avg([r[f] for r in all_recs]) or 0, 2) for f in fields}
    bench["MARKET"]["n_agents"] = len(all_recs)

    for lvl, names in LEVELS.items():
        recs = [enrich[NAME_TO_ID[n]] for n in names if NAME_TO_ID[n] in enrich]
        bench[lvl] = {f: round(avg([r[f] for r in recs]) or 0, 2) for f in fields}
        bench[lvl]["n_agents"] = len(recs)

    # Round enrichment values for cleaner JSON
    for aid, fields_map in enrich.items():
        for k, v in fields_map.items():
            if isinstance(v, float):
                fields_map[k] = round(v, 2)

    with open(OUT_ENRICH, "w") as f: json.dump(enrich, f, ensure_ascii=False, indent=0)
    with open(OUT_BENCH, "w") as f: json.dump(bench, f, ensure_ascii=False, indent=2)

    print(f"✓ {len(enrich)} agents enriched -> {OUT_ENRICH}")
    print(f"✓ benchmarks (MARKET + 4 levels) -> {OUT_BENCH}")
    print("\nMarket benchmarks:")
    for k in fields:
        print(f"  {k:<22} = {bench['MARKET'][k]}")
    print("\nLevel benchmarks (calls/opp, pct_calls_long, daily_call_min, emails_per_opp):")
    for lvl in ["L0","L1","L2","L3"]:
        print(f"  {lvl}: calls/opp={bench[lvl]['calls_per_opp']} | %long={bench[lvl]['pct_calls_long']}% | dailycall={bench[lvl]['daily_call_min']}min | em/opp={bench[lvl]['emails_per_opp']}")

if __name__ == "__main__":
    main()
