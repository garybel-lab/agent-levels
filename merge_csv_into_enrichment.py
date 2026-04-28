#!/usr/bin/env python3
"""Merge calls_per_opp.csv + emails_per_opp.csv into agent_enrichment.json
and recompute level_benchmarks.json."""
import csv, json, re, unicodedata
from statistics import mean

ROOT = "/workspaces/agent-levels"
CALLS_CSV    = f"{ROOT}/calls_per_opp.csv"
EMAILS_CSV   = f"{ROOT}/emails_per_opp.csv"
ENRICH_FILE  = f"{ROOT}/agent_enrichment.json"
BENCH_FILE   = f"{ROOT}/level_benchmarks.json"

# All agent IDs that appear in window._agData_* (panel-side IDs we want to populate)
AGENT_IDS = [
    "victoria_nouchi","samantha_philippon_joubert","emy_chomon","claire_lardenois",
    "zoe_morales","laurine_ghis","fanny_mangin","margot_casties","antoine_revardeau",
    "constant_le_dantec","fiona_tandar","camille_bouvier","chloe_solanki","thomas_dufrene",
    "thibaut_brachet","ella_thevenet","alice_saint_jean","valerie_aleksandrowicz",
    "juliette_cazes","lucie_collado","laury_de_oliveira","elise_francois","joumala_elalaoui",
    "yann_magueur","melissa_braun","helene_ristic","marceau_cattin","charlotte_garrido",
    "pauline_preynat","emilie_belguise","fabienne_pottelain","alexandra_mennechez",
    "malek_adjroud","bertrand_cazabat","solene_billon","camille_mollon","mila_gascoin",
    "marion_hucbourg_hernandez","aurelie_guesnet","alexandre_paprocki","charline_blas",
    "marion_ambroisine_quinard","margot_bonnet","line_desmazures","camille_lecomte",
    "guillaume_dupont","claire_henry","eloise_lebert","philippe_tkatchouk","amandine_godin",
    "edwige_clad","florence_touyard","eva_ancelin","laurine_calas","marine_hecquard",
    "thomas_guibert","charline_verdeaux",
]

def normalize(s):
    """Normalize a name: lowercase, strip accents, hyphens->spaces, then spaces->underscore."""
    s = s.strip()
    s = unicodedata.normalize('NFKD', s).encode('ASCII','ignore').decode('ASCII')
    s = s.lower()
    s = s.replace("-", " ")  # hyphen -> space (so Saint-Jean → saint jean → saint_jean)
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s

# Manual aliases for tricky cases
ALIASES = {
    "joumala_el_alaoui_ismaili": "joumala_elalaoui",
    "joumala_elalaoui": "joumala_elalaoui",
    "margot_jeanne_catherine_casties": "margot_casties",
    "samantha_philippon_joubert": "samantha_philippon_joubert",
    "samantha_philipponjoubert": "samantha_philippon_joubert",
    "marion_ambroisine_quinard": "marion_ambroisine_quinard",
}

def to_id(name):
    n = normalize(name)
    if n in ALIASES: return ALIASES[n]
    return n

def parse_pct(v):
    if v is None or v == "": return None
    return float(str(v).replace("%","").replace(",","").strip())

def parse_dur(v):
    """'2h 23m' -> 143"""
    if not v: return None
    m = re.match(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?", str(v).strip())
    if not m: return None
    h = int(m.group(1) or 0); mm = int(m.group(2) or 0)
    return h*60 + mm if (h or mm) else None

def parse_num(v):
    if v is None or v == "": return None
    return float(str(v).replace(",","").strip())

# === LOAD CALLS ===
calls_data = {}
with open(CALLS_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        aid = to_id(row["Agent Name"])
        calls_data[aid] = {
            "calls_per_opp": parse_num(row["Average amount of Call per opps"]),
            "pct_calls_long": parse_pct(row["Pct Call Done Longer Than 10min"]),
            "daily_call_min": parse_dur(row["Avg Daily Call Time (mins)"]),
            "_calls_total":   parse_num(row["Sum Calls Raw"]),
            "_opps_with_call": parse_num(row["Opps with Call Done"]),
        }

# === LOAD EMAILS ===
emails_data = {}
with open(EMAILS_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        aid = to_id(row["Name"])
        emails_data[aid] = {
            "emails_in":      parse_num(row["Avr Inbound Emails"]),
            "emails_out":     parse_num(row["Avr Outbound Emails"]),
            "emails_per_opp": parse_num(row["Total"]),
        }

# === LOAD EXISTING ENRICHMENT ===
with open(ENRICH_FILE) as f:
    enrich = json.load(f)

# === MATCH & UPDATE ===
matched_calls = 0
matched_emails = 0
unmatched_in_csv_calls = []
unmatched_in_csv_emails = []
missing_after_merge = []

for aid in AGENT_IDS:
    if aid not in enrich:
        # Create blank entry for agents that weren't in the original Looker enrichment
        enrich[aid] = {
            "cyc_sp_to_sc": None, "cyc_fc_to_offer": None, "cyc_fc_to_oc": None,
            "cyc_oc_to_pending": None, "cyc_oc_to_rc": None, "cyc_pending_to_conf": None,
            "emails_in": None, "emails_out": None, "emails_per_opp": None,
            "calls_per_opp": None, "pct_calls_long": None, "daily_call_min": None,
            "open_cases": None, "open_tasks": None,
            "looker_opps": None, "looker_cr3": None,
            "stage_ps": None, "stage_so": None, "stage_rc": None, "stage_sp": None,
            "fc_done_ratio": None, "offer_made_ratio": None, "oc_done_ratio": None,
            "rc_done_ratio": None, "oc_booking_ratio": None, "rc_booking_ratio": None,
        }
    if aid in calls_data:
        d = calls_data[aid]
        for k in ("calls_per_opp","pct_calls_long","daily_call_min"):
            if d[k] is not None:
                enrich[aid][k] = d[k]
        matched_calls += 1
    else:
        missing_after_merge.append((aid, "calls"))
    if aid in emails_data:
        d = emails_data[aid]
        for k in ("emails_in","emails_out","emails_per_opp"):
            if d[k] is not None:
                enrich[aid][k] = d[k]
        matched_emails += 1
    else:
        missing_after_merge.append((aid, "emails"))

# Identify CSV rows with no matching agent (TLs etc)
calls_set = set(calls_data.keys()); emails_set = set(emails_data.keys()); ag_set = set(AGENT_IDS)
unmatched_in_csv_calls = sorted(calls_set - ag_set)
unmatched_in_csv_emails = sorted(emails_set - ag_set)

# === RECOMPUTE BENCHMARKS ===
LEVEL_AGENTS = {
    "L0": ["thomas_dufrene","elise_francois","florence_touyard","claire_lardenois","charline_verdeaux","marine_hecquard","laurine_calas","margot_bonnet","zoe_morales","edwige_clad","eva_ancelin"],
    "L1": ["emy_chomon","thibaut_brachet","emilie_belguise","claire_henry","thomas_guibert","ella_thevenet","juliette_cazes","bertrand_cazabat"],
    "L2": ["line_desmazures","mila_gascoin","malek_adjroud","antoine_revardeau","helene_ristic","melissa_braun","eloise_lebert","charline_blas","margot_casties","amandine_godin","chloe_solanki","fabienne_pottelain","alexandra_mennechez","marion_hucbourg_hernandez","constant_le_dantec","alexandre_paprocki","aurelie_guesnet","laury_de_oliveira","philippe_tkatchouk"],
    "L3": ["pauline_preynat","camille_lecomte","guillaume_dupont","camille_mollon","valerie_aleksandrowicz","lucie_collado","victoria_nouchi","samantha_philippon_joubert","charlotte_garrido","laurine_ghis","yann_magueur","fanny_mangin","joumala_elalaoui","alice_saint_jean"],
}

FIELDS = ["cyc_sp_to_sc","cyc_fc_to_offer","cyc_fc_to_oc","cyc_oc_to_pending","cyc_oc_to_rc","cyc_pending_to_conf",
          "emails_in","emails_out","emails_per_opp","calls_per_opp","pct_calls_long","daily_call_min",
          "fc_done_ratio","offer_made_ratio","oc_done_ratio","rc_done_ratio","oc_booking_ratio","rc_booking_ratio"]

def avg(vals):
    v = [x for x in vals if x is not None]
    return round(mean(v), 2) if v else 0

bench = {}
canonical = sum(LEVEL_AGENTS.values(), [])
bench["MARKET"] = {f: avg([enrich.get(a,{}).get(f) for a in canonical]) for f in FIELDS}
bench["MARKET"]["n_agents"] = len(canonical)
for lvl, names in LEVEL_AGENTS.items():
    bench[lvl] = {f: avg([enrich.get(a,{}).get(f) for a in names]) for f in FIELDS}
    bench[lvl]["n_agents"] = len(names)

# Round enrichment values
for aid, fields in enrich.items():
    for k, v in fields.items():
        if isinstance(v, float):
            fields[k] = round(v, 2)

with open(ENRICH_FILE, "w") as f: json.dump(enrich, f, ensure_ascii=False, indent=0)
with open(BENCH_FILE, "w") as f: json.dump(bench, f, ensure_ascii=False, indent=2)

print(f"✓ Calls matched : {matched_calls} / {len(AGENT_IDS)}")
print(f"✓ Emails matched: {matched_emails} / {len(AGENT_IDS)}")
print(f"\nCSV rows not matched to any agent (TLs / off-list, normal):")
print(f"  calls : {unmatched_in_csv_calls}")
print(f"  emails: {len(unmatched_in_csv_emails)} entries (TLs and other staff)")

still_missing = sorted(set(aid for aid,_ in missing_after_merge))
if still_missing:
    print(f"\n⚠ Agents missing data after merge:")
    for aid in still_missing:
        miss = [k for k in ('emails_per_opp','calls_per_opp') if enrich.get(aid,{}).get(k) is None]
        if miss: print(f"  {aid}: missing {miss}")

print(f"\nUpdated MARKET benchmarks (key comm metrics):")
for k in ("emails_per_opp","calls_per_opp","pct_calls_long","daily_call_min"):
    print(f"  {k:<22} = {bench['MARKET'][k]}")
print(f"\nUpdated LEVEL benchmarks:")
for lvl in ("L0","L1","L2","L3"):
    b = bench[lvl]
    print(f"  {lvl}: em/opp={b['emails_per_opp']} | calls/opp={b['calls_per_opp']} | %long={b['pct_calls_long']}% | dailycall={b['daily_call_min']}min")
