#!/usr/bin/env python3
"""Deep pattern analysis: find strongest level-differentiators across all available metrics."""
import json
from statistics import mean, stdev

ROOT = "/workspaces/agent-levels"
with open(f"{ROOT}/agent_enrichment.json") as f: enrich = json.load(f)
with open(f"{ROOT}/level_benchmarks.json") as f: bench = json.load(f)

LEVEL_AGENTS = {
    "L0": ["thomas_dufrene","elise_francois","florence_touyard","claire_lardenois","charline_verdeaux","marine_hecquard","laurine_calas","margot_bonnet","zoe_morales","edwige_clad","eva_ancelin"],
    "L1": ["emy_chomon","thibaut_brachet","emilie_belguise","claire_henry","thomas_guibert","ella_thevenet","juliette_cazes","bertrand_cazabat"],
    "L2": ["line_desmazures","mila_gascoin","malek_adjroud","antoine_revardeau","helene_ristic","melissa_braun","eloise_lebert","charline_blas","margot_casties","amandine_godin","chloe_solanki","fabienne_pottelain","alexandra_mennechez","marion_hucbourg_hernandez","constant_le_dantec","alexandre_paprocki","aurelie_guesnet","laury_de_oliveira","philippe_tkatchouk"],
    "L3": ["pauline_preynat","camille_lecomte","guillaume_dupont","camille_mollon","valerie_aleksandrowicz","lucie_collado","victoria_nouchi","samantha_philippon_joubert","charlotte_garrido","laurine_ghis","yann_magueur","fanny_mangin","joumala_elalaoui","alice_saint_jean"],
}

ALL_FIELDS = [
    ("cyc_sp_to_sc",         "Supply Pend → Conf",       "j",  True),   # lower better
    ("cyc_fc_to_offer",      "FC → Offer",               "j",  True),
    ("cyc_fc_to_oc",         "FC → OC",                  "j",  True),
    ("cyc_oc_to_pending",    "OC → Pending",             "j",  True),
    ("cyc_oc_to_rc",         "OC → RC",                  "j",  True),
    ("cyc_pending_to_conf",  "Pending → Conf",           "j",  True),
    ("emails_in",            "Emails inbound",           "",   False),
    ("emails_out",           "Emails outbound",          "",   False),
    ("emails_per_opp",       "Emails / opp",             "",   False),
    ("calls_per_opp",        "Calls / opp",              "",   False),
    ("pct_calls_long",       "% calls >10min",           "%",  None),  # context-dependent
    ("daily_call_min",       "Daily call time",          "min",False),
    ("fc_done_ratio",        "FC Done %",                "%",  False),
    ("offer_made_ratio",     "Offer Made %",             "%",  False),
    ("oc_done_ratio",        "OC Done %",                "%",  False),
    ("rc_done_ratio",        "RC Done %",                "%",  False),
    ("oc_booking_ratio",     "1er OC Bkg %",             "%",  False),
    ("rc_booking_ratio",     "RC Bkg %",                 "%",  False),
    ("looker_cr3",           "CR3 gross",                "%",  False),
    ("open_cases",           "Open Cases",               "",   True),
    ("open_tasks",           "Open Tasks",               "",   True),
]

def avg_filter(vals):
    v = [x for x in vals if x is not None]
    return mean(v) if v else None

# 1. Per-level per-field averages
print("=" * 100)
print("LEVEL × METRIC matrix (sorted by gap L0→L3)")
print("=" * 100)
print(f"{'Metric':<28} {'L0':>10} {'L1':>10} {'L2':>10} {'L3':>10} {'Mkt':>10}  {'Gap L0→L3':>12}  Verdict")
print("-" * 110)

rows = []
for fkey, flabel, unit, lower_better in ALL_FIELDS:
    lvl_avgs = {}
    for lvl, agents in LEVEL_AGENTS.items():
        lvl_avgs[lvl] = avg_filter([enrich.get(a,{}).get(fkey) for a in agents])
    mkt = avg_filter([enrich.get(a,{}).get(fkey) for ags in LEVEL_AGENTS.values() for a in ags])
    if all(v is None for v in lvl_avgs.values()): continue
    l0 = lvl_avgs["L0"] or 0; l3 = lvl_avgs["L3"] or 0
    gap = l3 - l0
    rows.append((fkey, flabel, lvl_avgs, mkt, gap, lower_better))

# Sort by absolute gap
rows.sort(key=lambda x: abs(x[4]), reverse=True)

for fkey, flabel, lvl_avgs, mkt, gap, lower_better in rows:
    l0 = lvl_avgs.get("L0") or 0
    l1 = lvl_avgs.get("L1") or 0
    l2 = lvl_avgs.get("L2") or 0
    l3 = lvl_avgs.get("L3") or 0
    mkt_v = mkt or 0
    if lower_better is True:
        verdict = "L3 BETTER (lower)" if l3 < l0 else "L3 worse"
    elif lower_better is False:
        verdict = "L3 BETTER (higher)" if l3 > l0 else "L3 worse"
    else:
        verdict = "context"
    print(f"{flabel:<28} {l0:>10.2f} {l1:>10.2f} {l2:>10.2f} {l3:>10.2f} {mkt_v:>10.2f}  {gap:>+12.2f}  {verdict}")

# 2. Strong differentiators: gap > 1.5 std
print()
print("=" * 100)
print("STRONGEST PATTERNS (gap × significance)")
print("=" * 100)

pattern_rows = []
for fkey, flabel, lvl_avgs, mkt, gap, lower_better in rows:
    # Spread between levels (max - min)
    vals = [v for v in lvl_avgs.values() if v is not None]
    if len(vals) < 4: continue
    spread = max(vals) - min(vals)
    avg_val = mean(vals)
    rel_spread = spread / avg_val if avg_val else 0
    pattern_rows.append((fkey, flabel, spread, rel_spread, lvl_avgs, lower_better))

pattern_rows.sort(key=lambda x: x[3], reverse=True)
for fkey, flabel, spread, rel_spread, lvl_avgs, lower_better in pattern_rows[:12]:
    sorted_levels = sorted(lvl_avgs.items(), key=lambda x: x[1] if x[1] is not None else 0)
    chain = " < ".join(f"{l}({v:.1f})" for l,v in sorted_levels)
    print(f"{flabel:<26} | spread={spread:>6.2f} ({rel_spread*100:>4.0f}%) | order: {chain}")

# 3. Top performers per level (for narrative)
print()
print("=" * 100)
print("TOP/BOTTOM PERFORMERS PER LEVEL (CR3 ranked)")
print("=" * 100)
for lvl, agents in LEVEL_AGENTS.items():
    ranked = sorted([(a, enrich.get(a,{}).get("looker_cr3", 0) or 0) for a in agents], key=lambda x:-x[1])
    print(f"\n{lvl}:")
    print(f"  TOP:    {ranked[0][0]:<30} CR3={ranked[0][1]:.1f}%")
    print(f"  TOP 2:  {ranked[1][0]:<30} CR3={ranked[1][1]:.1f}%")
    print(f"  BOT:    {ranked[-1][0]:<30} CR3={ranked[-1][1]:.1f}%")

# 4. Outliers (agents in lvl that look like another lvl)
print()
print("=" * 100)
print("AGENTS WHO LOOK LIKE A DIFFERENT LEVEL (CR3-based candidates for promotion/demotion)")
print("=" * 100)
LEVEL_THRESHOLDS = {"L0":(0,21), "L1":(21,24), "L2":(24,27), "L3":(27,100)}
for lvl, agents in LEVEL_AGENTS.items():
    lo, hi = LEVEL_THRESHOLDS[lvl]
    promotable = []
    demotable = []
    for a in agents:
        cr3 = enrich.get(a,{}).get("looker_cr3")
        if cr3 is None: continue
        if cr3 >= hi + 1: promotable.append((a, cr3))
        if cr3 < lo - 0: demotable.append((a, cr3))
    if promotable: print(f"\n{lvl} ↑ promotable (CR3 > L{int(lvl[1])+1} threshold):")
    for a,c in sorted(promotable, key=lambda x:-x[1])[:3]:
        print(f"  {a:<30} CR3={c:.1f}%")
    if demotable: print(f"\n{lvl} ↓ underperforming:")
    for a,c in sorted(demotable, key=lambda x:x[1])[:3]:
        print(f"  {a:<30} CR3={c:.1f}%")
