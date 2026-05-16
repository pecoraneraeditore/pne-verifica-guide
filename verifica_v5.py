#!/usr/bin/env python3
"""
verifica_v5.py — La Pecora Nera — Versione 5.0
SerpAPI (Google Maps) + Web Scraping (The Fork / Facebook / Instagram)

Variabili d'ambiente richieste:
  SERPAPI_KEY    — chiave API SerpAPI
  BREVO_API_KEY  — chiave API Brevo
  GITHUB_TOKEN   — Personal Access Token GitHub (scope: repo)
  GITHUB_REPO    — es. "pecoraneraeditore/pne-verifica-guide"
"""

import os, sys, time, base64, re, shutil, subprocess
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import openpyxl
from openpyxl.styles import PatternFill, Font
import pandas as pd

# ── Configurazione ───────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = SCRIPT_DIR
SERPAPI_KEY   = os.environ.get("SERPAPI_KEY",  "5f1b668b04050150d1280e8d2608b98b8ac1f246fe86ef3ed6d5a914e2aa3c09")
BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN",  "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO",   "")
SENDER_EMAIL  = "s.cargiani@lapecoranera.net"
RECIPIENTS    = [{"email": "s.cargiani@lapecoranera.net"},
                 {"email": "f.darienzo@lapecoranera.net"}]
today_str     = datetime.now().strftime("%d/%m/%Y")

# Ricerche SerpAPI per run (max 250/mese — 2 run/sett → ~8/mese → ~30/run)
MAX_PER_CITY  = 10

# ── Utility ──────────────────────────────────────────────────────────────────
def _str(v, d=""):
    if v is None: return d
    s = str(v).strip()
    return d if s in ("nan", "None", "") else s

# ── SerpAPI: Google Maps ─────────────────────────────────────────────────────
def verifica_google_maps(nome, citta):
    try:
        params = {
            "engine": "google",
            "q": f"{nome} ristorante {citta}",
            "api_key": SERPAPI_KEY,
            "hl": "it", "gl": "it", "num": 3,
        }
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        data = resp.json()
        res = {"status": "INCERTO", "orari_ap": "", "orari_ch": "",
               "rating": "", "reviews": "",
               "ultima_rev": str(datetime.now().year), "note": ""}

        kg = data.get("knowledge_graph", {})
        if kg.get("rating"):  res["rating"]   = str(kg["rating"])
        if kg.get("reviews"): res["reviews"]  = str(kg["reviews"])
        if kg.get("hours"):   res["orari_ap"] = str(kg["hours"])[:80]

        for lr in data.get("local_results", [])[:1]:
            if lr.get("rating"):  res["rating"]   = str(lr["rating"])
            if lr.get("reviews"): res["reviews"]  = str(lr["reviews"])
            if lr.get("hours"):   res["orari_ap"] = str(lr.get("hours", ""))[:80]
            os_ = lr.get("open_state", "").lower()
            if any(w in os_ for w in ["aperto", "open", "chiude", "closes", "apre alle"]):
                res["status"] = "APERTO"
            elif "permanentemente" in os_ or "permanently closed" in os_:
                res["status"] = "CHIUSO"

        # Se troviamo rating → quasi certamente aperto
        if res["rating"] and res["status"] == "INCERTO":
            res["status"] = "APERTO"
            res["note"]   = f"Rating Google: {res['rating']} ({res['reviews']} rec.)"

        # Analisi organic results
        for org in data.get("organic_results", [])[:3]:
            snip = (org.get("snippet", "") + " " + org.get("title", "")).lower()
            if "chiuso definitivamente" in snip or "permanently closed" in snip:
                res["status"] = "CHIUSO"
                res["note"]   = "Chiuso definitivamente (ricerca web)"
                break
            if res["status"] == "INCERTO" and any(
                    w in snip for w in ["2026", "2027", "prenota", "menu", "reservations"]):
                res["status"] = "APERTO"
                res["note"]   = "Trovato online con menzione recente"

        time.sleep(1.2)
        return res
    except Exception as e:
        time.sleep(1)
        return {"status": "INCERTO", "orari_ap": "", "orari_ch": "",
                "rating": "", "reviews": "", "ultima_rev": "",
                "note": f"Err SerpAPI: {str(e)[:40]}"}

# ── Web scraping: The Fork ───────────────────────────────────────────────────
def verifica_the_fork(nome, citta):
    try:
        url = (f"https://www.thefork.it/search?"
               f"q={nome.replace(' ', '+')}+{citta.replace(' ', '+')}")
        resp = requests.get(url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                     "Accept-Language": "it-IT,it;q=0.9"},
            timeout=10)
        text = resp.text.lower()
        trovato = (nome.lower()[:12] in text or "prenota" in text) and len(resp.text) > 3000
        rating  = ""
        m = re.search(r'\b([89]\.\d|10\.0|7\.[5-9])\b', text)
        if m: rating = m.group(1)
        time.sleep(1)
        return {"trovato": trovato, "rating": rating,
                "note": "TF: trovato" if trovato else "TF: non trovato"}
    except Exception as e:
        time.sleep(1)
        return {"trovato": False, "rating": "", "note": f"TF err: {str(e)[:30]}"}

# ── Web scraping: Facebook ───────────────────────────────────────────────────
def verifica_facebook(handle):
    if not handle or str(handle).strip() in ("", "nan", "None"):
        return {"trovato": False, "note": "FB: nessun handle"}
    try:
        h = str(handle).strip().lstrip("@")
        resp = requests.get(f"https://www.facebook.com/{h}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=10, allow_redirects=True)
        text = resp.text.lower()
        trovato = (resp.status_code == 200 and len(resp.text) > 5000
                   and "page not found" not in text
                   and "pagina non trovata" not in text
                   and "questa pagina non è disponibile" not in text)
        time.sleep(1)
        return {"trovato": trovato, "note": f"FB: {'trovato' if trovato else 'non trovato'} ({h})"}
    except Exception as e:
        time.sleep(1)
        return {"trovato": False, "note": f"FB err: {str(e)[:30]}"}

# ── Web scraping: Instagram ──────────────────────────────────────────────────
def verifica_instagram(handle):
    if not handle or str(handle).strip() in ("", "nan", "None"):
        return {"trovato": False, "note": "IG: nessun handle"}
    try:
        h = str(handle).strip().lstrip("@")
        resp = requests.get(f"https://www.instagram.com/{h}/",
            headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"},
            timeout=10, allow_redirects=True)
        text = resp.text.lower()
        trovato = (resp.status_code == 200 and len(resp.text) > 8000
                   and "sorry, this page" not in text
                   and "page not found" not in text)
        time.sleep(1)
        return {"trovato": trovato, "note": f"IG: {'trovato' if trovato else 'non trovato'} ({h})"}
    except Exception as e:
        time.sleep(1)
        return {"trovato": False, "note": f"IG err: {str(e)[:30]}"}

# ── Leggi Excel ──────────────────────────────────────────────────────────────
def leggi_excel(city):
    wb = openpyxl.load_workbook(os.path.join(DATA_DIR, f"Verifica_{city}_2027.xlsx"))
    ws = wb.active
    hdrs = [c.value for c in ws[1]]
    return [dict(zip(hdrs, row)) for row in ws.iter_rows(min_row=2, values_only=True) if row[0]]

# ── MAIN ─────────────────────────────────────────────────────────────────────
COLS = ["Nome Ristorante", "Indirizzo", "Telefono", "Sito", "Facebook", "Instagram",
        "Ultimo post Facebook", "Ultimo post Instagram", "Status",
        "Ultima revisione Google", "Ultima revisione The Fork", "Ultima revisione Tripadvisor",
        "Orari apertura", "Orari chiusura", "Social attivo?", "Menzioni notizie",
        "Note aggiuntive", "Data verifica"]

print(f"\n{'='*60}")
print(f"VERIFICA APERTURA RISTORANTI — LA PECORA NERA v5.0")
print(f"Data: {today_str}")
print(f"{'='*60}\n")

stats        = {}
serpapi_count = 0
all_rows     = {}

for city in ["Milano", "Roma", "Torino"]:
    print(f"\n── {city} ──────────────────────────────────────────────")
    rows    = leggi_excel(city)
    incerto = [r for r in rows if _str(r.get("Status")) == "INCERTO"]
    altri   = [r for r in rows if _str(r.get("Status")) != "INCERTO"]
    print(f"Totale: {len(rows)} | Mantenuti: {len(altri)} | INCERTO: {len(incerto)}")

    da_verif  = incerto[:MAX_PER_CITY]
    rimandati = incerto[MAX_PER_CITY:]
    print(f"Verifico ora: {len(da_verif)} | Rimandati: {len(rimandati)}")

    cs = {
        "aperto":  sum(1 for r in altri if _str(r.get("Status")) == "APERTO"),
        "chiuso":  sum(1 for r in altri if _str(r.get("Status")) == "CHIUSO"),
        "incerto": len(incerto),
        "verificati": 0, "promossi": 0,
        "the_fork": 0, "facebook": 0, "instagram": 0,
    }
    updated = list(altri)

    print()
    for r in da_verif:
        nome = _str(r.get("Nome Ristorante"))
        print(f"  [{serpapi_count+1:02d}] {nome[:38]:<38}", end="", flush=True)

        # Google Maps via SerpAPI
        gm = verifica_google_maps(nome, city)
        serpapi_count += 1
        cs["verificati"] += 1

        if gm["status"] != "INCERTO":
            r["Status"] = gm["status"]
            cs["promossi"] += 1
            cs["incerto"] -= 1
            if gm["status"] == "APERTO": cs["aperto"] += 1
            else:                        cs["chiuso"] += 1

        if gm["orari_ap"]: r["Orari apertura"] = gm["orari_ap"]
        if gm["orari_ch"]: r["Orari chiusura"] = gm["orari_ch"]
        r["Ultima revisione Google"] = gm["ultima_rev"] or _str(r.get("Ultima revisione Google"))

        # Social / The Fork
        tf = verifica_the_fork(nome, city)
        fb = verifica_facebook(_str(r.get("Facebook")))
        ig = verifica_instagram(_str(r.get("Instagram")))

        if tf["trovato"]:
            cs["the_fork"] += 1
            r["Ultima revisione The Fork"] = str(datetime.now().year)
        if fb["trovato"]: cs["facebook"]  += 1
        if ig["trovato"]: cs["instagram"] += 1

        soc = tf["trovato"] or fb["trovato"] or ig["trovato"]
        if soc: r["Social attivo?"] = "Si"

        # Se social attivo e ancora INCERTO → promuovi ad APERTO
        if soc and r["Status"] == "INCERTO":
            r["Status"]  = "APERTO"
            cs["aperto"] += 1
            cs["incerto"] -= 1
            cs["promossi"] += 1

        r["Note aggiuntive"] = (
            (gm["note"] + " | " if gm["note"] else "") +
            tf["note"] + " | " + fb["note"] + " | " + ig["note"]
        )[:200]
        r["Data verifica"] = today_str
        updated.append(r)
        print(f" → {r['Status']:8s} | TF:{tf['trovato']} FB:{fb['trovato']} IG:{ig['trovato']}")

    for r in rimandati:
        r["Note aggiuntive"] = "Non verificato in questa sessione"
        updated.append(r)

    all_rows[city] = updated
    fa = sum(1 for r in updated if _str(r.get("Status")) == "APERTO")
    fc = sum(1 for r in updated if _str(r.get("Status")) == "CHIUSO")
    fi = sum(1 for r in updated if _str(r.get("Status")) == "INCERTO")
    cs.update({"aperto": fa, "chiuso": fc, "incerto": fi})
    stats[city] = cs
    print(f"\n  ► {city}: APERTO={fa} CHIUSO={fc} INCERTO={fi} | "
          f"Verificati={cs['verificati']} Promossi={cs['promossi']}")

print(f"\n{'='*60}")
print(f"SerpAPI usate in questa sessione: {serpapi_count}")
print(f"{'='*60}")

# ── Salva Excel con formattazione ────────────────────────────────────────────
print("\nSalvataggio file Excel...")

for city in ["Milano", "Roma", "Torino"]:
    path = os.path.join(DATA_DIR, f"Verifica_{city}_2027.xlsx")
    df   = pd.DataFrame([{c: _str(r.get(c)) for c in COLS} for r in all_rows[city]], columns=COLS)
    df.to_excel(path, index=False)

    wb     = openpyxl.load_workbook(path)
    ws     = wb.active
    yellow = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    red    = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    si     = COLS.index("Status") + 1

    for ri, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row), 2):
        sv = ws.cell(row=ri, column=si).value
        if sv == "INCERTO":
            for c in row: c.fill = yellow
        elif sv == "CHIUSO":
            for c in row:
                c.fill = red
                c.font = Font(color="FFFFFF", bold=True)

    cw = {"Nome Ristorante": 30, "Indirizzo": 35, "Telefono": 14, "Sito": 28,
          "Facebook": 22, "Instagram": 22, "Ultimo post Facebook": 18,
          "Ultimo post Instagram": 18, "Status": 10, "Ultima revisione Google": 18,
          "Ultima revisione The Fork": 18, "Ultima revisione Tripadvisor": 18,
          "Orari apertura": 25, "Orari chiusura": 20, "Social attivo?": 12,
          "Menzioni notizie": 18, "Note aggiuntive": 45, "Data verifica": 14}
    for i, cn in enumerate(COLS, 1):
        ws.column_dimensions[ws.cell(1, i).column_letter].width = cw.get(cn, 14)
    wb.save(path)
    print(f"  ✓ Verifica_{city}_2027.xlsx ({len(all_rows[city])} righe)")

# ── Email via Brevo ───────────────────────────────────────────────────────────
print("\nInvio email via Brevo...")

ta  = sum(s["aperto"]    for s in stats.values())
tc  = sum(s["chiuso"]    for s in stats.values())
ti  = sum(s["incerto"]   for s in stats.values())
ttf = sum(s["the_fork"]  for s in stats.values())
tfb = sum(s["facebook"]  for s in stats.values())
tig = sum(s["instagram"] for s in stats.values())
tv  = sum(s["verificati"]for s in stats.values())
tp  = sum(s["promossi"]  for s in stats.values())

body = f"Verifica apertura ristoranti — {today_str}\n\nGOOGLE MAPS (SerpAPI):\n"
for city, s in stats.items():
    body += f"  {city}: {s['aperto']} APERTO, {s['chiuso']} CHIUSO, {s['incerto']} INCERTO\n"
body += (f"\nSOCIAL MEDIA:\n"
         f"  The Fork trovato: {ttf} ristoranti\n"
         f"  Facebook trovato: {tfb} ristoranti\n"
         f"  Instagram trovato: {tig} ristoranti\n"
         f"\nSTATISTICHE SESSIONE:\n"
         f"  Ricerche SerpAPI: {serpapi_count} / 250 disponibili/mese\n"
         f"  Verifiche web scraping: {tv} completate\n"
         f"  Ristoranti appena verificati: {tv}\n"
         f"  Promossi INCERTO→APERTO/CHIUSO: {tp}\n"
         f"  Ancora INCERTO: {ti}\n"
         f"\nTOTALE GUIDE: APERTO={ta}  CHIUSO={tc}  INCERTO={ti}\n\n"
         f"La Pecora Nera — Sistema automatico di verifica v5.0\n")

if not BREVO_API_KEY:
    print("  BREVO_API_KEY non impostata — email saltata.")
else:
    try:
        att = []
        for city in ["Milano", "Roma", "Torino"]:
            with open(os.path.join(DATA_DIR, f"Verifica_{city}_2027.xlsx"), "rb") as f:
                att.append({"name": f"Verifica_{city}_2027.xlsx",
                            "content": base64.b64encode(f.read()).decode()})
        r = requests.post("https://api.brevo.com/v3/smtp/email",
            headers={"api-key": BREVO_API_KEY, "content-type": "application/json"},
            json={"sender":  {"name": "La Pecora Nera — Sistema automatico", "email": SENDER_EMAIL},
                  "to":       RECIPIENTS,
                  "subject":  f"Verifica apertura locali guide del {today_str}",
                  "textContent": body,
                  "attachment":  att},
            timeout=60)
        if r.status_code in (200, 201):
            print(f"  ✓ Email inviata con successo! (HTTP {r.status_code})")
        else:
            print(f"  ✗ Errore email: HTTP {r.status_code} — {r.text[:200]}")
    except Exception as e:
        print(f"  ✗ Eccezione email: {e}")

# ── Git push ──────────────────────────────────────────────────────────────────
print("\nGit push...")
if not GITHUB_TOKEN or not GITHUB_REPO:
    print("  GITHUB_TOKEN / GITHUB_REPO non impostati — skip.")
else:
    try:
        subprocess.run(["git", "config", "user.email", SENDER_EMAIL],    cwd=SCRIPT_DIR, check=True)
        subprocess.run(["git", "config", "user.name",  "PNE Routine Bot"], cwd=SCRIPT_DIR, check=True)
        for city in ["Milano", "Roma", "Torino"]:
            subprocess.run(["git", "add", f"Verifica_{city}_2027.xlsx"], cwd=SCRIPT_DIR, check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=SCRIPT_DIR)
        if diff.returncode != 0:
            subprocess.run(["git", "commit", "-m", f"Verifica {today_str}"], cwd=SCRIPT_DIR, check=True)
            subprocess.run(["git", "remote", "set-url", "origin",
                            f"https://{GITHUB_TOKEN}@github.com/{GITHUB_REPO}.git"],
                           cwd=SCRIPT_DIR, check=True)
            subprocess.run(["git", "push"], cwd=SCRIPT_DIR, check=True)
            print("  ✓ File Verifica aggiornati nel repository.")
        else:
            print("  Nessuna modifica da committare.")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Errore git: {e}")

# ── Log ───────────────────────────────────────────────────────────────────────
log_path = os.path.join(DATA_DIR, f"Log_Verifica_{datetime.now().strftime('%d%m%Y')}.txt")
with open(log_path, "w", encoding="utf-8") as f:
    f.write(f"{'='*60}\nREPORT VERIFICA v5.0 — {today_str}\n{'='*60}\n\n")
    for city, s in stats.items():
        f.write(f"{city}: APERTO={s['aperto']} CHIUSO={s['chiuso']} INCERTO={s['incerto']}\n")
        f.write(f"  Verificati={s['verificati']} Promossi={s['promossi']} "
                f"TF={s['the_fork']} FB={s['facebook']} IG={s['instagram']}\n\n")
    f.write(f"SerpAPI usate: {serpapi_count}\n"
            f"Totale: APERTO={ta} CHIUSO={tc} INCERTO={ti}\n\n{'='*60}\n")
print(f"  Log salvato: {os.path.basename(log_path)}")

print(f"\n{'='*60}")
print("✓ COMPLETATO")
print(f"{'='*60}\n")
