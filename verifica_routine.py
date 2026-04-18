#!/usr/bin/env python3
"""
verifica_routine.py  –  Verifica apertura ristoranti guide La Pecora Nera
Progettato per girare come Claude Routine (cloud) oppure localmente.

Variabili d'ambiente richieste:
  BREVO_API_KEY  – chiave API Brevo
  GITHUB_TOKEN   – Personal Access Token GitHub (scope: repo)
  GITHUB_REPO    – es. "nome_utente/pne-verifica-guide"

Struttura attesa nella cartella dello script (stessa cartella, no sottocartelle):
  Milano_2027.xlsx, Roma_2027.xlsx, Torino_2027.xlsx
  Milano_dati_2026_per_2027.xlsx, Roma_dati_2026_per_2027.xlsx, Torino_dati_2026_per_2027.xlsx
  Verifica_Milano_2027.xlsx, Verifica_Roma_2027.xlsx, Verifica_Torino_2027.xlsx
"""

import os
import sys
import re
import base64
import subprocess
from datetime import datetime, timedelta

# ── dipendenze: auto-install se mancano ────────────────────────────────────
def _ensure(pkg, import_name=None):
    try:
        __import__(import_name or pkg)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", pkg, "-q"], check=True)

for _p in [("pandas", None), ("openpyxl", None), ("requests", None), ("playwright", None)]:
    _ensure(_p[0], _p[1])

import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill, Font
import requests

# ── Playwright ──────────────────────────────────────────────────────────────
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    print("  Playwright non disponibile — social media non verificati.")

# ── configurazione ──────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = SCRIPT_DIR

BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "")

SENDER_EMAIL  = "s.cargiani@lapecoranera.net"
RECIPIENTS    = [
    {"email": "s.cargiani@lapecoranera.net"},
    {"email": "f.darienzo@lapecoranera.net"},
]

today      = datetime.now()
today_str  = today.strftime("%d/%m/%Y")
cutoff_30d = today - timedelta(days=30)

# ── ristoranti confermati manualmente ──────────────────────────────────────
CONFIRMED_APERTO = {
    "Ristorante D O":  {"note": "Michelin 2026 confermato (Cornaredo)",
                        "orari_ap": "12:00", "orari_ch": "14:30, 20:00-22:30"},
    "Erba Brusca":     {"note": "Confermato aperto feb 2026 (Yelp/sito)",
                        "orari_ap": "12:00 (gio-dom), 20:00 (mer-dom)", "orari_ch": "14:00, 22:30"},
    "Remulass":        {"note": "Guida Michelin 2026 confermata",
                        "orari_ap": "", "orari_ch": ""},
    "Frangente":       {"note": "Listato tra migliori Milano 2026 (Gambero Rosso)",
                        "orari_ap": "", "orari_ch": ""},
    "Cucina Franca":   {"note": "Presente su TheFork 2026",
                        "orari_ap": "", "orari_ch": ""},
    "Bistrot 64":      {"note": "Aggiornato marzo 2026 (Yelp): lun 19:30-23:30, mar-sab 12-15:30 e 19:30-23:30",
                        "orari_ap": "12:00 (mar-sab), 19:30", "orari_ch": "15:30, 23:30"},
    "Glass Hostaria":  {"note": "Michelin Guide Roma 2026 confermato",
                        "orari_ap": "", "orari_ch": ""},
    "Connubio":        {"note": "Confermato aperto (recensioni nov 2025, sito attivo)",
                        "orari_ap": "19:30", "orari_ch": "23:00"},
}

# ══════════════════════════════════════════════════════════════════════════
# SCRAPING SOCIAL MEDIA
# ══════════════════════════════════════════════════════════════════════════

def _parse_timestamp(ts_int):
    try:
        return datetime.fromtimestamp(int(ts_int)).strftime("%d/%m/%Y")
    except:
        return None

def _parse_iso(dt_str):
    try:
        d = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return d.strftime("%d/%m/%Y")
    except:
        return None

def scrape_facebook(handle, page):
    """Visita la pagina Facebook mobile e restituisce la data dell'ultimo post."""
    if not handle or not handle.strip():
        return ""
    h = handle.strip().lstrip("@")
    try:
        page.goto(f"https://m.facebook.com/{h}", timeout=12000, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)

        # Accetta cookie se compare il banner
        for txt in ["Accetta tutto", "Accetta tutti i cookie", "Accetta", "OK", "Allow all cookies"]:
            try:
                page.click(f'button:has-text("{txt}")', timeout=1500)
                page.wait_for_timeout(800)
                break
            except:
                pass

        # 1. abbr[data-utime]
        timestamps = page.evaluate("""
            () => Array.from(document.querySelectorAll('abbr[data-utime]'))
                       .map(e => parseInt(e.getAttribute('data-utime')))
                       .filter(t => !isNaN(t))
        """)
        if timestamps:
            result = _parse_timestamp(max(timestamps))
            if result:
                return result

        # 2. time[datetime]
        datetimes = page.evaluate("""
            () => Array.from(document.querySelectorAll('time[datetime]'))
                       .map(e => e.getAttribute('datetime'))
        """)
        dates = [_parse_iso(d) for d in datetimes if d]
        dates = [d for d in dates if d]
        if dates:
            return sorted(dates)[-1]

        # 3. cerca timestamp numerici nel testo della pagina
        matches = re.findall(r'"publish_time":(\d{10})', page.content())
        if matches:
            result = _parse_timestamp(max(int(m) for m in matches))
            if result:
                return result

        return "N/D"

    except PlaywrightTimeout:
        return "Timeout"
    except Exception as e:
        return f"Err: {str(e)[:25]}"


def scrape_instagram(handle, page):
    """Visita il profilo Instagram e restituisce la data dell'ultimo post."""
    if not handle or not handle.strip():
        return ""
    h = handle.strip().lstrip("@")
    try:
        page.goto(f"https://www.instagram.com/{h}/", timeout=12000, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)

        content = page.content()

        # 1. taken_at_timestamp nel JSON embedded
        matches = re.findall(r'"taken_at_timestamp"\s*:\s*(\d+)', content)
        if matches:
            result = _parse_timestamp(max(int(m) for m in matches))
            if result:
                return result

        # 2. edge_owner_to_timeline_media timestamp
        matches2 = re.findall(r'"timestamp"\s*:\s*(\d{10})', content)
        if matches2:
            result = _parse_timestamp(max(int(m) for m in matches2))
            if result:
                return result

        # 3. time[datetime]
        datetimes = page.evaluate("""
            () => Array.from(document.querySelectorAll('time[datetime]'))
                       .map(e => e.getAttribute('datetime'))
        """)
        dates = [_parse_iso(d) for d in datetimes if d]
        dates = [d for d in dates if d]
        if dates:
            return sorted(dates)[-1]

        return "N/D"

    except PlaywrightTimeout:
        return "Timeout"
    except Exception as e:
        return f"Err: {str(e)[:25]}"


# ══════════════════════════════════════════════════════════════════════════
# 1. ELABORAZIONE PER CITTÀ
# ══════════════════════════════════════════════════════════════════════════

def _str(v, default=""):
    if v is None:
        return default
    s = str(v).strip()
    return default if s in ("nan", "None", "") else s

stats = {}

# Avvia browser Playwright una volta sola per tutte le città
_playwright = None
_browser    = None
_bctx       = None

if PLAYWRIGHT_AVAILABLE:
    print("\nAvvio browser per social media scraping...")
    try:
        _playwright = sync_playwright().start()
        _browser    = _playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        _bctx = _browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="it-IT",
            viewport={"width": 1280, "height": 800},
        )
        print("Browser avviato.")
    except Exception as e:
        print(f"Errore avvio browser: {e}")
        _playwright = _browser = _bctx = None

for city in ["Milano", "Roma", "Torino"]:
    print(f"\n=== {city} ===")

    # -- leggi guida --
    guide_file = os.path.join(DATA_DIR, f"{city}_2027.xlsx")
    df_guide   = pd.read_excel(guide_file, header=6)
    col_pl     = df_guide.columns[0]
    df_guide   = df_guide[df_guide[col_pl] == 0].copy()
    df_guide   = df_guide[df_guide["Nome"].notna()]
    df_guide   = df_guide[df_guide["Nome"].astype(str).str.strip().str.len() > 0]
    actual_cols = ["Nome"]
    if "Indirizzo" in df_guide.columns: actual_cols.append("Indirizzo")
    if "Telefono"  in df_guide.columns: actual_cols.append("Telefono")
    df_guide   = df_guide[actual_cols].copy()

    # -- leggi dati --
    dati_file = os.path.join(DATA_DIR, f"{city}_dati_2026_per_2027.xlsx")
    try:
        df_dati      = pd.read_excel(dati_file)
        nome_col     = next((c for c in df_dati.columns if c.lower() in ["nome","ristorante"]), None)
        internet_col = next((c for c in df_dati.columns if "internet" in c.lower() or "sito" in c.lower()), None)
        fb_col       = next((c for c in df_dati.columns if "facebook" in c.lower()), None)
        ig_col       = next((c for c in df_dati.columns if c.lower() == "instagram"), None)
        pick         = [c for c in [nome_col, internet_col, fb_col, ig_col] if c]
        if nome_col and internet_col and fb_col and ig_col:
            df_dati_clean = df_dati[pick].copy()
            df_dati_clean.columns = ["Nome","Sito","Facebook","Instagram"]
            df_dati_clean = df_dati_clean.drop_duplicates("Nome")
        else:
            df_dati_clean = pd.DataFrame(columns=["Nome","Sito","Facebook","Instagram"])
    except Exception as e:
        print(f"  Dati errore: {e}")
        df_dati_clean = pd.DataFrame(columns=["Nome","Sito","Facebook","Instagram"])

    # -- leggi verifica precedente (persistenza 30 giorni) --
    verif_file = os.path.join(DATA_DIR, f"Verifica_{city}_2027.xlsx")
    if os.path.exists(verif_file):
        df_verif = pd.read_excel(verif_file)
        def _parse_date(d):
            try: return datetime.strptime(str(d).strip(), "%d/%m/%Y")
            except: return None
        df_verif["_date"] = df_verif["Data verifica"].apply(_parse_date)
        keep_mask   = df_verif["Status"].isin(["APERTO","CHIUSO"]) & (df_verif["_date"] >= cutoff_30d)
        df_keep     = df_verif[keep_mask].copy()
        df_reverify = df_verif[~keep_mask].copy()
    else:
        df_keep     = pd.DataFrame()
        df_reverify = pd.DataFrame()

    print(f"  Guide: {len(df_guide)}, Mantieni: {len(df_keep)}, Ri-verifica: {len(df_reverify)}")

    df_merged = df_guide.merge(df_dati_clean, on="Nome", how="left")

    rows         = []
    new_verified = 0
    social_checked = 0

    # Pagine Playwright riutilizzabili
    _fb_page = _bctx.new_page() if _bctx else None
    _ig_page = _bctx.new_page() if _bctx else None

    for _, row in df_merged.iterrows():
        nome      = _str(row["Nome"])
        indirizzo = _str(row.get("Indirizzo"))
        telefono  = _str(row.get("Telefono"))
        sito      = _str(row.get("Sito"))
        facebook  = _str(row.get("Facebook"))
        instagram = _str(row.get("Instagram"))

        kept = df_keep[df_keep["Nome Ristorante"] == nome] if len(df_keep) else pd.DataFrame()

        if len(kept) > 0:
            k          = kept.iloc[0]
            status     = k["Status"]
            data_v     = k["Data verifica"]
            ul_google  = _str(k.get("Ultima revisione Google"))
            ul_fork    = _str(k.get("Ultima revisione The Fork"))
            ul_trip    = _str(k.get("Ultima revisione Tripadvisor"))
            orari_ap   = _str(k.get("Orari apertura"))
            orari_ch   = _str(k.get("Orari chiusura"))
            social_att = _str(k.get("Social attivo?"))
            menzioni   = _str(k.get("Menzioni notizie"))
            note       = _str(k.get("Note aggiuntive"))
            fb_post    = _str(k.get("Ultimo post Facebook"))
            ig_post    = _str(k.get("Ultimo post Instagram"))
            if not sito:      sito      = _str(k.get("Sito"))
            if not facebook:  facebook  = _str(k.get("Facebook"))
            if not instagram: instagram = _str(k.get("Instagram"))

        elif nome in CONFIRMED_APERTO:
            conf       = CONFIRMED_APERTO[nome]
            status     = "APERTO"
            data_v     = today_str
            ul_google  = ul_fork = ul_trip = ""
            orari_ap   = conf.get("orari_ap", "")
            orari_ch   = conf.get("orari_ch", "")
            social_att = "Si"
            menzioni   = ""
            note       = conf.get("note", "")
            fb_post    = ""
            ig_post    = ""
            new_verified += 1

        else:
            prev = df_reverify[df_reverify["Nome Ristorante"] == nome] if len(df_reverify) else pd.DataFrame()
            if len(prev) > 0:
                p          = prev.iloc[0]
                status     = "INCERTO"
                data_v     = today_str
                ul_google  = _str(p.get("Ultima revisione Google"))
                ul_fork    = _str(p.get("Ultima revisione The Fork"))
                ul_trip    = _str(p.get("Ultima revisione Tripadvisor"))
                orari_ap   = _str(p.get("Orari apertura"))
                orari_ch   = _str(p.get("Orari chiusura"))
                social_att = _str(p.get("Social attivo?"))
                menzioni   = _str(p.get("Menzioni notizie"))
                note       = "Non verificato in questa sessione"
                fb_post    = _str(p.get("Ultimo post Facebook"))
                ig_post    = _str(p.get("Ultimo post Instagram"))
                if not facebook:  facebook  = _str(p.get("Facebook"))
                if not instagram: instagram = _str(p.get("Instagram"))
            else:
                status     = "INCERTO"
                data_v     = today_str
                ul_google  = ul_fork = ul_trip = ""
                orari_ap   = orari_ch = social_att = menzioni = ""
                note       = "Nuovo ristorante - da verificare"
                fb_post    = ""
                ig_post    = ""

        # ── Social media scraping (solo se handle presente e data non già nota) ──
        if _bctx and (facebook or instagram):
            need_fb = facebook and not fb_post
            need_ig = instagram and not ig_post
            if need_fb or need_ig:
                social_checked += 1
                if need_fb and _fb_page:
                    fb_post = scrape_facebook(facebook, _fb_page)
                if need_ig and _ig_page:
                    ig_post = scrape_instagram(instagram, _ig_page)

        rows.append({
            "Nome Ristorante":              nome,
            "Indirizzo":                    indirizzo,
            "Telefono":                     telefono,
            "Sito":                         sito,
            "Facebook":                     facebook,
            "Instagram":                    instagram,
            "Ultimo post Facebook":         fb_post,
            "Ultimo post Instagram":        ig_post,
            "Status":                       status,
            "Ultima revisione Google":      ul_google,
            "Ultima revisione The Fork":    ul_fork,
            "Ultima revisione Tripadvisor": ul_trip,
            "Orari apertura":               orari_ap,
            "Orari chiusura":               orari_ch,
            "Social attivo?":               social_att,
            "Menzioni notizie":             menzioni,
            "Note aggiuntive":              note,
            "Data verifica":                data_v,
        })

    # Chiudi pagine Playwright della città
    if _fb_page:
        try: _fb_page.close()
        except: pass
    if _ig_page:
        try: _ig_page.close()
        except: pass

    df_out    = pd.DataFrame(rows)
    n_aperto  = len(df_out[df_out["Status"] == "APERTO"])
    n_chiuso  = len(df_out[df_out["Status"] == "CHIUSO"])
    n_incerto = len(df_out[df_out["Status"] == "INCERTO"])

    stats[city] = {
        "totale": len(df_out), "aperto": n_aperto,
        "chiuso": n_chiuso, "incerto": n_incerto,
        "mantenuti": len(df_keep), "verificati_oggi": new_verified,
        "social_checked": social_checked,
    }
    print(f"  Output: {len(df_out)} | APERTO={n_aperto} CHIUSO={n_chiuso} INCERTO={n_incerto} | Social verificati={social_checked}")

    # -- salva Excel con colori --
    df_out.to_excel(verif_file, index=False)
    wb = openpyxl.load_workbook(verif_file)
    ws = wb.active
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    red_fill    = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    status_col  = next((c.column for c in ws[1] if c.value == "Status"), None)
    if status_col:
        for r in ws.iter_rows(min_row=2, max_row=ws.max_row):
            sv = r[status_col - 1].value
            if sv == "INCERTO":
                for c in r: c.fill = yellow_fill
            elif sv == "CHIUSO":
                for c in r:
                    c.fill = red_fill
                    c.font = Font(color="FFFFFF")
    for col in ws.columns:
        max_len = max((len(str(c.value)) for c in col if c.value), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)
    wb.save(verif_file)
    print(f"  Salvato: {verif_file}")

# Chiudi browser
if _browser:
    try: _browser.close()
    except: pass
if _playwright:
    try: _playwright.stop()
    except: pass

# ══════════════════════════════════════════════════════════════════════════
# 2. RIEPILOGO
# ══════════════════════════════════════════════════════════════════════════

print("\n=== RIEPILOGO ===")
for city, s in stats.items():
    print(f"{city}: {s['aperto']} APERTO, {s['chiuso']} CHIUSO, {s['incerto']} INCERTO (tot {s['totale']})")
    print(f"  Mantenuti: {s['mantenuti']}, Confermati oggi: {s['verificati_oggi']}, Social verificati: {s['social_checked']}")

# ══════════════════════════════════════════════════════════════════════════
# 3. INVIO EMAIL VIA BREVO
# ══════════════════════════════════════════════════════════════════════════

print("\n=== INVIO EMAIL ===")

if not BREVO_API_KEY:
    print("BREVO_API_KEY non impostata — email non inviata.")
else:
    attachments = []
    for city in ["Milano", "Roma", "Torino"]:
        path = os.path.join(DATA_DIR, f"Verifica_{city}_2027.xlsx")
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        attachments.append({"name": f"Verifica_{city}_2027.xlsx", "content": b64})

    body = (
        f"Verifica apertura ristoranti del {today_str}\n\n"
        + "\n".join(
            f"{city}: {s['aperto']} APERTO, {s['chiuso']} CHIUSO, {s['incerto']} INCERTO"
            for city, s in stats.items()
        )
        + f"\n\nMantenuti dalla verifica precedente: {sum(s['mantenuti'] for s in stats.values())}"
        + f"\nConfermati APERTO oggi: {sum(s['verificati_oggi'] for s in stats.values())}"
        + f"\nSocial media verificati: {sum(s['social_checked'] for s in stats.values())}"
        + "\n\nIn allegato i file Excel con il dettaglio per ciascuna città."
    )

    payload = {
        "sender":      {"name": "La Pecora Nera - Sistema automatico", "email": SENDER_EMAIL},
        "to":          RECIPIENTS,
        "subject":     f"Verifica apertura locali guide del {today_str}",
        "textContent": body,
        "attachment":  attachments,
    }

    try:
        resp = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": BREVO_API_KEY, "content-type": "application/json"},
            json=payload,
            timeout=60,
        )
        if resp.status_code in (200, 201):
            print(f"Email inviata! Status: {resp.status_code}")
        else:
            print(f"Errore Brevo: {resp.status_code} – {resp.text[:300]}")
    except Exception as e:
        print(f"Eccezione invio email: {e}")

# ══════════════════════════════════════════════════════════════════════════
# 4. GIT PUSH dei file Verifica aggiornati
# ══════════════════════════════════════════════════════════════════════════

print("\n=== GIT PUSH ===")

if not GITHUB_TOKEN or not GITHUB_REPO:
    print("GITHUB_TOKEN / GITHUB_REPO non impostati — salto il push.")
else:
    try:
        subprocess.run(["git", "config", "user.email", SENDER_EMAIL],     cwd=SCRIPT_DIR, check=True)
        subprocess.run(["git", "config", "user.name",  "PNE Routine Bot"], cwd=SCRIPT_DIR, check=True)
        for city in ["Milano", "Roma", "Torino"]:
            subprocess.run(["git", "add", f"Verifica_{city}_2027.xlsx"], cwd=SCRIPT_DIR, check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=SCRIPT_DIR)
        if diff.returncode != 0:
            subprocess.run(["git", "commit", "-m", f"Verifica {today_str}"], cwd=SCRIPT_DIR, check=True)
            remote_url = f"https://{GITHUB_TOKEN}@github.com/{GITHUB_REPO}.git"
            subprocess.run(["git", "remote", "set-url", "origin", remote_url], cwd=SCRIPT_DIR, check=True)
            subprocess.run(["git", "push"], cwd=SCRIPT_DIR, check=True)
            print("File Verifica aggiornati nel repository GitHub.")
        else:
            print("Nessuna modifica da committare.")
    except subprocess.CalledProcessError as e:
        print(f"Errore git: {e}")

print("\n=== COMPLETATO ===")
