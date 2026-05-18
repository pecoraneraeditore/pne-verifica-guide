"""Genera il JSON dei ristoranti per la mappa auto-aggiornante.

v2: geocoding a cascata con normalizzazione.

Legge un file guida (es. Roma_2027.xlsx), geocodifica gli indirizzi nuovi via
Nominatim (con cache su disco), e scrive un JSON che la pagina HTML legge.

Strategia di geocoding per ogni ristorante:
  1. Indirizzo originale + città + "Italia"
  2. Indirizzo normalizzato (tolti "Loc.", "c/o", "ang.", "Box", "snc",
     civici suffissati tipo 44/D-E ridotti a 44, apostrofi curvi → dritti)
  3. Solo la via estratta (es. "Galleria X, Via Y, 1" → "Via Y, 1")
  4. Via senza numero civico
  5. Solo città  → pin marcato come approssimato
  6. Se anche città fallisce: il ristorante finisce nella lista falliti

Uso:
    python mappa/genera_mappa.py data/Roma_2027.xlsx \
        --out docs/data/roma.json \
        --cache mappa/geocode_cache.json \
        --citta-default Roma --regione Lazio
"""
from __future__ import annotations
import argparse, json, re, sys, time, urllib.parse, urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import openpyxl

CATEGORIES = {
    'fatto':           {'label': 'Fatto',            'color': '#E53935'},
    'riservato':       {'label': 'Riservato per',    'color': '#757575'},
    'da_fare':         {'label': 'Da fare',          'color': '#FFFFFF'},
    'per_editori':     {'label': 'Per editori',      'color': '#F48FB1'},
    'da_finire':       {'label': 'Da finire',        'color': '#FBC02D'},
    'non_recensibile': {'label': 'Non recensibile',  'color': '#1565C0'},
    'altro_azzurro':   {'label': 'Altro',            'color': '#4FC3F7'},
    'plurimo':         {'label': 'Indirizzo plurimo','color': '#FB8C00'},
}

USER_AGENT = 'PNE-Mappa-Guida/2.0 (contatto: s.cargiani@lapecoranera.net)'


# --- Lettura xlsx -------------------------------------------------------------

def categorize(fill) -> str:
    fg = fill.fgColor
    if fg.type == 'rgb':
        rgb = (fg.rgb or '').upper()
        if rgb == 'FFFF0000':           return 'fatto'
        if rgb == 'FFFFFF00':           return 'da_finire'
        if rgb == 'FFFFC000':           return 'plurimo'
        if rgb in ('00000000', '', None): return 'da_fare'
    elif fg.type == 'theme':
        t, tint = fg.theme, fg.tint or 0
        if t == 0 and tint < -0.2:    return 'riservato'
        if t == 5 and tint > 0.5:     return 'per_editori'
        if t == 3 and tint > 0.4:     return 'non_recensibile'
        if t == 4 and tint > 0.2:     return 'altro_azzurro'
    elif fg.type == 'indexed' and fg.indexed == 10:
        return 'fatto'
    return 'da_fare'


def estrai_ristoranti(xlsx_path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb['Tavole e pause golose']
    out = []
    for r in range(8, ws.max_row + 1):
        nome = ws.cell(row=r, column=4).value
        if not nome or not str(nome).strip():
            continue
        out.append({
            'nome':      str(nome).strip(),
            'tipologia': str(ws.cell(row=r, column=5).value or '').strip(),
            'indirizzo': str(ws.cell(row=r, column=6).value or '').strip(),
            'zona':      str(ws.cell(row=r, column=7).value or '').strip(),
            'citta':     str(ws.cell(row=r, column=8).value or '').strip(),
            'recensore': str(ws.cell(row=r, column=14).value or '').strip(),
            'categoria': categorize(ws.cell(row=r, column=4).fill),
        })
    return out


# --- Normalizzazione indirizzo -----------------------------------------------

# Prefissi via riconosciuti per l'estrazione "estrai_via"
_PREFISSI_VIA = (r'Via|Viale|V\.le|Piazza|P\.zza|P\.za|Corso|C\.so|Largo|Lgo'
                 r'|Vicolo|Strada|S\.da|Salita|Lungomare|Lungotevere|Borgo'
                 r'|Contrada|Localit[àa]|Loc\.?')

# Marker che tagliano la parte narrativa (tutto ciò che segue viene scartato)
_CUT_MARKERS = [
    ' - Loc.', ' - Loc ', ' - loc.', ' - loc ',
    ' c/o ', ' C/O ',
    ' ang. ', ' Ang. ', ' angolo ',
    ' - Box ', ' Box ',
    ' Mercato ', ' mercato ',
    ' Galleria ', ' galleria ',
]


def normalizza(s: str) -> str:
    """Pulisce un indirizzo per migliorare il match Nominatim."""
    if not s:
        return ''
    s = s.replace('’', "'").replace('‘', "'")  # apostrofi curvi → dritti
    # Taglia tutto dopo il primo marker narrativo
    lower = s.lower()
    for cm in _CUT_MARKERS:
        idx = lower.find(cm.lower())
        if idx > 0:
            s = s[:idx]
            lower = s.lower()
    # "44/D-E" → "44", "732/E-F-G-H" → "732"
    s = re.sub(r'(\d+)\s*/\s*[A-Za-z][\w\-/]*', r'\1', s)
    # "732 F/G" → "732"
    s = re.sub(r'(\d+)\s+[A-Z](?:/[A-Z])+\b', r'\1', s)
    # ", snc" finale
    s = re.sub(r',?\s*snc\s*$', '', s, flags=re.IGNORECASE)
    return s.strip(' ,')


def estrai_via(s: str) -> str:
    """Se l'indirizzo ha un prefisso narrativo, estrae solo la via + civico."""
    if not s:
        return ''
    pattern = rf'\b({_PREFISSI_VIA})\s+[^,]+(?:,\s*\d+\S*)?'
    m = re.search(pattern, s, flags=re.IGNORECASE)
    return m.group(0) if m else s


def via_senza_civico(s: str) -> str:
    """Rimuove il numero civico finale."""
    return re.sub(r',\s*\d.*$', '', s).strip(' ,')


# --- Geocoding ----------------------------------------------------------------

def cache_key(r: dict) -> str:
    return f"{r['nome'].lower()}|{r['indirizzo'].lower()}|{r['citta'].lower()}"


def nominatim_query(q: str, timeout: int = 10) -> tuple[float, float] | None:
    url = ('https://nominatim.openstreetmap.org/search?'
           + urllib.parse.urlencode({
               'q': q, 'format': 'json', 'limit': '1', 'countrycodes': 'it',
           }))
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  ! Nominatim error per '{q[:60]}…': {e}", file=sys.stderr)
        return None
    if not data:
        return None
    try:
        return float(data[0]['lat']), float(data[0]['lon'])
    except (KeyError, ValueError, IndexError):
        return None


def costruisci_varianti(r: dict) -> list[tuple[str, bool]]:
    """Ritorna una lista di (query, approssimato) da provare in ordine."""
    ind = r['indirizzo']
    city = r['citta']
    varianti: list[tuple[str, bool]] = []

    def add(q: str, approx: bool):
        if q and (q, approx) not in varianti:
            varianti.append((q, approx))

    # v1: originale
    if ind and city:
        add(f"{ind}, {city}, Italia", False)
    elif ind:
        add(f"{ind}, Italia", False)

    # v2: normalizzato
    norm = normalizza(ind)
    if norm and city:
        add(f"{norm}, {city}, Italia", False)

    # v3: via estratta da prefissi narrativi
    via = estrai_via(norm or ind)
    if via and city and via != norm:
        add(f"{via}, {city}, Italia", False)

    # v4: via senza civico
    via_sc = via_senza_civico(via or norm or ind)
    if via_sc and city and via_sc != via:
        add(f"{via_sc}, {city}, Italia", False)

    # v5: solo città (approssimato!)
    if city:
        add(f"{city}, Italia", True)

    return varianti


def geocodifica_tutti(
    ristoranti: list[dict],
    cache_path: Path,
    no_geocode: bool = False,
    sleep_s: float = 1.1,
) -> tuple[int, int, list[dict]]:
    """Aggiunge lat/lon/approssimato ai ristoranti (in-place).
    Ritorna (n_da_cache, n_nuovi, falliti)."""
    cache: dict = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError:
            cache = {}

    n_cache, n_new = 0, 0
    falliti: list[dict] = []

    for r in ristoranti:
        key = cache_key(r)
        if key in cache and cache[key].get('lat') is not None:
            r['lat'] = cache[key]['lat']
            r['lon'] = cache[key]['lon']
            r['approssimato'] = bool(cache[key].get('approssimato', False))
            r['geocoded_at'] = cache[key].get('geocoded_at')
            n_cache += 1
            continue

        if no_geocode:
            r['lat'] = None
            r['lon'] = None
            falliti.append({'nome': r['nome'], 'motivo': 'no-geocode flag'})
            continue

        # Cascata di varianti
        found = None
        approx_flag = False
        used_q = None
        for q, approx in costruisci_varianti(r):
            coords = nominatim_query(q)
            time.sleep(sleep_s)
            if coords:
                found = coords
                approx_flag = approx
                used_q = q
                break

        if found is None:
            r['lat'] = None
            r['lon'] = None
            falliti.append({
                'nome': r['nome'],
                'indirizzo': r['indirizzo'],
                'citta': r['citta'],
                'motivo': 'no result dopo cascata (tentate '
                          f"{len(costruisci_varianti(r))} varianti)",
            })
            continue

        lat, lon = found
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        r['lat'] = lat
        r['lon'] = lon
        r['approssimato'] = approx_flag
        r['geocoded_at'] = today
        cache[key] = {
            'lat': lat, 'lon': lon,
            'approssimato': approx_flag, 'geocoded_at': today,
        }
        n_new += 1
        if approx_flag:
            print(f"  ~ {r['nome']}: approssimato a livello città ({r['citta']})")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    return n_cache, n_new, falliti


# --- Output JSON --------------------------------------------------------------

def scrivi_json(ristoranti, out_path, source_file, citta_default,
                regione, falliti):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    valid = [r for r in ristoranti if r.get('lat') is not None]
    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'source_file': source_file,
        'citta_default': citta_default,
        'regione': regione,
        'categories': CATEGORIES,
        'restaurants': valid,
        'geocoding_failed': falliti,
        'stats': {
            'totale_letti':    len(ristoranti),
            'con_coordinate':  len(valid),
            'approssimati':    sum(1 for r in valid if r.get('approssimato')),
            'precisi':         sum(1 for r in valid if not r.get('approssimato')),
            'falliti':         len(falliti),
            'con_recensore':   sum(1 for r in ristoranti if r['recensore']),
        },
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('xlsx', type=Path)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--cache', type=Path, required=True)
    p.add_argument('--no-geocode', action='store_true')
    p.add_argument('--sleep', type=float, default=1.1)
    p.add_argument('--citta-default', default='Roma')
    p.add_argument('--regione', default='Lazio',
                   help='Regione per overlay confine sulla mappa (default Lazio)')
    args = p.parse_args()

    if not args.xlsx.exists():
        print(f"File non trovato: {args.xlsx}", file=sys.stderr)
        return 1

    print(f"Lettura {args.xlsx.name}…")
    ristoranti = estrai_ristoranti(args.xlsx)
    cats = Counter(r['categoria'] for r in ristoranti)
    print(f"  letti {len(ristoranti)} ristoranti  categorie: {dict(cats)}")

    print(f"Geocoding (cache: {args.cache.name})…")
    n_cache, n_new, falliti = geocodifica_tutti(
        ristoranti, args.cache,
        no_geocode=args.no_geocode, sleep_s=args.sleep,
    )
    approx = sum(1 for r in ristoranti if r.get('approssimato'))
    print(f"  da cache: {n_cache}")
    print(f"  nuovi geocodificati: {n_new}")
    print(f"    di cui approssimati a livello città: {approx}")
    print(f"  falliti del tutto: {len(falliti)}")
    if falliti[:8]:
        print("  esempi falliti (necessitano fix manuale nel xlsx):")
        for f in falliti[:8]:
            print(f"    - {f['nome']}  [{f.get('citta')}]: {f['motivo']}")

    scrivi_json(ristoranti, args.out, args.xlsx.name,
                args.citta_default, args.regione, falliti)
    print(f"JSON scritto: {args.out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
