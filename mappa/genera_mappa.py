"""Genera il JSON dei ristoranti per la mappa auto-aggiornante.

Legge un file guida (es. Roma_2027.xlsx), geocodifica gli indirizzi nuovi via
Nominatim (con cache su disco per non ri-chiamare ogni settimana), e scrive
un JSON che la pagina HTML in docs/ legge a runtime.

Uso (in locale o in GitHub Actions):
    python mappa/genera_mappa.py data/Roma_2027.xlsx \
        --out docs/data/roma.json \
        --cache mappa/geocode_cache.json

Flag utili:
    --no-geocode   non chiama Nominatim, usa SOLO la cache (test locale veloce)
    --sleep 1.1    delay tra chiamate Nominatim (default 1.1s, rispetta TOS)
"""
from __future__ import annotations
import argparse, json, sys, time, urllib.parse, urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import openpyxl

# --- Categorie (dalla legenda in righe 3-4 del foglio guida) -----------------
# label = testo mostrato nel popup; color = hex usato per il pin Leaflet
CATEGORIES = {
    'fatto':           {'label': 'Fatto',            'color': '#E53935'},  # rosso
    'riservato':       {'label': 'Riservato per',    'color': '#757575'},  # grigio
    'da_fare':         {'label': 'Da fare',          'color': '#FFFFFF'},  # bianco
    'per_editori':     {'label': 'Per editori',      'color': '#F48FB1'},  # rosa
    'da_finire':       {'label': 'Da finire',        'color': '#FBC02D'},  # giallo
    'non_recensibile': {'label': 'Non recensibile',  'color': '#1565C0'},  # blu scuro
    'altro_azzurro':   {'label': 'Altro',            'color': '#4FC3F7'},  # azzurro
    'plurimo':         {'label': 'Indirizzo plurimo', 'color': '#FB8C00'}, # arancione
}

USER_AGENT = 'PNE-Mappa-Guida/1.0 (contatto: s.cargiani@lapecoranera.net)'


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
    # Header riga 7; dati da riga 8
    # col 4=Nome, 5=Tipologia, 6=Indirizzo, 7=Zona, 8=Città, 14=Recensore
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


# --- Geocoding ----------------------------------------------------------------

def cache_key(r: dict) -> str:
    """Chiave stabile: cambia solo se Nome/Indirizzo/Città cambiano."""
    return f"{r['nome'].lower()}|{r['indirizzo'].lower()}|{r['citta'].lower()}"


def nominatim_lookup(r: dict, timeout: int = 10) -> tuple[float, float] | None:
    """Una singola chiamata Nominatim. Ritorna (lat, lon) o None."""
    parts = [p for p in [r['indirizzo'], r['citta']] if p]
    if not parts:
        return None
    q = ', '.join(parts) + ', Italia'
    url = ('https://nominatim.openstreetmap.org/search?'
           + urllib.parse.urlencode({
               'q': q, 'format': 'json', 'limit': '1', 'countrycodes': 'it',
           }))
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  ! Nominatim error per '{r['nome']}': {e}", file=sys.stderr)
        return None
    if not data:
        return None
    try:
        return float(data[0]['lat']), float(data[0]['lon'])
    except (KeyError, ValueError, IndexError):
        return None


def geocodifica_tutti(
    ristoranti: list[dict],
    cache_path: Path,
    no_geocode: bool = False,
    sleep_s: float = 1.1,
) -> tuple[list[dict], list[dict]]:
    """Aggiunge lat/lon ai ristoranti (in-place sul dict).
    Ritorna (geocodificati_nuovi, falliti)."""
    cache: dict = {}
    if cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding='utf-8'))

    new_geocoded: list[dict] = []
    failed: list[dict] = []

    for r in ristoranti:
        key = cache_key(r)
        if key in cache and cache[key].get('lat') is not None:
            r['lat'] = cache[key]['lat']
            r['lon'] = cache[key]['lon']
            r['geocoded_at'] = cache[key].get('geocoded_at')
            continue

        if no_geocode:
            # In modalità test: lascia il record senza coordinate
            r['lat'] = None
            r['lon'] = None
            failed.append({'nome': r['nome'], 'motivo': 'no-geocode flag'})
            continue

        coords = nominatim_lookup(r)
        time.sleep(sleep_s)  # rispetta i TOS di Nominatim (max 1 req/s)
        if coords is None:
            r['lat'] = None
            r['lon'] = None
            failed.append({
                'nome': r['nome'],
                'indirizzo': r['indirizzo'],
                'citta': r['citta'],
                'motivo': 'no result da Nominatim',
            })
            # NON cacheiamo i fallimenti: la prossima settimana ritentiamo
            continue

        lat, lon = coords
        r['lat'] = lat
        r['lon'] = lon
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        r['geocoded_at'] = today
        cache[key] = {'lat': lat, 'lon': lon, 'geocoded_at': today}
        new_geocoded.append(r)

    # Salva la cache aggiornata
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding='utf-8',
    )

    return new_geocoded, failed


# --- Output JSON --------------------------------------------------------------

def scrivi_json(
    ristoranti: list[dict],
    out_path: Path,
    source_file: str,
    citta_default: str,
    failed: list[dict],
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'source_file': source_file,
        'citta_default': citta_default,
        'categories': CATEGORIES,
        'restaurants': [r for r in ristoranti if r.get('lat') is not None],
        'geocoding_failed': failed,
        'stats': {
            'totale_letti': len(ristoranti),
            'con_coordinate': sum(1 for r in ristoranti if r.get('lat') is not None),
            'falliti': len(failed),
            'con_recensore': sum(1 for r in ristoranti if r['recensore']),
        },
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('xlsx', type=Path, help='File guida, es. data/Roma_2027.xlsx')
    p.add_argument('--out', type=Path, required=True,
                   help='JSON di output, es. docs/data/roma.json')
    p.add_argument('--cache', type=Path, required=True,
                   help='File cache geocoding, es. mappa/geocode_cache.json')
    p.add_argument('--no-geocode', action='store_true',
                   help='Non chiamare Nominatim, usa solo cache (per test locale)')
    p.add_argument('--sleep', type=float, default=1.1,
                   help='Secondi di attesa tra chiamate Nominatim (default 1.1)')
    p.add_argument('--citta-default', default='Roma',
                   help='Città di default per centratura mappa (default Roma)')
    args = p.parse_args()

    if not args.xlsx.exists():
        print(f"File non trovato: {args.xlsx}", file=sys.stderr)
        return 1

    print(f"Lettura {args.xlsx.name}…")
    ristoranti = estrai_ristoranti(args.xlsx)
    cats = Counter(r['categoria'] for r in ristoranti)
    print(f"  letti {len(ristoranti)} ristoranti")
    print(f"  categorie: {dict(cats)}")

    print(f"Geocoding (cache: {args.cache.name})…")
    new_geo, failed = geocodifica_tutti(
        ristoranti, args.cache,
        no_geocode=args.no_geocode, sleep_s=args.sleep,
    )
    print(f"  da cache: {len(ristoranti) - len(new_geo) - len(failed)}")
    print(f"  nuovi geocodificati: {len(new_geo)}")
    print(f"  falliti: {len(failed)}")
    if failed[:5]:
        print("  esempi falliti:")
        for f in failed[:5]:
            print(f"    - {f['nome']}: {f.get('motivo')}")

    scrivi_json(ristoranti, args.out, args.xlsx.name, args.citta_default, failed)
    print(f"JSON scritto: {args.out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
