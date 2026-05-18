"""Genera un CSV per import su Google My Maps da un file guida Pecora Nera.

Estrae, dal foglio "Tavole e pause golose":
  - Nome, Indirizzo, Città  → indirizzo completo per il geocoding lato My Maps
  - Recensore               → mostrato nel popup
  - Colore della riga       → categoria (per colorare i pin)

Uso:
    python genera_mappa_csv.py Roma_2027.xlsx
    python genera_mappa_csv.py Roma_2027.xlsx --out Roma_2027_mappa.csv
"""
from __future__ import annotations
import argparse, csv, sys
from collections import Counter
from pathlib import Path
import openpyxl

# Categorie dedotte dalla legenda del file guida (righe 3-4)
CAT_LABEL = {
    'fatto':           'Fatto',
    'riservato':       'Riservato per',
    'da_fare':         'Da fare',
    'per_editori':     'Per editori',
    'da_finire':       'Da finire',
    'non_recensibile': 'Non recensibile',
    'altro_azzurro':   'Altro (azzurro)',
    'plurimo':         'Indirizzo plurimo',
}


def categorize(fill) -> str:
    """Mappa il colore di sfondo della cella alla categoria del file."""
    fg = fill.fgColor
    if fg.type == 'rgb':
        rgb = (fg.rgb or '').upper()
        if rgb == 'FFFF0000':
            return 'fatto'
        if rgb == 'FFFFFF00':
            return 'da_finire'
        if rgb == 'FFFFC000':
            return 'plurimo'
        if rgb in ('00000000', '', None):
            return 'da_fare'
    elif fg.type == 'theme':
        t, tint = fg.theme, fg.tint or 0
        if t == 0 and tint < -0.2:
            return 'riservato'
        if t == 5 and tint > 0.5:
            return 'per_editori'
        if t == 3 and tint > 0.4:
            return 'non_recensibile'
        if t == 4 and tint > 0.2:
            return 'altro_azzurro'
    elif fg.type == 'indexed' and fg.indexed == 10:
        return 'fatto'
    return 'da_fare'


def estrai_ristoranti(xlsx_path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb['Tavole e pause golose']
    out = []
    # Header in riga 7, dati da riga 8
    # Col 4=Nome, 5=Tipologia, 6=Indirizzo, 7=Zona, 8=Città, 14=Recensore
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


def scrivi_csv(ristoranti: list[dict], out_path: Path) -> None:
    with out_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['Nome', 'Indirizzo completo', 'Categoria', 'Recensore',
                    'Tipologia', 'Zona', 'Città', 'Descrizione'])
        for r in ristoranti:
            addr_parts = [p for p in [r['indirizzo'], r['citta']] if p]
            indirizzo = ', '.join(addr_parts) + (', Italia' if addr_parts else '')
            desc = []
            if r['recensore']:
                desc.append(f"Recensore: {r['recensore']}")
            if r['tipologia']:
                desc.append(f"Tipologia: {r['tipologia']}")
            if r['zona']:
                desc.append(f"Zona: {r['zona']}")
            desc.append(f"Stato: {CAT_LABEL[r['categoria']]}")
            w.writerow([
                r['nome'], indirizzo, CAT_LABEL[r['categoria']],
                r['recensore'], r['tipologia'], r['zona'], r['citta'],
                ' • '.join(desc),
            ])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('xlsx', type=Path, help='File guida, es. Roma_2027.xlsx')
    p.add_argument('--out', type=Path, default=None,
                   help='CSV di output (default: <nome>_mappa.csv)')
    args = p.parse_args()

    if not args.xlsx.exists():
        print(f"File non trovato: {args.xlsx}", file=sys.stderr)
        return 1

    out_path = args.out or args.xlsx.with_name(args.xlsx.stem + '_mappa.csv')
    ristoranti = estrai_ristoranti(args.xlsx)
    scrivi_csv(ristoranti, out_path)

    cats = Counter(r['categoria'] for r in ristoranti)
    citta = Counter(r['citta'] for r in ristoranti)
    print(f"Letti {len(ristoranti)} ristoranti da {args.xlsx.name}")
    print(f"  con recensore compilato: {sum(1 for r in ristoranti if r['recensore'])}")
    print(f"  città distinte: {len(citta)}")
    print(f"  categorie: {dict(cats)}")
    print(f"CSV scritto: {out_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
