"""Gemeinsame Einkauf-Review-Helfer fuer Modul 1 (Order Entry) und Modul 2 (Mail Scraper).

Stellt kleine stateless Funktionen bereit, die den Einkauf-Review-Pfad
zwischen beiden Modulen vereinheitlichen:
- Widget-Population (Payload → EinkaufHeadFormWidget + EinkaufItemsTableWidget)
- Payload-Collection (Widgets → Payload)
- Post-Save-Aktionen (Bildentscheidungen + Pending Matches)
- Review-Bundle-Verteilung + -Bereinigung
- Warenwert-Berechnung und Delta-Pruefung
- Summen-Banner-Refresh
- Validierung, Warnungen und Pflichtfeld-Markierung
- Save-Readiness-Pruefung und Checkliste
- Order-Number-Callback-Factory
- Widget-Reset
"""

import logging

from module.einkauf_pipeline import EinkaufPipeline

log = logging.getLogger(__name__)


# ── Widget-Population ────────────────────────────────────────────

def populate_einkauf_widgets(form_widget, items_widget, payload, ean_callback=None):
  """Befuellt EinkaufHeadFormWidget + EinkaufItemsTableWidget aus einem Payload.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
    payload: dict mit Kopfdaten + 'waren'-Liste.
    ean_callback: Optionaler Callback fuer lokale EAN-Aufloesung.
  """
  safe_payload = payload if isinstance(payload, dict) else {}
  form_widget.set_payload(safe_payload)
  items_widget.set_items(
    safe_payload.get("waren", []),
    ean_fill_callback=ean_callback,
    payload=safe_payload,
  )


# ── Payload-Collection ───────────────────────────────────────────

def collect_einkauf_payload(form_widget, items_widget, base_payload=None):
  """Sammelt den Einkauf-Payload aus EinkaufHeadFormWidget + EinkaufItemsTableWidget.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
    base_payload: Optionaler Basis-Payload (wird nicht mutiert).

  Returns:
    dict: Payload mit Kopfdaten + 'waren'-Liste.
  """
  base = dict(base_payload) if isinstance(base_payload, dict) else {}
  result = form_widget.apply_to_payload(base)
  result["waren"] = items_widget.get_items()
  return result


# ── Post-Save-Aktionen ──────────────────────────────────────────

def apply_einkauf_post_save(parent_widget, settings_manager, items_widget, save_result, db=None):
  """Wendet Bildentscheidungen und Pending-Matches nach erfolgreichem Einkauf-Save an.

  Args:
    parent_widget: Eltern-Widget fuer Dialoge.
    settings_manager: SettingsManager-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
    save_result: Ergebnis von EinkaufPipeline.confirm_and_save_single().
    db: Optionale bestehende DB-Verbindung.

  Returns:
    dict mit:
      'db': Aktualisierte DB-Verbindung.
      'image_result': Ergebnis der Bildentscheidungen.
      'match_result': Ergebnis der Pending-Matches.
  """
  einkauf_id = save_result.get("einkauf_id")
  save_db = save_result.get("db", db)
  image_result = {}
  match_result = {}

  if einkauf_id and save_db:
    image_result = items_widget.apply_saved_image_decisions(save_db, einkauf_id) or {}
    if image_result.get("reason") == "error":
      log.warning(
        "Bildentscheidungen konnten nicht angewendet werden: %s",
        image_result.get("message", ""),
      )

  match_result = EinkaufPipeline.confirm_and_apply_pending_matches(
    parent_widget,
    settings_manager,
    db=save_db,
  )
  save_db = match_result.get("db", save_db)

  return {
    "db": save_db,
    "image_result": image_result,
    "match_result": match_result,
  }


# ── Review-Bundle-Verteilung ─────────────────────────────────────

def set_einkauf_review_data(form_widget, items_widget, bundle):
  """Verteilt ein Review-Bundle an EinkaufHeadFormWidget + EinkaufItemsTableWidget.

  Beide Module rufen nach EinkaufPipeline.build_order_review_bundle() exakt
  dieselbe Sequenz auf – diese Funktion vereinheitlicht diesen Pfad.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
    bundle: Review-Bundle von EinkaufPipeline.build_order_review_bundle().
  """
  form_widget.set_review_data(bundle)
  items_widget.set_review_data(bundle)


def clear_einkauf_review_data(form_widget, items_widget):
  """Entfernt Review-Hervorhebungen von beiden Widgets.

  Symmetrisches Gegenstueck zu set_einkauf_review_data().
  Wird aufgerufen wenn kein Review-Bundle verfuegbar ist
  (z.B. keine Bestellnummer oder Fehler beim Bundle-Aufbau).

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
  """
  form_widget.clear_review_data()
  items_widget.clear_review_data()


# ── Warenwert-Berechnung und Delta-Pruefung ──────────────────────────────────

def compute_warenwert(items) -> float:
  """Berechnet den Warenwert (Summe Menge * EKP-Brutto) aus einer Artikelliste.

  Identischer Algorithmus wie SummenBannerWidget.update_from_items(),
  aber als freie Funktion ohne Widget-Abhaengigkeit (Circular-Dep-frei).

  Args:
    items: Liste von Artikel-Dicts mit 'menge' und 'ekp_brutto'.

  Returns:
    float: Berechneter Warenwert in EUR.
  """
  warenwert = 0.0
  for item in (items or []):
    try:
      m = float(str(item.get("menge", 1) or 1).replace(",", "."))
      p = float(str(item.get("ekp_brutto", 0) or 0).replace(",", "."))
      warenwert += m * p
    except (ValueError, TypeError):
      pass
  return warenwert


def check_warenwert_delta(items, gesamt_ekp_brutto) -> str | None:
  """Prueft ob die Summe der Einzelpreise vom KI-Gesamtpreis abweicht.

  Args:
    items: Liste von Artikel-Dicts mit 'menge' und 'ekp_brutto'.
    gesamt_ekp_brutto: KI-ermittelter Gesamtpreis (str oder float).

  Returns:
    str: Warnungstext wenn Abweichung > 0.02 EUR, sonst None.
  """
  warenwert = compute_warenwert(items)
  try:
    ki_gesamt = float(str(gesamt_ekp_brutto or 0).replace(",", "."))
  except (ValueError, TypeError):
    ki_gesamt = 0.0
  if ki_gesamt > 0 and abs(warenwert - ki_gesamt) > 0.02:
    return f"Preisabweichung: Berechnet {warenwert:.2f} EUR vs. KI {ki_gesamt:.2f} EUR"
  return None


# ── Summen-Banner aktualisieren ──────────────────────────────────────────────

def refresh_summen_banner(banner_widget, items_widget, payload) -> None:
  """Aktualisiert das Summen-Banner aus Items-Widget und Payload.

  Kapselt den wiederkehrenden Post-Populate-Aufruf nach populate_einkauf_widgets().
  SummenBannerWidget.update_from_items() verwaltet setVisible(True/False) selbst.

  Args:
    banner_widget: SummenBannerWidget-Instanz.
    items_widget:  EinkaufItemsTableWidget-Instanz.
    payload:       Payload-Dict mit optionalem 'gesamt_ekp_brutto'.
  """
  items = items_widget.get_items()
  gesamt = (payload or {}).get("gesamt_ekp_brutto", 0) if isinstance(payload, dict) else 0
  banner_widget.update_from_items(items, gesamt)


# ── Validierung und Warnungen ────────────────────────────────────────────────

# Pflichtfelder, deren Fehlen in den Einkauf-Warnungen gemeldet wird.
_EINKAUF_WARN_FIELDS = [
    ("bestellnummer", "Bestellnummer"),
    ("shop_name", "Shop"),
    ("bestell_datum", "Bestelldatum"),
]


def _field_text(form_widget, key):
  """Liest den Textinhalt eines Feldes aus EinkaufHeadFormWidget.inputs."""
  w = form_widget.inputs.get(key)
  return str(w.text()).strip() if w and hasattr(w, "text") else ""


def validate_einkauf_waren(items):
  """Validiert die Waren-Liste und gibt gefundene Probleme zurueck.

  Prueft:
  - Keine Artikelpositionen vorhanden
  - Keine Position mit Produktname

  Args:
    items: Liste von Artikel-Dicts.

  Returns:
    list[str]: Leere Liste wenn alles OK, sonst Fehlertexte.
  """
  issues = []
  if not items:
    issues.append("Keine Artikelpositionen vorhanden")
    return issues
  if not any(str(it.get("produkt_name", "")).strip() for it in items):
    issues.append("Mindestens eine Position muss einen Produktnamen haben")
  return issues


def collect_einkauf_warnings(form_widget, items, payload):
  """Sammelt Standard-Einkauf-Review-Warnungen.

  Prueft Preisdelta, fehlende Pflichtfelder und leere Artikelliste.
  Die Warnungen werden in der Reihenfolge zurueckgegeben:
  1. Preisabweichung (KI vs. berechneter Warenwert)
  2. Fehlende Pflichtfelder (Bestellnummer, Shop, Bestelldatum)
  3. Keine Artikelpositionen

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz (fuer Pflichtfeld-Pruefung).
    items: Liste von Artikel-Dicts (z.B. von items_widget.get_items()).
    payload: Payload-Dict mit optionalem 'gesamt_ekp_brutto'.

  Returns:
    list[str]: Warnungstexte (leer wenn alles OK).
  """
  warnings = []

  # Preisdelta
  safe_payload = payload if isinstance(payload, dict) else {}
  delta = check_warenwert_delta(items, safe_payload.get("gesamt_ekp_brutto", 0))
  if delta:
    warnings.append(delta)

  # Fehlende Pflichtfelder
  for key, label in _EINKAUF_WARN_FIELDS:
    if not _field_text(form_widget, key):
      warnings.append(f'Feld "{label}" ist leer')

  # Artikelpositionen
  if not items:
    warnings.append("Keine Artikelpositionen vorhanden")

  return warnings


# ── Pflichtfeld-Markierung ───────────────────────────────────────────────────

def mark_pflichtfeld(form_widget, field_key, is_valid, tooltip=None):
  """Setzt oder entfernt die visuelle Pflichtfeld-Markierung auf einem Feld.

  Markiert das innere QLineEdit des InlineChangeFieldRow rot (Border + Tooltip)
  wenn is_valid False ist, raeumt die Markierung bei True auf.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    field_key: Schluessel des Feldes (z.B. 'bestellnummer').
    is_valid: True = Feld gueltig (Markierung entfernen), False = markieren.
    tooltip: Optionaler Tooltip-Text fuer ungueltige Felder.
  """
  widget = form_widget.inputs.get(field_key)
  if not widget:
    return
  inner = getattr(widget, "normal_input", widget)
  if not is_valid:
    inner.setStyleSheet(
      "QLineEdit { border: 1px solid #f7768e; background-color: #2d1f2f; }"
    )
    inner.setToolTip(tooltip or "Pflichtfeld muss ausgefuellt sein")
  else:
    inner.setStyleSheet("")
    inner.setToolTip("")


# ── Save-Readiness ───────────────────────────────────────────────────────────

def check_einkauf_save_ready(form_widget, items_widget, mark_fields=False, tooltip=None):
  """Prueft ob die Mindestanforderungen fuer einen Einkauf-Save erfuellt sind.

  Benoetigt mindestens:
  - Bestellnummer ausgefuellt
  - Mindestens eine Artikelposition mit Produktname

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
    mark_fields: Wenn True, wird die Bestellnummer visuell markiert
        (rot bei leer, neutral bei gefuellt).
    tooltip: Optionaler Tooltip fuer leeres Bestellnummer-Feld.

  Returns:
    bool: True wenn speicherbereit.
  """
  bestellnummer = _field_text(form_widget, "bestellnummer")
  if mark_fields:
    mark_pflichtfeld(form_widget, "bestellnummer", bool(bestellnummer), tooltip)
  items = items_widget.get_items()
  has_named_item = any(str(it.get("produkt_name", "")).strip() for it in items)
  return bool(bestellnummer) and has_named_item


# ── Validierungs-Checkliste ──────────────────────────────────────────────────

# Standard-Einkauf-Checklisten-Felder: (field_key, Label).
_EINKAUF_CHECKLIST_FIELDS = [
    ("bestellnummer", "Bestellnummer"),
    ("shop_name", "Shop-Name"),
    ("bestell_datum", "Bestelldatum"),
]


def build_einkauf_checklist(form_widget, items):
  """Erstellt die Standard-Einkauf-Validierungs-Checkliste.

  Prueft Pflichtfelder, Gesamtpreis > 0 und mindestens 1 Artikel.
  Modulspezifische Eintraege (z.B. 'Mapping erledigt') koennen
  vom Aufrufer an die zurueckgegebene Liste angehaengt werden.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items: Liste von Artikel-Dicts (z.B. von items_widget.get_items()).

  Returns:
    list[tuple[str, bool]]: Liste von (Label, ist_ok) Eintraegen.
  """
  checks = []
  for key, label in _EINKAUF_CHECKLIST_FIELDS:
    checks.append((label, bool(_field_text(form_widget, key))))

  # Gesamtpreis > 0 (float-Parse)
  gesamt_ok = False
  try:
    gesamt_text = _field_text(form_widget, "gesamt_ekp_brutto")
    if gesamt_text:
      gesamt_ok = float(gesamt_text.replace(",", ".")) > 0
  except (ValueError, TypeError):
    pass
  checks.append(("Gesamtpreis", gesamt_ok))
  checks.append(("Min. 1 Artikel", len(items or []) > 0))
  return checks


def format_checklist_text(checks):
  """Formatiert eine Checkliste als mehrzeiligen Text mit Haekchen/Kreuzen.

  Args:
    checks: Liste von (Label, ist_ok) Tupeln.

  Returns:
    str: Mehrzeiliger Text mit ✓/✗ pro Eintrag.
  """
  return "\n".join(
    f"{'\u2713' if ok else '\u2717'} {label}" for label, ok in checks
  )


# ── Order-Number-Callback ───────────────────────────────────────────────────

def make_order_number_callback(inputs_dict, payload_dict, text_fn=None):
  """Erzeugt einen Callback fuer Bestellnummer-Aenderungen durch die Pipeline.

  Beide Module erzeugen in ihrem Save-Flow einen identischen Callback,
  der bei Pipeline-generierter Bestellnummer sowohl das Widget als auch
  das Payload-Dict aktualisiert. Diese Factory vereinheitlicht das Pattern.

  Args:
    inputs_dict: Dict mit Widget-Referenzen (z.B. form_widget.inputs).
    payload_dict: Dict dessen 'bestellnummer'-Schluessel aktualisiert wird.
    text_fn: Optionale Text-Normalisierungsfunktion (z.B. _safe_text).

  Returns:
    callable: Callback-Funktion fuer on_order_number_changed.
  """
  def _on_order_number_changed(new_no):
    text = text_fn(new_no) if text_fn else new_no
    payload_dict["bestellnummer"] = text
    widget = inputs_dict.get("bestellnummer")
    if widget:
      widget.setText(text)
  return _on_order_number_changed


# ── Widget-Reset ─────────────────────────────────────────────────────────────

def reset_einkauf_widgets(form_widget, items_widget):
  """Setzt beide Einkauf-Widgets auf ihren Ausgangszustand zurueck.

  Leert Kopfdaten-Formular, Inline-Suggestions und Artikeltabelle.
  Geeignet nach erfolgreichem Speichern oder beim Moduswechsel.

  Args:
    form_widget: EinkaufHeadFormWidget-Instanz.
    items_widget: EinkaufItemsTableWidget-Instanz.
  """
  form_widget.clear_values()
  for widget in form_widget.inputs.values():
    if hasattr(widget, "clear_suggestions"):
      widget.clear_suggestions()
  items_widget.clear_items()
