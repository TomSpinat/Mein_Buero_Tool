"""Gemeinsame Einkauf-Review-Helfer fuer Modul 1 (Order Entry) und Modul 2 (Mail Scraper).

Stellt kleine stateless Funktionen bereit, die den Einkauf-Review-Pfad
zwischen beiden Modulen vereinheitlichen:
- Widget-Population (Payload → EinkaufHeadFormWidget + EinkaufItemsTableWidget)
- Payload-Collection (Widgets → Payload)
- Post-Save-Aktionen (Bildentscheidungen + Pending Matches)
- Review-Bundle-Verteilung (bundle → EinkaufHeadFormWidget + EinkaufItemsTableWidget)
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
