from collections import defaultdict


def _to_float(value, default=0.0):
    if value in (None, ""):
        return float(default)
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return float(default)


def _to_int(value, default=1):
    try:
        return int(float(str(value or default).replace(",", ".")))
    except Exception:
        return int(default)


def _round_money(value):
    return round(float(value or 0.0) + 1e-9, 2)


def _has_value(value):
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _to_netto(brutto, ust_satz_pct):
    ust = _to_float(ust_satz_pct, 0.0)
    brutto_value = _to_float(brutto, 0.0)
    if ust > 0:
        return _round_money(brutto_value / (1 + ust / 100))
    return _round_money(brutto_value)


def _setting_value(settings, key, default=None):
    if settings is None:
        return default
    getter = getattr(settings, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            return getter(key)
    if isinstance(settings, dict):
        return settings.get(key, default)
    return default


def _normalize_steuer_modus(value):
    mode = str(value or "").strip().lower()
    return "regelbesteuerung" if mode == "regelbesteuerung" else "kleinunternehmer"


def _infer_purchase_ust_from_totals(payload):
    payload = dict(payload or {})
    brutto = _to_float(
        payload.get("einstand_gesamt_brutto")
        or payload.get("gesamt_ekp_brutto")
        or payload.get("total_brutto")
        or 0.0
    )
    netto = _to_float(
        payload.get("einstand_gesamt_netto")
        or payload.get("total_netto")
        or payload.get("gesamt_netto")
        or 0.0
    )
    if brutto > 0 and netto > 0 and brutto > netto:
        inferred = ((brutto / netto) - 1) * 100
        for candidate in (19.0, 7.0, 20.0, 21.0):
            if abs(inferred - candidate) <= 0.35:
                return candidate, "derived"
        if 0 < inferred < 30:
            return round(inferred, 2), "derived"
    return 0.0, ""


def resolve_purchase_tax_context(payload, settings=None, steuer_modus=None, default_ust_satz=19.0):
    payload = dict(payload or {})
    mode = _normalize_steuer_modus(
        steuer_modus if steuer_modus is not None else _setting_value(settings, "steuer_modus", "kleinunternehmer")
    )
    default_ust = _to_float(
        default_ust_satz if default_ust_satz is not None else _setting_value(settings, "default_ust_satz", 19.0),
        19.0,
    )
    is_reverse_charge = bool(payload.get("reverse_charge", False))

    if mode != "regelbesteuerung" or is_reverse_charge:
        return {
            "steuer_modus": mode,
            "reverse_charge": is_reverse_charge,
            "ust_satz": 0.0,
            "uses_default_ust": False,
            "source": "net_mode",
            "display_kind": "netto",
        }

    explicit_ust = _to_float(payload.get("ust_satz", 0.0))
    if explicit_ust > 0:
        return {
            "steuer_modus": mode,
            "reverse_charge": is_reverse_charge,
            "ust_satz": explicit_ust,
            "uses_default_ust": False,
            "source": "explicit",
            "display_kind": "brutto",
        }

    inferred_ust, inferred_source = _infer_purchase_ust_from_totals(payload)
    if inferred_ust > 0:
        return {
            "steuer_modus": mode,
            "reverse_charge": is_reverse_charge,
            "ust_satz": inferred_ust,
            "uses_default_ust": False,
            "source": inferred_source,
            "display_kind": "brutto",
        }

    return {
        "steuer_modus": mode,
        "reverse_charge": is_reverse_charge,
        "ust_satz": default_ust,
        "uses_default_ust": True,
        "source": "default",
        "display_kind": "brutto",
    }


def infer_purchase_ust_satz(payload, default_ust_satz=19.0, steuer_modus=None, settings=None):
    return _to_float(
        resolve_purchase_tax_context(
            payload,
            settings=settings,
            steuer_modus=steuer_modus,
            default_ust_satz=default_ust_satz,
        ).get("ust_satz", 0.0),
        0.0,
    )


def build_purchase_amount_field_labels(payload, settings=None, steuer_modus=None, default_ust_satz=19.0):
    tax_context = resolve_purchase_tax_context(
        payload,
        settings=settings,
        steuer_modus=steuer_modus,
        default_ust_satz=default_ust_satz,
    )
    total_suffix = str(tax_context.get("display_kind", "brutto") or "brutto")
    component_suffix = total_suffix

    effective_ust = _to_float(tax_context.get("ust_satz", 0.0), 0.0)
    payload = dict(payload or {})
    warenwert = _round_money(_to_float(payload.get("warenwert_brutto", 0.0)))
    versand = _round_money(_to_float(payload.get("versandkosten_brutto", 0.0)))
    neben = _round_money(_to_float(payload.get("nebenkosten_brutto", 0.0)))
    rabatt = _round_money(abs(_to_float(payload.get("rabatt_brutto", 0.0))))
    components_total = _round_money(warenwert + versand + neben - rabatt)
    gross_total = _round_money(
        _to_float(
            payload.get("einstand_gesamt_brutto")
            or payload.get("gesamt_ekp_brutto")
            or payload.get("total_brutto")
            or 0.0
        )
    )
    net_total = _round_money(
        _to_float(
            payload.get("einstand_gesamt_netto")
            or payload.get("total_netto")
            or payload.get("gesamt_netto")
            or 0.0
        )
    )

    if total_suffix == "brutto" and effective_ust > 0:
        if net_total > 0 and abs(components_total - net_total) <= 0.05:
            component_suffix = "netto"
        elif gross_total > 0 and abs(components_total - gross_total) <= 0.05:
            component_suffix = "brutto"

    return {
        "gesamt_ekp_brutto": f"Gesamtpreis ({total_suffix})",
        "versandkosten_brutto": f"Versandkosten ({component_suffix})",
        "nebenkosten_brutto": f"Nebenkosten ({component_suffix})",
        "rabatt_brutto": f"Rabatt/Gutschrift ({component_suffix})",
    }


def format_eur(value):
    return f"{_to_float(value, 0.0):,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", ".")


def build_money_breakdown(label, value, formula_text="", parts=None, fallback_note=""):
    return {
        "label": str(label or "Betrag"),
        "value": _round_money(value),
        "formula_text": str(formula_text or "").strip(),
        "parts": list(parts or []),
        "fallback_note": str(fallback_note or "").strip(),
    }


def render_money_tooltip(breakdown):
    if not isinstance(breakdown, dict):
        return ""

    lines = [str(breakdown.get("label", "Betrag") or "Betrag")]
    formula_text = str(breakdown.get("formula_text", "") or "").strip()
    if formula_text:
        lines.append(formula_text)

    for part in list(breakdown.get("parts", []) or [])[:5]:
        if isinstance(part, str):
            if part.strip():
                lines.append(part.strip())
            continue
        if not isinstance(part, dict):
            continue
        part_label = str(part.get("label", "") or "").strip()
        if not part_label:
            continue
        if "value_text" in part:
            lines.append(f"{part_label}: {str(part.get('value_text', '') or '').strip()}")
            continue
        lines.append(f"{part_label}: {format_eur(part.get('value', 0.0))}")

    fallback_note = str(breakdown.get("fallback_note", "") or "").strip()
    if fallback_note:
        lines.append(fallback_note)

    lines.append(f"Ergebnis: {format_eur(breakdown.get('value', 0.0))}")
    return "\n".join(line for line in lines if line)


def _equation_with_terms(name, terms, result):
    pieces = []
    for index, term in enumerate(list(terms or [])):
        if not isinstance(term, dict):
            continue
        label = str(term.get("label", "") or "").strip()
        operator = str(term.get("operator", "+" if index > 0 else "") or "")
        value = abs(_to_float(term.get("value", 0.0)))
        term_text = f"{label} {format_eur(value)}" if label else format_eur(value)
        if index == 0 or not operator:
            pieces.append(term_text)
        else:
            pieces.append(f"{operator} {term_text}")
    if not pieces:
        return f"{str(name or 'Betrag')}: {format_eur(result)}"
    return f"{str(name or 'Betrag')} = {' '.join(pieces)} = {format_eur(result)}"


def _division_equation(name, numerator_label, numerator_value, divisor_label, divisor_value, result):
    return (
        f"{str(name or 'Betrag')} = "
        f"{str(numerator_label or 'Wert')} {format_eur(numerator_value)} / "
        f"{str(divisor_label or 'Divisor')} {str(divisor_value)} = {format_eur(result)}"
    )


def _build_purchase_summary_tooltips(meta, header_total, source_totals):
    summary_tooltips = {}
    warenwert = _round_money(meta.get("warenwert_brutto", 0.0))
    versand = _round_money(meta.get("versandkosten_brutto", 0.0))
    neben = _round_money(meta.get("nebenkosten_brutto", 0.0))
    rabatt = _round_money(meta.get("rabatt_brutto", 0.0))
    bezugskosten = _round_money(versand + neben - rabatt)
    einstand_brutto = _round_money(meta.get("einstand_gesamt_brutto", 0.0))
    einstand_netto = _round_money(meta.get("einstand_gesamt_netto", 0.0))
    artikel_teile = []
    for source_index in sorted(source_totals.keys())[:4]:
        total_value = _round_money(source_totals.get(source_index, {}).get("warenwert_brutto", 0.0))
        artikel_teile.append({"label": f"Artikel {int(source_index) + 1}", "value": total_value})

    warenwert_formel = _equation_with_terms("Warenwert brutto", artikel_teile or [{"label": "Produktpreise zusammen", "value": warenwert}], warenwert)
    if len(source_totals) > 4:
        warenwert_note = f"Weitere Artikel sind im Gesamtwert enthalten. Insgesamt: {len(source_totals)} Artikelzeilen."
    else:
        warenwert_note = ""

    summary_tooltips["warenwert_brutto"] = render_money_tooltip(
        build_money_breakdown(
            "Warenwert brutto",
            warenwert,
            warenwert_formel,
            parts=artikel_teile or [{"label": "Produktpreise zusammen", "value": warenwert}],
            fallback_note=warenwert_note,
        )
    )
    summary_tooltips["bezugskosten_total"] = render_money_tooltip(
        build_money_breakdown(
            "Bezugskosten",
            bezugskosten,
            _equation_with_terms(
                "Bezugskosten",
                [
                    {"label": "Versand", "value": versand},
                    {"label": "Nebenkosten", "value": neben, "operator": "+"},
                    {"label": "Rabatt", "value": rabatt, "operator": "-"},
                ],
                bezugskosten,
            ),
            parts=[
                {"label": "Versand", "value": versand},
                {"label": "Nebenkosten", "value": neben},
                {"label": "Rabatt", "value": rabatt},
            ],
        )
    )
    summary_tooltips["einstand_gesamt_brutto"] = render_money_tooltip(
        build_money_breakdown(
            "Einstand gesamt brutto",
            einstand_brutto,
            _equation_with_terms(
                "Einstand gesamt brutto",
                [
                    {"label": "Warenwert", "value": warenwert},
                    {"label": "Versand", "value": versand, "operator": "+"},
                    {"label": "Nebenkosten", "value": neben, "operator": "+"},
                    {"label": "Rabatt", "value": rabatt, "operator": "-"},
                ],
                einstand_brutto,
            ),
            parts=[
                {"label": "Warenwert", "value": warenwert},
                {"label": "Versand", "value": versand},
                {"label": "Nebenkosten", "value": neben},
                {"label": "Rabatt", "value": rabatt},
            ],
        )
    )
    netto_divisor = _round_money(1 + (_to_float(meta.get("ust_satz_ekp", 0.0)) / 100))
    netto_fallback = "Bei Kleinunternehmer oder Reverse Charge ist brutto gleich netto."
    netto_formel = (
        f"Einstand gesamt netto = Einstand brutto {format_eur(einstand_brutto)} = {format_eur(einstand_netto)}"
        if abs(einstand_brutto - einstand_netto) < 0.01
        else _division_equation("Einstand gesamt netto", "Einstand brutto", einstand_brutto, "USt-Faktor", netto_divisor, einstand_netto)
    )
    summary_tooltips["einstand_gesamt_netto"] = render_money_tooltip(
        build_money_breakdown(
            "Einstand gesamt netto",
            einstand_netto,
            netto_formel,
            parts=[
                {"label": "Einstand brutto", "value": einstand_brutto},
            ],
            fallback_note=netto_fallback,
        )
    )
    if header_total > 0:
        delta = _round_money(abs(warenwert - header_total))
        summary_tooltips["delta_to_header_total"] = render_money_tooltip(
            build_money_breakdown(
                "Abweichung",
                delta,
                f"Abweichung = |Berechnet {format_eur(warenwert)} - Erkannter Gesamtpreis {format_eur(header_total)}| = {format_eur(delta)}",
                parts=[
                    {"label": "Berechnet", "value": warenwert},
                    {"label": "Erkannter Gesamtpreis", "value": header_total},
                ],
            )
        )
    return summary_tooltips


def _build_purchase_source_tooltips(source_totals, meta):
    item_tooltips = {}
    versand = _round_money(meta.get("versandkosten_brutto", 0.0))
    neben = _round_money(meta.get("nebenkosten_brutto", 0.0))
    rabatt = _round_money(meta.get("rabatt_brutto", 0.0))
    ust_satz = _to_float(meta.get("ust_satz_ekp", 0.0))
    ust_faktor = f"{(1 + ust_satz / 100):.2f}".replace(".", ",") if ust_satz > 0 else "1,00"
    gesamt_warenwert = _round_money(sum(_round_money(totals.get("warenwert_brutto", 0.0)) for totals in source_totals.values()))
    bezugskosten_gesamt = _round_money(versand + neben - rabatt)
    for source_index, totals in source_totals.items():
        qty = max(1, int(totals.get("menge", 1) or 1))
        warenwert = _round_money(totals.get("warenwert_brutto", 0.0))
        bezug = _round_money(totals.get("bezugskosten_anteil_brutto", 0.0))
        einstand = _round_money(totals.get("einstand_brutto", 0.0))
        einstand_netto = _round_money(totals.get("einstand_netto", 0.0))
        anteil_prozent = 0.0
        if gesamt_warenwert > 0:
            anteil_prozent = round((warenwert / gesamt_warenwert) * 100, 2)

        item_tooltips[int(source_index)] = {
            "bezugskosten_anteil_brutto": render_money_tooltip(
                build_money_breakdown(
                    "Bezugskostenanteil",
                    bezug,
                    (
                        f"Bezugskostenanteil = Bezugskosten gesamt {format_eur(bezugskosten_gesamt)} "
                        f"x Positionsanteil {anteil_prozent:.2f} % = {format_eur(bezug)}"
                    ),
                    parts=[
                        {"label": "Stueckzahl", "value_text": str(qty)},
                        {"label": "Produktpreis gesamt", "value": warenwert},
                        {"label": "Bezugskosten gesamt", "value": bezugskosten_gesamt},
                        {"label": "Positionsanteil", "value_text": f"{anteil_prozent:.2f} %"},
                        {"label": "Versand", "value": versand},
                        {"label": "Nebenkosten", "value": neben},
                        {"label": "Rabatt", "value": rabatt},
                    ],
                )
            ),
            "einstand_brutto": render_money_tooltip(
                build_money_breakdown(
                    "Einstand brutto",
                    einstand,
                    _equation_with_terms(
                        "Einstand brutto",
                        [
                            {"label": "Produktpreis", "value": warenwert},
                            {"label": "Bezugskostenanteil", "value": bezug, "operator": "+"},
                        ],
                        einstand,
                    ),
                    parts=[
                        {"label": "Produktpreis gesamt", "value": warenwert},
                        {"label": "Bezugskostenanteil", "value": bezug},
                    ],
                )
            ),
            "einstand_netto": render_money_tooltip(
                build_money_breakdown(
                    "Einstand netto",
                    einstand_netto,
                    (
                        f"Einstand netto = Einstand brutto {format_eur(einstand)} = {format_eur(einstand_netto)}"
                        if abs(einstand - einstand_netto) < 0.01
                        else _division_equation("Einstand netto", "Einstand brutto", einstand, "USt-Faktor", ust_faktor, einstand_netto)
                    ),
                    parts=[
                        {"label": "Einstand brutto", "value": einstand},
                    ],
                    fallback_note="Bei Kleinunternehmer oder Reverse Charge ist brutto gleich netto.",
                )
            ),
        }
    return item_tooltips


def calculate_purchase_costs(data_dict, unit_rows, steuer_modus="kleinunternehmer", default_ust_satz=19.0):
    data_dict = dict(data_dict or {})
    prepared_rows = [dict(row or {}) for row in (unit_rows or []) if isinstance(row, dict)]
    total_units = max(1, len(prepared_rows))
    warenwert_brutto = _round_money(sum(_to_float(row.get("ekp_brutto", 0.0)) for row in prepared_rows))

    header_total = _to_float(data_dict.get("gesamt_ekp_brutto", 0.0))
    versand = _to_float(data_dict.get("versandkosten_brutto", 0.0))
    neben = _to_float(data_dict.get("nebenkosten_brutto", 0.0))
    rabatt = abs(_to_float(data_dict.get("rabatt_brutto", 0.0)))

    explicit_components = any(
        _has_value(data_dict.get(key))
        for key in ("versandkosten_brutto", "nebenkosten_brutto", "rabatt_brutto")
    )

    if explicit_components:
        einstand_gesamt = warenwert_brutto + versand + neben - rabatt
        if header_total > 0 and abs(header_total - einstand_gesamt) > 0.01:
            delta = header_total - einstand_gesamt
            if delta >= 0:
                neben += delta
            else:
                rabatt += abs(delta)
            einstand_gesamt = header_total
    else:
        if header_total > 0:
            delta = header_total - warenwert_brutto
            if delta >= 0:
                neben = delta
            else:
                rabatt = abs(delta)
            einstand_gesamt = header_total
        else:
            einstand_gesamt = warenwert_brutto

    versand = _round_money(versand)
    neben = _round_money(neben)
    rabatt = _round_money(rabatt)
    einstand_gesamt = _round_money(einstand_gesamt)
    extras_total = _round_money(versand + neben - rabatt)

    use_value_weight = warenwert_brutto > 0.0
    distributed = []
    distributed_sum = 0.0
    for row in prepared_rows:
        base = _to_float(row.get("ekp_brutto", 0.0))
        if use_value_weight:
            share_raw = extras_total * (base / warenwert_brutto)
        else:
            share_raw = extras_total / total_units
        share = _round_money(share_raw)
        distributed.append(share)
        distributed_sum += share

    rounding_delta = _round_money(extras_total - distributed_sum)
    if distributed and abs(rounding_delta) > 0:
        distributed[-1] = _round_money(distributed[-1] + rounding_delta)

    is_reverse_charge = bool(data_dict.get("reverse_charge", False))
    ust_satz = infer_purchase_ust_satz(
        data_dict,
        default_ust_satz=default_ust_satz,
        steuer_modus=steuer_modus,
    )
    netto_ust = 0.0 if (steuer_modus != "regelbesteuerung" or is_reverse_charge) else ust_satz

    source_totals = defaultdict(
        lambda: {
            "menge": 0,
            "warenwert_brutto": 0.0,
            "bezugskosten_anteil_brutto": 0.0,
            "einstand_brutto": 0.0,
            "einstand_netto": 0.0,
        }
    )

    for idx, row in enumerate(prepared_rows):
        bezugskosten_anteil = distributed[idx] if idx < len(distributed) else 0.0
        ekp = _to_float(row.get("ekp_brutto", 0.0))
        row["bezugskosten_anteil_brutto"] = _round_money(bezugskosten_anteil)
        row["einstand_brutto"] = _round_money(ekp + bezugskosten_anteil)
        row["ust_satz_ekp"] = ust_satz
        row["reverse_charge"] = is_reverse_charge
        if steuer_modus == "regelbesteuerung":
            row["einstand_netto"] = _to_netto(row["einstand_brutto"], netto_ust)
        else:
            row["einstand_netto"] = row["einstand_brutto"]

        source_index = row.get("source_row_index")
        if source_index is not None and str(source_index).strip() != "":
            bucket = source_totals[int(source_index)]
            bucket["menge"] += 1
            bucket["warenwert_brutto"] = _round_money(bucket["warenwert_brutto"] + ekp)
            bucket["bezugskosten_anteil_brutto"] = _round_money(
                bucket["bezugskosten_anteil_brutto"] + row["bezugskosten_anteil_brutto"]
            )
            bucket["einstand_brutto"] = _round_money(bucket["einstand_brutto"] + row["einstand_brutto"])
            bucket["einstand_netto"] = _round_money(bucket["einstand_netto"] + row["einstand_netto"])

    einstand_gesamt_netto = _to_netto(einstand_gesamt, netto_ust)

    meta = {
        "warenwert_brutto": warenwert_brutto,
        "versandkosten_brutto": versand,
        "nebenkosten_brutto": neben,
        "rabatt_brutto": rabatt,
        "einstand_gesamt_brutto": einstand_gesamt,
        "einstand_gesamt_netto": einstand_gesamt_netto,
        "ust_satz_ekp": ust_satz,
        "reverse_charge": is_reverse_charge,
        "_money_tooltips": _build_purchase_summary_tooltips(
            {
                "warenwert_brutto": warenwert_brutto,
                "versandkosten_brutto": versand,
                "nebenkosten_brutto": neben,
                "rabatt_brutto": rabatt,
                "einstand_gesamt_brutto": einstand_gesamt,
                "einstand_gesamt_netto": einstand_gesamt_netto,
                "ust_satz_ekp": ust_satz,
            },
            _round_money(header_total),
            source_totals,
        ),
        "_item_money_tooltips": _build_purchase_source_tooltips(
            source_totals,
            {
                "versandkosten_brutto": versand,
                "nebenkosten_brutto": neben,
                "rabatt_brutto": rabatt,
                "ust_satz_ekp": ust_satz,
            },
        ),
        "_source_row_costs": {int(key): dict(value) for key, value in source_totals.items()},
    }
    return meta, prepared_rows


def calculate_purchase_payload_breakdown(payload, settings=None):
    payload = dict(payload or {})
    items = payload.get("waren", []) or []
    steuer_modus = _setting_value(settings, "steuer_modus", "kleinunternehmer")
    default_ust_satz = _setting_value(settings, "default_ust_satz", 19.0)
    unit_rows = []
    for source_index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        menge = max(1, _to_int(item.get("menge", 1), 1))
        for _ in range(menge):
            unit_rows.append(
                {
                    "source_row_index": source_index,
                    "ekp_brutto": _to_float(item.get("ekp_brutto", 0.0)),
                }
            )

    if unit_rows:
        meta, _prepared_rows = calculate_purchase_costs(
            payload,
            unit_rows,
            steuer_modus=steuer_modus,
            default_ust_satz=default_ust_satz,
        )
        return meta

    summary_tooltips = {}
    stored_warenwert = _round_money(_to_float(payload.get("warenwert_brutto", 0.0)))
    stored_versand = _round_money(_to_float(payload.get("versandkosten_brutto", 0.0)))
    stored_neben = _round_money(_to_float(payload.get("nebenkosten_brutto", 0.0)))
    stored_rabatt = _round_money(abs(_to_float(payload.get("rabatt_brutto", 0.0))))
    stored_einstand_brutto = _round_money(_to_float(payload.get("einstand_gesamt_brutto", 0.0)))
    stored_einstand_netto = _round_money(_to_float(payload.get("einstand_gesamt_netto", 0.0)))
    tax_context = resolve_purchase_tax_context(
        payload,
        settings=settings,
        steuer_modus=steuer_modus,
        default_ust_satz=_setting_value(settings, "default_ust_satz", 19.0),
    )
    stored_ust = _to_float(tax_context.get("ust_satz", 0.0), 0.0)
    stored_ust_faktor = f"{(1 + stored_ust / 100):.2f}".replace(".", ",") if stored_ust > 0 else "1,00"
    missing_parts_note = "Einzelwerte fehlen, deshalb ist nur der gespeicherte Endwert bekannt."

    if abs(stored_warenwert) >= 0.0001:
        summary_tooltips["warenwert_brutto"] = render_money_tooltip(
            build_money_breakdown(
                "Warenwert brutto",
                stored_warenwert,
                f"Warenwert brutto = gespeicherter Warenwert {format_eur(stored_warenwert)} = {format_eur(stored_warenwert)}",
                parts=[
                    {"label": "Gespeicherter Warenwert", "value": stored_warenwert},
                ],
            )
        )

    bezugskosten = stored_versand + stored_neben - stored_rabatt
    if abs(bezugskosten) >= 0.0001:
        summary_tooltips["bezugskosten_total"] = render_money_tooltip(
            build_money_breakdown(
                "Bezugskosten",
                bezugskosten,
                _equation_with_terms(
                    "Bezugskosten",
                    [
                        {"label": "Versand", "value": stored_versand},
                        {"label": "Nebenkosten", "value": stored_neben, "operator": "+"},
                        {"label": "Rabatt", "value": stored_rabatt, "operator": "-"},
                    ],
                    bezugskosten,
                ),
                parts=[
                    {"label": "Versand", "value": stored_versand},
                    {"label": "Nebenkosten", "value": stored_neben},
                    {"label": "Rabatt", "value": stored_rabatt},
                ],
            )
        )

    if abs(stored_einstand_brutto) >= 0.0001:
        has_parts = any(abs(value) >= 0.0001 for value in (stored_warenwert, stored_versand, stored_neben, stored_rabatt))
        summary_tooltips["einstand_gesamt_brutto"] = render_money_tooltip(
            build_money_breakdown(
                "Einstand gesamt brutto",
                stored_einstand_brutto,
                (
                    _equation_with_terms(
                        "Einstand gesamt brutto",
                        [
                            {"label": "Warenwert", "value": stored_warenwert},
                            {"label": "Versand", "value": stored_versand, "operator": "+"},
                            {"label": "Nebenkosten", "value": stored_neben, "operator": "+"},
                            {"label": "Rabatt", "value": stored_rabatt, "operator": "-"},
                        ],
                        stored_einstand_brutto,
                    )
                    if has_parts
                    else f"Einstand gesamt brutto = gespeicherter Endwert {format_eur(stored_einstand_brutto)} = {format_eur(stored_einstand_brutto)}"
                ),
                parts=[
                    {"label": "Warenwert", "value": stored_warenwert},
                    {"label": "Versand", "value": stored_versand},
                    {"label": "Nebenkosten", "value": stored_neben},
                    {"label": "Rabatt", "value": stored_rabatt},
                ] if has_parts else [{"label": "Gespeicherter Endwert", "value": stored_einstand_brutto}],
                fallback_note="" if has_parts else missing_parts_note,
            )
        )

    if abs(stored_einstand_netto) >= 0.0001:
        netto_same = abs(stored_einstand_brutto - stored_einstand_netto) < 0.01
        summary_tooltips["einstand_gesamt_netto"] = render_money_tooltip(
            build_money_breakdown(
                "Einstand gesamt netto",
                stored_einstand_netto,
                (
                    f"Einstand gesamt netto = Einstand brutto {format_eur(stored_einstand_brutto)} = {format_eur(stored_einstand_netto)}"
                    if netto_same
                    else _division_equation(
                        "Einstand gesamt netto",
                        "Einstand brutto",
                        stored_einstand_brutto,
                        "USt-Faktor",
                        stored_ust_faktor,
                        stored_einstand_netto,
                    )
                ),
                parts=[
                    {"label": "Einstand brutto", "value": stored_einstand_brutto},
                    {"label": "USt-Satz", "value_text": f"{stored_ust:.2f} %".replace(".", ",")},
                ],
                fallback_note="Bei Kleinunternehmer oder Reverse Charge ist brutto gleich netto." if netto_same else "",
            )
        )

    return {
        "warenwert_brutto": stored_warenwert,
        "versandkosten_brutto": stored_versand,
        "nebenkosten_brutto": stored_neben,
        "rabatt_brutto": stored_rabatt,
        "einstand_gesamt_brutto": stored_einstand_brutto,
        "einstand_gesamt_netto": stored_einstand_netto,
        "ust_satz": stored_ust,
        "_money_tooltips": summary_tooltips,
        "_item_money_tooltips": {},
        "_source_row_costs": {},
    }


def build_finance_money_tooltips(metrics):
    metrics = dict(metrics or {})
    return {
        "lagerwert": render_money_tooltip(
            build_money_breakdown(
                "Gebundenes Kapital",
                metrics.get("lagerwert", 0.0),
                "Summe aus Einstand oder EKP mal Menge fuer alle lagernden Positionen.",
            )
        ),
        "forderungen": render_money_tooltip(
            build_money_breakdown(
                "Offene Forderungen",
                metrics.get("forderungen", 0.0),
                "Summe aller offenen Verkaufspreise aus noch nicht bezahlten Tickets.",
            )
        ),
        "gewinn": render_money_tooltip(
            build_money_breakdown(
                "Realisierter Gewinn",
                metrics.get("gewinn", 0.0),
                "Summe aller gespeicherten Gewinne aus bereits bezahlten Tickets.",
            )
        ),
        "gewinn_netto": render_money_tooltip(
            build_money_breakdown(
                "Realisierter Gewinn netto",
                metrics.get("gewinn_netto", 0.0),
                "Summe aller gespeicherten Netto-Gewinne aus bereits bezahlten Tickets.",
                fallback_note="Aus bereits berechneten Positionswerten uebernommen.",
            )
        ),
        "bezugskosten": render_money_tooltip(
            build_money_breakdown(
                "Bezugskosten",
                metrics.get("bezugskosten", 0.0),
                "Summe aus Versand und Nebenkosten minus Rabatt ueber alle aktiven Einkaufsbestellungen.",
            )
        ),
        "einstand": render_money_tooltip(
            build_money_breakdown(
                "Einstand gesamt",
                metrics.get("einstand", 0.0),
                "Summe aller Einstandswerte aus aktiven Einkaufsbestellungen.",
                fallback_note="Gespeicherter Einstand wird bevorzugt, sonst der erkannte Gesamtpreis.",
            )
        ),
        "vorsteuer": render_money_tooltip(
            build_money_breakdown(
                "Vorsteuer",
                metrics.get("vorsteuer", 0.0),
                "Summe aus Einkauf brutto minus Einkauf netto fuer alle Positionen.",
            )
        ),
        "ust_schuld": render_money_tooltip(
            build_money_breakdown(
                "USt-Schuld",
                metrics.get("ust_schuld", 0.0),
                "Summe aus Verkauf brutto minus Verkauf netto fuer bereits bezahlte Tickets.",
            )
        ),
    }


def build_poms_stats_tooltips(stats):
    stats = dict(stats or {})
    return {
        "revenue_current": render_money_tooltip(
            build_money_breakdown(
                "Turnover (Mo)",
                stats.get("revenue_current", 0.0),
                "Summe aller Verkaufspreise aus dem aktuellen Monat.",
            )
        ),
        "profit_current": render_money_tooltip(
            build_money_breakdown(
                "Profit (Mo)",
                stats.get("profit_current", 0.0),
                "Summe aller gespeicherten Gewinne aus dem aktuellen Monat.",
            )
        ),
    }


def build_poms_row_money_tooltips(row):
    row = dict(row or {})
    tooltips = {}
    ek_value = _to_float(row.get("ek", 0.0))
    if abs(ek_value) >= 0.0001:
        source_note = "Einstand verwendet, sonst EKP."
        if row.get("einstand_brutto_raw") not in (None, ""):
            source_note = "Einstand der Position verwendet."
        elif row.get("ekp_brutto_raw") not in (None, ""):
            source_note = "Kein Einstand gespeichert, daher EKP verwendet."
        tooltips["ek"] = render_money_tooltip(
            build_money_breakdown(
                "EK",
                ek_value,
                "Angezeigter Einkaufswert der Position.",
                fallback_note=source_note,
            )
        )

    win_value = _to_float(row.get("win", 0.0))
    if abs(win_value) >= 0.0001:
        tooltips["win"] = render_money_tooltip(
            build_money_breakdown(
                "Win",
                win_value,
                "Gespeicherter Gewinn dieser Position.",
                fallback_note="Aus bereits berechnetem Positionsgewinn uebernommen.",
            )
        )
    return tooltips
