from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urlparse


_URL_RE = re.compile(r"https?://[^\s<>'\"\\]+", re.IGNORECASE)
_AMAZON_HOST_RE = re.compile(r"(^|\.)amazon\.[a-z.]+$", re.IGNORECASE)

_KNOWN_DIRECT_URL_FIELDS = (
    "tracking_url",
    "tracking_link",
    "tracking_href",
    "shipment_tracking_url",
    "shipment_tracking_link",
    "carrier_tracking_url",
    "carrier_tracking_link",
    "liefertracking_url",
    "versand_link",
    "tracking_page_url",
    "tracking_page_link",
    "amazon_tracking_url",
    "amazon_shiptrack_url",
)

_KNOWN_TEXT_FIELDS = (
    "_email_message",
    "_email_message_text",
    "_email_plain_text",
    "_email_html",
    "_email_body",
    "_raw_email_text",
    "_raw_email_html",
    "_mail_text",
    "_mail_html",
    "email_message",
    "email_text",
    "email_html",
    "mail_text",
    "mail_html",
    "custom_text",
)


def _clean_url(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.strip("()[]{}<>.,;\"'")
    parsed = urlparse(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return text


def _collect_urls_from_text(value) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    found = []
    seen = set()
    for match in _URL_RE.findall(text):
        cleaned = _clean_url(match)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            found.append(cleaned)
    return found


def _iter_payload_strings(payload, max_depth=5):
    if max_depth < 0:
        return
    if isinstance(payload, dict):
        for value in payload.values():
            yield from _iter_payload_strings(value, max_depth=max_depth - 1)
        return
    if isinstance(payload, (list, tuple, set)):
        for item in payload:
            yield from _iter_payload_strings(item, max_depth=max_depth - 1)
        return
    if isinstance(payload, (str, int, float, bool)):
        text = str(payload).strip()
        if text:
            yield text


def infer_carrier_from_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if not host:
        return ""
    if "dhl" in host or "deutschepost" in host:
        return "dhl"
    if "dpd" in host:
        return "dpd"
    if "gls" in host:
        return "gls"
    if "hermes" in host:
        return "hermes"
    if "ups" in host:
        return "ups"
    if "swiship" in host:
        return "swiship"
    if _AMAZON_HOST_RE.search(host) or ("amazon." in host and ("shiptrack" in path or "progress-tracker" in path)):
        return "amazon"
    return ""


def normalize_carrier_name(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "dhl" in text or "deutsche post" in text:
        return "dhl"
    if "dpd" in text:
        return "dpd"
    if "gls" in text:
        return "gls"
    if "hermes" in text:
        return "hermes"
    if "ups" in text:
        return "ups"
    if "amazon" in text:
        return "amazon"
    if "swiship" in text:
        return "swiship"
    return text


def parse_amazon_tracking_url(url: str) -> dict[str, str]:
    cleaned = _clean_url(url)
    if not cleaned:
        return {}
    parsed = urlparse(cleaned)
    host = parsed.netloc.lower().strip()
    path = parsed.path.lower().strip()
    if not _AMAZON_HOST_RE.search(host):
        return {}
    if "/gp/css/shiptrack/" not in path:
        return {}

    query = parse_qs(parsed.query)
    order_id = str((query.get("orderID") or [""])[0] or "").strip()
    ordering_shipment_id = str((query.get("orderingShipmentId") or [""])[0] or "").strip()
    package_id = str((query.get("packageId") or [""])[0] or "").strip()
    if not (host and order_id and ordering_shipment_id and package_id):
        return {}
    return {
        "tracking_url": cleaned,
        "tracking_url_kind": "amazon_shiptrack",
        "amazon_marketplace_domain": host,
        "amazon_order_id": order_id,
        "amazon_ordering_shipment_id": ordering_shipment_id,
        "amazon_package_id": package_id,
    }


def build_amazon_tracking_url(domain: str, order_id: str, ordering_shipment_id: str, package_id: str) -> str:
    host = str(domain or "").strip().lower()
    if host.startswith("https://") or host.startswith("http://"):
        host = urlparse(host).netloc.lower().strip()
    host = host.lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    host = f"www.{host}" if host else ""
    if not _AMAZON_HOST_RE.search(host):
        return ""

    order_id = str(order_id or "").strip()
    ordering_shipment_id = str(ordering_shipment_id or "").strip()
    package_id = str(package_id or "").strip()
    if not (order_id and ordering_shipment_id and package_id):
        return ""

    query = urlencode(
        {
            "ie": "UTF8",
            "orderID": order_id,
            "orderingShipmentId": ordering_shipment_id,
            "packageId": package_id,
        }
    )
    return f"https://{host}/gp/css/shiptrack/view.html?{query}"


def build_standard_tracking_url(paketdienst: str, tracking_number: str) -> str:
    carrier = normalize_carrier_name(paketdienst)
    tracking_number = str(tracking_number or "").strip()
    if not tracking_number:
        return ""
    if carrier == "dhl":
        return f"https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?piececode={tracking_number}"
    if carrier == "dpd":
        return f"https://tracking.dpd.de/status/de_DE/parcel/{tracking_number}"
    if carrier == "gls":
        return f"https://gls-group.eu/DE/de/paketverfolgung?match={tracking_number}"
    if carrier == "hermes":
        return f"https://www.myhermes.de/empfangen/sendungsverfolgung/sendungsinformation/{tracking_number}"
    if carrier == "ups":
        return f"https://www.ups.com/track?loc=de_DE&tracknum={tracking_number}"
    return ""


def collect_tracking_url_candidates(payload) -> list[dict[str, str]]:
    payload = payload if isinstance(payload, dict) else {}
    candidates = []
    seen = set()

    def _add_candidate(url_value, source_label):
        cleaned = _clean_url(url_value)
        if not cleaned or cleaned in seen:
            return
        carrier = infer_carrier_from_url(cleaned)
        amazon_data = parse_amazon_tracking_url(cleaned)
        if not carrier and not amazon_data:
            return
        seen.add(cleaned)
        entry = {
            "tracking_url": cleaned,
            "tracking_url_source": str(source_label or "payload_scan").strip() or "payload_scan",
            "tracking_url_kind": "carrier_direct",
            "carrier": carrier,
        }
        if amazon_data:
            entry.update(amazon_data)
            entry["carrier"] = "amazon"
        candidates.append(entry)

    for key in _KNOWN_DIRECT_URL_FIELDS:
        if key in payload:
            _add_candidate(payload.get(key), key)

    for row in list(payload.get("_scan_sources", []) or []):
        if not isinstance(row, dict):
            continue
        for key in ("tracking_url", "tracking_link", "source_url", "url", "href", "link"):
            if key in row:
                _add_candidate(row.get(key), f"_scan_sources.{key}")

    for key in _KNOWN_TEXT_FIELDS:
        if key in payload:
            for url in _collect_urls_from_text(payload.get(key)):
                _add_candidate(url, key)

    for value in _iter_payload_strings(payload):
        for url in _collect_urls_from_text(value):
            _add_candidate(url, "payload_text_scan")

    return candidates


def enrich_tracking_payload(data_dict) -> dict:
    payload = dict(data_dict or {})
    tracking_url = _clean_url(payload.get("tracking_url", ""))
    tracking_url_source = str(payload.get("tracking_url_source", "") or "").strip()
    tracking_url_kind = str(payload.get("tracking_url_kind", "") or "").strip()
    amazon_marketplace_domain = str(payload.get("amazon_marketplace_domain", "") or "").strip()
    amazon_order_id = str(payload.get("amazon_order_id", "") or "").strip()
    amazon_ordering_shipment_id = str(payload.get("amazon_ordering_shipment_id", "") or "").strip()
    amazon_package_id = str(payload.get("amazon_package_id", "") or "").strip()

    if tracking_url:
        direct_carrier = infer_carrier_from_url(tracking_url)
        amazon_data = parse_amazon_tracking_url(tracking_url)
        if amazon_data:
            tracking_url_kind = "amazon_shiptrack"
            amazon_marketplace_domain = amazon_marketplace_domain or amazon_data.get("amazon_marketplace_domain", "")
            amazon_order_id = amazon_order_id or amazon_data.get("amazon_order_id", "")
            amazon_ordering_shipment_id = amazon_ordering_shipment_id or amazon_data.get("amazon_ordering_shipment_id", "")
            amazon_package_id = amazon_package_id or amazon_data.get("amazon_package_id", "")
        elif direct_carrier:
            tracking_url_kind = tracking_url_kind or "carrier_direct"
        tracking_url_source = tracking_url_source or "payload_explicit"

    if not tracking_url:
        desired_carrier = normalize_carrier_name(payload.get("paketdienst", ""))
        tracking_number = str(payload.get("tracking_nummer_einkauf", "") or "").strip()
        best = None
        best_score = -1
        for candidate in collect_tracking_url_candidates(payload):
            score = 0
            candidate_carrier = candidate.get("carrier", "")
            if candidate.get("tracking_url_kind") == "amazon_shiptrack":
                score += 80
            else:
                score += 40
            if desired_carrier and candidate_carrier == desired_carrier:
                score += 30
            if desired_carrier in ("amazon", "swiship") and candidate.get("tracking_url_kind") == "amazon_shiptrack":
                score += 40
            if tracking_number and tracking_number.lower() in str(candidate.get("tracking_url", "")).lower():
                score += 10
            if score > best_score:
                best = candidate
                best_score = score
        if best:
            tracking_url = str(best.get("tracking_url", "") or "").strip()
            tracking_url_source = str(best.get("tracking_url_source", "") or "").strip()
            tracking_url_kind = str(best.get("tracking_url_kind", "") or "").strip()
            amazon_marketplace_domain = amazon_marketplace_domain or str(best.get("amazon_marketplace_domain", "") or "").strip()
            amazon_order_id = amazon_order_id or str(best.get("amazon_order_id", "") or "").strip()
            amazon_ordering_shipment_id = amazon_ordering_shipment_id or str(best.get("amazon_ordering_shipment_id", "") or "").strip()
            amazon_package_id = amazon_package_id or str(best.get("amazon_package_id", "") or "").strip()

    if not tracking_url and amazon_marketplace_domain and amazon_order_id and amazon_ordering_shipment_id and amazon_package_id:
        built = build_amazon_tracking_url(
            amazon_marketplace_domain,
            amazon_order_id,
            amazon_ordering_shipment_id,
            amazon_package_id,
        )
        if built:
            tracking_url = built
            tracking_url_kind = "amazon_shiptrack"
            tracking_url_source = tracking_url_source or "amazon_parts"

    payload["tracking_url"] = tracking_url
    payload["tracking_url_source"] = tracking_url_source
    payload["tracking_url_kind"] = tracking_url_kind
    payload["amazon_marketplace_domain"] = amazon_marketplace_domain
    payload["amazon_order_id"] = amazon_order_id
    payload["amazon_ordering_shipment_id"] = amazon_ordering_shipment_id
    payload["amazon_package_id"] = amazon_package_id
    return payload


def build_tracking_target(shipment: dict) -> dict[str, str]:
    shipment = shipment if isinstance(shipment, dict) else {}
    paketdienst = str(shipment.get("paketdienst", "") or "").strip()
    carrier = normalize_carrier_name(paketdienst)
    tracking_url = _clean_url(shipment.get("tracking_url", ""))
    tracking_url_kind = str(shipment.get("tracking_url_kind", "") or "").strip()
    tracking_number = str(shipment.get("tracking_number", shipment.get("tracking_nummer_einkauf", "")) or "").strip()

    if carrier in ("amazon", "swiship"):
        if tracking_url and tracking_url_kind == "amazon_shiptrack":
            return {"url": tracking_url, "kind": "amazon_shiptrack", "reason": ""}
        built = build_amazon_tracking_url(
            shipment.get("amazon_marketplace_domain", ""),
            shipment.get("amazon_order_id", ""),
            shipment.get("amazon_ordering_shipment_id", ""),
            shipment.get("amazon_package_id", ""),
        )
        if built:
            return {"url": built, "kind": "amazon_shiptrack", "reason": ""}
        return {
            "url": "",
            "kind": "",
            "reason": "Amazon-Tracking-Link fehlt. Es sind noch nicht genug Amazon-Daten gespeichert.",
        }

    if tracking_url:
        return {"url": tracking_url, "kind": tracking_url_kind or "carrier_direct", "reason": ""}

    built = build_standard_tracking_url(paketdienst, tracking_number)
    if built:
        return {"url": built, "kind": "carrier_standard", "reason": ""}

    return {
        "url": "",
        "kind": "",
        "reason": "Tracking-Link fehlt. Fuer diese Sendung sind weder ein direkter Link noch genug Trackingdaten gespeichert.",
    }
