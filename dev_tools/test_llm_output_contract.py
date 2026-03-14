import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""Kleine lokale Selbsttests fuer den KI-Kern und den Output-Vertrag."""

from module.ai_provider_core import ProviderResponseFormatError, parse_strict_json_object
from module.gemini_provider_adapter import GeminiStructuredProvider
from module.scan_output_contract import (
    StructuredOutputValidationError,
    get_scan_output_schema,
    validate_and_normalize_output,
)
from module.llm_output_contract import get_output_contract_schema
from module.llm_provider import ProviderRequest


def _ok(name):
    print(f"[OK] {name}")


def _fail(name, err):
    raise AssertionError(f"[FAIL] {name}: {err}")


def test_valid_einkauf():
    payload = {
        "bestellnummer": "FD-2026-PS5-441",
        "kaufdatum": "2026-03-08",
        "shop_name": "Amazon",
        "waren": [
            {
                "produkt_name": "Sony PlayStation 5 Pro",
                "varianten_info": "White",
                "ean": "0711719577293",
                "menge": "3",
                "ekp_brutto": "799.99",
            }
        ],
    }
    out = validate_and_normalize_output("einkauf", payload).to_dict()
    assert out["bestellnummer"] == "FD-2026-PS5-441"
    assert len(out["waren"]) == 1
    assert out["waren"][0]["menge"] == "3"
    _ok("valid einkauf")


def test_missing_required_field():
    payload = {
        "bestellnummer": "FD-2026-PS5-441",
    }
    try:
        validate_and_normalize_output("einkauf", payload)
    except StructuredOutputValidationError as exc:
        assert exc.error_kind == "incomplete_response"
        _ok("missing required field")
        return
    _fail("missing required field", "no exception")


def test_unexpected_field_is_captured():
    payload = {
        "ticket_name": "bounty-ticket-22031",
        "waren": [],
        "unknown_extra": "abc",
    }
    out = validate_and_normalize_output("verkauf", payload).to_dict()
    assert "_schema_unknown_fields" in out
    assert "unknown_extra" in out["_schema_unknown_fields"]
    _ok("unexpected field captured")


def test_empty_response_parser():
    try:
        parse_strict_json_object("", provider="gemini")
    except ProviderResponseFormatError as exc:
        assert exc.error_kind == "empty_response"
        _ok("empty response parser")
        return
    _fail("empty response parser", "no exception")


def test_invalid_json_and_fence_text():
    for idx, sample in enumerate(("not json", "```json\n{\"a\":1}\n```"), start=1):
        try:
            parse_strict_json_object(sample, provider="gemini")
        except ProviderResponseFormatError as exc:
            assert exc.error_kind == "invalid_response"
            continue
        _fail(f"invalid/fence parser {idx}", "no exception")
    _ok("invalid json and fence text")


def test_valid_verkauf():
    payload = {
        "ticket_name": "bounty-ticket-folgt-22031",
        "kaeufer": "tim#1234",
        "zahlungsziel": "Instant",
        "waren": [
            {
                "produkt_name": "AirPods Pro",
                "ean": "0195949052565",
                "menge": 2,
                "vk_brutto": "249.99",
                "marge_gesamt": "40.00",
            }
        ],
    }
    out = validate_and_normalize_output("verkauf", payload).to_dict()
    assert out["ticket_name"] == "bounty-ticket-folgt-22031"
    assert out["waren"][0]["menge"] == "2"
    _ok("valid verkauf")


def test_schema_alias_wrapper():
    assert get_scan_output_schema("einkauf") == get_output_contract_schema("einkauf")
    _ok("schema alias wrapper")


def test_gemini_capabilities_and_request_model():
    provider = GeminiStructuredProvider(model_name="gemini-2.5-flash")
    caps = provider.get_capabilities()
    assert caps.supports_response_mime_type is True
    assert caps.supports_system_instruction is True
    req = ProviderRequest(api_key="x", scan_mode="einkauf", response_schema=get_scan_output_schema("einkauf"))
    assert req.response_schema is not None
    _ok("gemini capabilities and request model")


def main():
    test_valid_einkauf()
    test_missing_required_field()
    test_unexpected_field_is_captured()
    test_empty_response_parser()
    test_invalid_json_and_fence_text()
    test_valid_verkauf()
    test_schema_alias_wrapper()
    test_gemini_capabilities_and_request_model()
    print("All tests passed.")


if __name__ == "__main__":
    main()
