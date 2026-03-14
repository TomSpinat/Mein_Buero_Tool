"""Kompatibilitaets-Wrapper fuer den neutralen Output-Vertrag."""

from module.scan_output_contract import (
    StructuredOutputValidationError,
    ValidatedEinkaufOutput,
    ValidatedOutput,
    ValidatedVerkaufOutput,
    get_scan_output_schema,
    get_scan_output_schema_json,
    validate_and_normalize_output,
)


get_output_contract_schema = get_scan_output_schema
get_output_contract_schema_json = get_scan_output_schema_json
