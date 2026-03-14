"""
Kleines Hilfsskript fuer die UPCitemdb-Normalisierung.
Zeigt fuer ein paar typische Problemfaelle die abgeleiteten Query-Varianten.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from module.upcitemdb_normalizer import ProductNameNormalizer


TEST_CASES = [
    "Sony PlayStation 5 DualSense Wireless Controller Weiss",
    "Sony PS5 DualSense Controller White V2",
    "Nintendo Switch Pro Controller schwarz",
    "Apple AirPods Pro 2. Gen MagSafe Ladecase USB-C",
    "Samsung 990 PRO NVMe M.2 SSD 2TB",
    "Logitech MX Master 3S kabellose Maus graphit",
]


def main():
    normalizer = ProductNameNormalizer()
    for idx, raw_name in enumerate(TEST_CASES, start=1):
        normalized = normalizer.normalize_for_upcitemdb(raw_name)
        print("=" * 80)
        print(f"Fall {idx}: {raw_name}")
        print(f"  cleaned_name     : {normalized.cleaned_name}")
        print(f"  brand            : {normalized.brand}")
        print(f"  model_code       : {normalized.model_code}")
        print(f"  product_family   : {normalized.product_family}")
        print(f"  category_hint    : {normalized.category_hint}")
        print(f"  color            : {normalized.color}")
        print(f"  size_or_capacity : {normalized.size_or_capacity}")
        print(f"  pack_size        : {normalized.pack_size}")
        print(f"  platform         : {normalized.platform}")
        print(f"  edition          : {normalized.edition}")
        print(f"  language_hint    : {normalized.language_hint}")
        print("  search_queries   :")
        for query in normalized.search_queries:
            print(f"    - {query}")


if __name__ == "__main__":
    main()
