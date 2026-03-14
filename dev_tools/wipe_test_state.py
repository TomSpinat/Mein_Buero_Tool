"""
wipe_test_state.py
Manuelles Test-Hilfsskript:
- Datenbank leeren
- Mapping zuruecksetzen
- optional Auto-Wipe beim App-Start aktivieren/deaktivieren

Beispiele:
  python dev_tools/wipe_test_state.py
  python dev_tools/wipe_test_state.py --enable-on-start
  python dev_tools/wipe_test_state.py --disable-on-start --no-wipe-now
"""

import argparse
import sys
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from config import SettingsManager
from module.database_manager import DatabaseManager
from module.test_reset import wipe_database_and_mapping


def main():
    parser = argparse.ArgumentParser(description="Testzustand zuruecksetzen")
    parser.add_argument("--enable-on-start", action="store_true", help="Auto-Wipe beim App-Start aktivieren")
    parser.add_argument("--disable-on-start", action="store_true", help="Auto-Wipe beim App-Start deaktivieren")
    parser.add_argument("--no-wipe-now", action="store_true", help="Jetzt nicht leeren, nur Start-Schalter aendern")
    args = parser.parse_args()

    settings = SettingsManager()

    if args.enable_on_start:
        settings.save_setting("test_wipe_on_start", True)
        print("[OK] test_wipe_on_start = True")

    if args.disable_on_start:
        settings.save_setting("test_wipe_on_start", False)
        print("[OK] test_wipe_on_start = False")

    if args.no_wipe_now:
        print("[INFO] Sofort-Wipe uebersprungen.")
        return 0

    db = DatabaseManager(settings)
    db.init_database()
    summary = wipe_database_and_mapping(settings, db_manager=db)

    print("[OK] Test-Reset abgeschlossen")
    print("  Tabellen geleert:", ", ".join(summary.get("wiped_tables", [])))
    print("  Mapping reset:", summary.get("mapping_path", ""))
    print("  Zeit:", summary.get("timestamp", ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

