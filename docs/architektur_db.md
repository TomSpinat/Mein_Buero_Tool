# DB-Architektur (einfach erklaert)

Stand: 10.03.2026

## Warum diese Datei?
Diese Uebersicht zeigt in einfachen Worten, wo welche Datenbank-Logik liegt.
So findest du schneller die richtige Stelle, wenn du spaeter etwas aendern willst.

## Kurzbild
Die Klasse `DatabaseManager` ist die **zentrale Schaltstelle**.
Sie sammelt mehrere kleine Bausteine (Mixins), damit der alte Monolith nicht mehr in einer Datei klebt.

- Vorteil: leichter wartbar
- Vorteil: Fehler leichter eingrenzbar
- Vorteil: bestehende Aufrufe bleiben kompatibel

## Welche Datei ist wofuer zustaendig?

### Zentrale Klasse
- `module/database_manager.py`
  - Verbindungsaufbau zur DB
  - Verbindung testen
  - kleine Helfer (`_to_float`, `_to_int`, ...)
  - bindet alle Bausteine zusammen

### Schema / DB-Wartung
- `module/db/schema_management_mixin.py`
  - `init_database()` -> Tabellen anlegen/erweitern
  - `wipe_all_data_for_testing()` -> Testdaten leeren

### EAN-Logik
- `module/db/ean_repository_mixin.py`
  - EAN speichern/suchen
  - Alias-Cache fuer chaotische Produktnamen

### POMS-Ansicht
- `module/db/poms_repository_mixin.py`
  - POMS-Statistiken
  - POMS-Liste/Filter
  - Massen-Statusupdate

### Bilder
- `module/db/media_repository_mixin.py`
  - Produktbild-Pfad lesen/speichern

### Bestellnummern-Helfer
- `module/db/order_lookup_mixin.py`
  - Bestellung per Nummer finden
  - neue freie Bestellnummer vorschlagen

### Ticket-Matching
- `module/db/ticket_matching_mixin.py`
  - Ticket-Preview
  - Matching von Ticket-Einheiten auf Lagerpositionen
  - offene Ticket-Reste spaeter wieder aufloesen

### Bestell-Verarbeitung
- `module/db/order_processing_mixin.py`
  - Einkauf speichern/erganzen
  - Kostenverteilung (Warenwert, Versand, Nebenkosten, Rabatt)
  - Enrichment-Vorschau fuer Nachreichungen

### Dashboard-ToDos
- `module/db/todo_mixin.py`
  - ToDo-Karten berechnen (EAN fehlt, Wareneingang offen, Versandbereit)

## Typische Wege im Alltag

### 1) Modul 1 speichert eine Bestellung
1. UI gibt normierte Daten an `DatabaseManager`.
2. Bestell-Verarbeitung schreibt Kopf + Positionen.
3. Ticket-Matching kann offene Tickets direkt aufloesen.
4. EANs werden lokal mitgelernt.

### 2) Modul 2 (Mail) verarbeitet Belege
1. Maildaten werden normalisiert.
2. Speichern laeuft ueber denselben DB-Kern.
3. Matching/Ergaenzungen folgen denselben Regeln wie in Modul 1.

### 3) POMS/ToDo lesen den Status
1. POMS nutzt eigene POMS-Bausteine.
2. ToDo-Modul nutzt ToDo-Baustein.
3. Beide greifen auf dieselben Tabellen zu.

## Begriffe kurz erklaert
- **Mixin**: ein Zusatz-Baustein mit Methoden, den man in eine Klasse "einsteckt".
- **Monolith-Datei**: eine riesige Datei mit zu vielen Aufgaben.
- **Kompatibel**: alte Aufrufe funktionieren weiter, obwohl intern umgebaut wurde.

## Wenn du etwas aendern willst
- Tabellen/Schema aendern -> `schema_management_mixin.py`
- EAN-Suche/Cache aendern -> `ean_repository_mixin.py`
- Matching-Regeln aendern -> `ticket_matching_mixin.py`
- Einkaufskosten/Enrichment aendern -> `order_processing_mixin.py`
- POMS aendern -> `poms_repository_mixin.py`
- ToDo-Logik aendern -> `todo_mixin.py`

## Wichtig
Bitte bei groesseren Umbauten immer zuerst Backup machen (Ordner `refactor_backups`).
