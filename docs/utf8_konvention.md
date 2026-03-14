# UTF-8-Konvention

Dieses Projekt fuehrt Textdateien nach Moeglichkeit in UTF-8.

## Praktische Regeln
- Python-, JSON-, Markdown-, TXT-, UI- und QSS-Dateien in UTF-8 speichern.
- Beim Lesen von JSON- oder Textdateien nach Moeglichkeit ein Encoding explizit angeben.
- Fuer bestehende Dateien mit moeglichem BOM ist `utf-8-sig` beim Lesen erlaubt.
- Beim Schreiben von JSON `ensure_ascii=False` verwenden, damit Umlaute und Sonderzeichen sauber erhalten bleiben.
- Typische Mojibake-Muster wie `Ã`, `Â`, `â` oder `ðŸ` nicht ignorieren, sondern pruefen.

## Schnellcheck
Zum Pruefen auf typische kaputte Zeichenfolgen:

```powershell
.\dev_tools\check_mojibake.ps1
```