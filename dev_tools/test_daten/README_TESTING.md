# Anleitung zur E2E-Workflow Simulation

Nutze diese Dateien, um den kompletten Prozess durchzuspielen, ohne echte Rechnungen hochzuladen.
Die Datenbank wurde per Skript komplett geleert.

### Schritt 1: Der Einkauf (Order Entry)
1. Öffne das "Rechnungs Scanner" Modul im Dashboard.
2. Modus: `🛒 Einkauf (Rechnung)` ist ausgewählt.
3. Nimm die Datei `01_MOCK_Rechnung.png` und ziehe sie per Drag&Drop in das graue Feld (oder öffne das Bild, mach einen Screenshot in die Zwischenablage und drücke `Strg+V` im Tool).
4. Klicke auf "Mit Gemini Scannen".
5. Klicke auf "Bestellung speichern".
-> **Check im Dashboard:** In der To-Do-Leiste rechts sollte nun automatisch "EAN fehlt!" aufploppen (Da Headset keine EAN auf dem Dioxyd-Screenshot hat).

### Schritt 2: Tracking (Mail Scraper)
*Da wir keine echte IONOS Verbindung testen wollen, nutzen wir den Button "Lokal aus EML laden"* (Sofern du dieses Feature im Modul einbaust, ansonsten reicht es, das Tracking manuell einzutippen).
-> Ziel: Die Bestellung von Dioxyd bekommt den Tracking Code "DHL-498218310".

### Schritt 3: Wareneingang
1. Das Tracking springt irgendwann (oder durch manuelle Änderung im Tracking-Radar) auf "Geliefert".
2. **Check im Dashboard:** In der To-Do-Leiste rechts sollte nun "Wareneingang prüfen" aufploppen.
3. Klicke auf das To-Do.
4. Prüfe die Bestellung manuell im UI ab.
-> **Check Finanzen:** Gehe ins Finanz-Modul. Unter "Gebundenes Kapital" sollten nun 2.736,97 € stehen.

### Schritt 4: Der Weiterverkauf (Discord)
1. Gehe in den "Rechnungs Scanner".
2. Wähle den Modus `🏷️ Verkauf (Discord-Ticket)`.
3. Wirf das Bild `02_MOCK_Discord_Ticket.png` hinein.
4. Klicke "Scannen".
5. Klicke Speichern.
-> Das Tool sollte nun automatisch 3 der Playstations aus dem Bestand nehmen und dem Bounty-Ticket zuordnen (Back-to-Back Matching).
-> **Check im Dashboard:** "Versandbereit" poppt in der To-Do Liste auf!

### Schritt 5: Ausgang (Packstation)
1. Klicke auf das "Versandbereit" To-Do.
2. Tippe als Ausgehende Tracking-Nummer z.B. `123456789` ein und drücke Enter.
3. Scanne (Tippe) die EAN `0711719395201` und drücke Enter.
4. Scanne eine fiktive Seriennummer `SN-TEST-001` und drücke Enter.
-> Der Artikel ist nun verpackt, der Flow ist abgeschlossen!

---

## Neuer E2E-Pfad: ticket-folgt + ohne Discord-Ticket

Zusätzlich zum UI-Workflow gibt es jetzt einen automatisierten Datenbank-E2E-Runner:

```powershell
cd C:\Users\timth\Desktop\Mein_Buero_Tool
python dev_tools\e2e_ticket_folgt_path.py
```

Der Runner prüft in einem Lauf:
1. Discord-Ticket wird zuerst gespeichert (`TICKET_FOLGT`, offene Einheiten vorhanden)
2. Späterer Einkauf verknüpft die offenen Einheiten automatisch (`MATCHED`)
3. Verkauf ohne Discord-Ticket bleibt möglich und ist versandbar

Achtung: Der Runner leert die Tabellen `waren_positionen`, `einkauf_bestellungen`, `verkauf_tickets`, `ausgangs_pakete` vor dem Test.