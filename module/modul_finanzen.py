"""
modul_finanzen.py
App-Modul zur Visualisierung von Finanzen und Cashflow.
"""

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QGridLayout, QPushButton
)

from module.database_manager import DatabaseManager
from module.status_model import InventoryStatus

from module.crash_logger import log_exception


# SQL-Fragment: vollstaendig stornierte Positionen ausschliessen
_STORNO_FILTER = "(w.storno_menge IS NULL OR w.storno_menge < w.menge)"


class FinanzenApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self._build_ui()
        self.refresh_data()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        lbl_title = QLabel("Finanz- & Cashflow-Uebersicht")
        lbl_title.setStyleSheet("font-size: 24px; font-weight: bold; color: #7aa2f7;")

        btn_refresh = QPushButton("Aktualisieren")
        btn_refresh.setProperty("class", "retro-btn")
        btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_refresh.clicked.connect(self.refresh_data)

        top_layout = QHBoxLayout()
        top_layout.addWidget(lbl_title)
        top_layout.addStretch()
        top_layout.addWidget(btn_refresh)
        main_layout.addLayout(top_layout)

        self.cards_layout = QGridLayout()
        self.cards_layout.setSpacing(15)

        self.lbl_lager = self._create_stat_card("Gebundenes Kapital", "0,00 EUR", "#a9b1d6", self.cards_layout, 0, 0)
        self.lbl_forderungen = self._create_stat_card("Offene Forderungen", "0,00 EUR", "#e0af68", self.cards_layout, 0, 1)
        self.lbl_rechnungen = self._create_stat_card("Rechnungen zu schreiben", "0 Ticket(s)", "#f7768e", self.cards_layout, 1, 0)
        self.lbl_gewinn, self.lbl_gewinn_netto = self._create_stat_card("Realisierter Gewinn", "0,00 EUR", "#9ece6a", self.cards_layout, 1, 1, with_sub=True)
        self.lbl_bezugskosten = self._create_stat_card("Bezugskosten (Versand/Nebenkosten)", "0,00 EUR", "#7dcfff", self.cards_layout, 2, 0)
        self.lbl_einstand = self._create_stat_card("Einstand gesamt (Einkauf)", "0,00 EUR", "#bb9af7", self.cards_layout, 2, 1)

        # Regelbesteuerung-only KPI-Karten (zweite Reihe)
        self.regelbesteuerung_frames = []
        self.lbl_vorsteuer = self._create_stat_card_regelbesteuerung("Vorsteuer (abziehbar)", "0,00 EUR", "#f7768e", self.cards_layout, 3, 0)
        self.lbl_ust_schuld = self._create_stat_card_regelbesteuerung("USt-Schuld (erhalten)", "0,00 EUR", "#e0af68", self.cards_layout, 3, 1)
        self.lbl_netto_marge = self._create_stat_card_regelbesteuerung("Netto-Marge %", "0,00 %", "#7aa2f7", self.cards_layout, 4, 0)

        main_layout.addLayout(self.cards_layout)
        main_layout.addStretch()

    def _create_stat_card(self, title, default_val, color, grid, row, col, with_sub=False):
        frame = QFrame()
        frame.setStyleSheet("QFrame { background-color: #242535; border: 1px solid #33354C; border-radius: 10px; }")

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(20, 20, 20, 20)

        title_lbl = QLabel(title)
        title_lbl.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {color}; border: none;")

        val_lbl = QLabel(default_val)
        val_lbl.setStyleSheet("font-size: 28px; font-weight: bold; color: #ffffff; border: none; margin-top: 10px;")

        layout.addWidget(title_lbl)
        layout.addWidget(val_lbl)

        if with_sub:
            sub_lbl = QLabel("")
            sub_lbl.setStyleSheet("font-size: 14px; color: #a9b1d6; border: none;")
            sub_lbl.setVisible(False)
            layout.addWidget(sub_lbl)
            grid.addWidget(frame, row, col)
            return val_lbl, sub_lbl

        grid.addWidget(frame, row, col)
        return val_lbl

    def _create_stat_card_regelbesteuerung(self, title, default_val, color, grid, row, col):
        frame = QFrame()
        frame.setStyleSheet("QFrame { background-color: #242535; border: 1px solid #33354C; border-radius: 10px; }")
        frame.setVisible(False)
        self.regelbesteuerung_frames.append(frame)

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(20, 20, 20, 20)

        title_lbl = QLabel(title)
        title_lbl.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {color}; border: none;")

        val_lbl = QLabel(default_val)
        val_lbl.setStyleSheet("font-size: 28px; font-weight: bold; color: #ffffff; border: none; margin-top: 10px;")

        layout.addWidget(title_lbl)
        layout.addWidget(val_lbl)

        grid.addWidget(frame, row, col)
        return val_lbl

    def _fmt_eur(self, value):
        return f"{float(value or 0.0):,.2f} EUR".replace(',', 'X').replace('.', ',').replace('X', '.')

    def _fmt_pct(self, value):
        return f"{float(value or 0.0):,.2f} %".replace(',', 'X').replace('.', ',').replace('X', '.')

    def refresh_data(self):
        steuer_modus = self.settings.get("steuer_modus", "kleinunternehmer")
        is_regelbesteuerung = steuer_modus == "regelbesteuerung"

        # Regelbesteuerung-Karten ein-/ausblenden
        for frame in self.regelbesteuerung_frames:
            frame.setVisible(is_regelbesteuerung)

        try:
            conn = self.db._get_connection()
            if not conn.is_connected():
                return
            cursor = conn.cursor(dictionary=True)

            cursor.execute(
                f"""
                SELECT SUM(COALESCE(einstand_brutto, ekp_brutto) * COALESCE(menge, 1)) AS total
                FROM waren_positionen w
                WHERE status = %s AND {_STORNO_FILTER}
                """,
                (InventoryStatus.IN_STOCK.value,),
            )
            res = cursor.fetchone()
            lagerwert = res['total'] if res and res['total'] else 0.0

            cursor.execute(
                f"""
                SELECT SUM(w.vk_brutto) AS forderung
                FROM waren_positionen w
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                WHERE t.geld_erhalten = FALSE AND {_STORNO_FILTER}
                """
            )
            res = cursor.fetchone()
            forderungen = res['forderung'] if res and res['forderung'] else 0.0

            cursor.execute("SELECT COUNT(id) AS counts FROM verkauf_tickets WHERE rechnung_an_abnehmer_verschickt = FALSE")
            res = cursor.fetchone()
            rechnungen = res['counts'] if res and res['counts'] else 0

            cursor.execute(
                f"""
                SELECT SUM(w.marge_gesamt) AS gewinn, SUM(w.marge_netto) AS gewinn_netto
                FROM waren_positionen w
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                WHERE t.geld_erhalten = TRUE AND {_STORNO_FILTER}
                """
            )
            res = cursor.fetchone()
            gewinn = res['gewinn'] if res and res['gewinn'] else 0.0
            gewinn_netto = res['gewinn_netto'] if res and res['gewinn_netto'] else 0.0

            cursor.execute(
                """
                SELECT SUM(COALESCE(versandkosten_brutto, 0) + COALESCE(nebenkosten_brutto, 0) - COALESCE(rabatt_brutto, 0)) AS bezugskosten
                FROM einkauf_bestellungen
                WHERE COALESCE(storno_status, 'aktiv') != 'storniert'
                """
            )
            res = cursor.fetchone()
            bezugskosten = res['bezugskosten'] if res and res['bezugskosten'] else 0.0

            cursor.execute(
                """
                SELECT SUM(COALESCE(einstand_gesamt_brutto, gesamt_ekp_brutto, 0)) AS einstand
                FROM einkauf_bestellungen
                WHERE COALESCE(storno_status, 'aktiv') != 'storniert'
                """
            )
            res = cursor.fetchone()
            einstand = res['einstand'] if res and res['einstand'] else 0.0

            self.lbl_lager.setText(self._fmt_eur(lagerwert))
            self.lbl_forderungen.setText(self._fmt_eur(forderungen))
            self.lbl_rechnungen.setText(f"{rechnungen} Ticket(s)")
            self.lbl_gewinn.setText(self._fmt_eur(gewinn))
            self.lbl_bezugskosten.setText(self._fmt_eur(bezugskosten))
            self.lbl_einstand.setText(self._fmt_eur(einstand))

            if is_regelbesteuerung:
                self.lbl_gewinn_netto.setText(f"Netto: {self._fmt_eur(gewinn_netto)}")
                self.lbl_gewinn_netto.setVisible(True)
            else:
                self.lbl_gewinn_netto.setVisible(False)

            if is_regelbesteuerung:
                cursor.execute(
                    f"""
                    SELECT SUM(COALESCE(einstand_brutto, 0) - COALESCE(einstand_netto, einstand_brutto, 0)) AS vorsteuer
                    FROM waren_positionen w
                    WHERE {_STORNO_FILTER}
                    """
                )
                res = cursor.fetchone()
                vorsteuer = res['vorsteuer'] if res and res['vorsteuer'] else 0.0

                cursor.execute(
                    f"""
                    SELECT SUM(COALESCE(w.vk_brutto, 0) - COALESCE(w.vk_netto, w.vk_brutto, 0)) AS ust_schuld
                    FROM waren_positionen w
                    JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                    WHERE t.geld_erhalten = TRUE AND {_STORNO_FILTER}
                    """
                )
                res = cursor.fetchone()
                ust_schuld = res['ust_schuld'] if res and res['ust_schuld'] else 0.0

                cursor.execute(
                    f"""
                    SELECT AVG(
                        CASE WHEN COALESCE(w.einstand_netto, 0) > 0
                             THEN (w.marge_netto / w.einstand_netto * 100)
                             ELSE NULL END
                    ) AS netto_marge_pct
                    FROM waren_positionen w
                    JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                    WHERE t.geld_erhalten = TRUE AND w.marge_netto IS NOT NULL AND {_STORNO_FILTER}
                    """
                )
                res = cursor.fetchone()
                netto_marge_pct = res['netto_marge_pct'] if res and res['netto_marge_pct'] else 0.0

                self.lbl_vorsteuer.setText(self._fmt_eur(vorsteuer))
                self.lbl_ust_schuld.setText(self._fmt_eur(ust_schuld))
                self.lbl_netto_marge.setText(self._fmt_pct(netto_marge_pct))

            cursor.close()
            conn.close()
        except Exception as exc:
            log_exception(__name__, exc)
            print(f"Fehler Finanzen: {exc}")
