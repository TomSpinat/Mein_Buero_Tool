"""
ean_lookup_dialog.py
Dialog zur Auswahl einer EAN aus Kandidaten (lokal + API).
"""

import os
import ssl
import urllib.request

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QRadioButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from module.crash_logger import log_exception


class EanLookupDialog(QDialog):
    def __init__(self, produkt_name, candidates, parent=None):
        super().__init__(parent)
        self.produkt_name = str(produkt_name or "").strip()
        self.candidates = candidates or []
        self.selected_candidate = None
        self.radio_group = QButtonGroup(self)
        self.radio_group.setExclusive(True)

        self.setWindowTitle("EAN aus Vorschlaegen waehlen")
        self.resize(980, 560)
        self.setMinimumSize(780, 420)
        self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

        self._build_ui()
        self._populate_table()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        lbl = QLabel(
            f"Produkt: <b>{self.produkt_name or '-'}</b><br>"
            "Waehle einen Treffer (Radio-Button) aus und klicke auf 'Uebernehmen'."
        )
        lbl.setWordWrap(True)
        lbl.setStyleSheet("font-size: 13px; color: #a9b1d6;")
        layout.addWidget(lbl)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["Auswahl", "Bild", "Produkt", "Variante", "EAN", "Quelle", "Sicherheit"])
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setDefaultSectionSize(86)
        self.table.setStyleSheet(
            """
            QTableWidget { background-color: #24283b; border: 1px solid #414868; border-radius: 6px; gridline-color: #414868; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
            """
        )
        self.table.doubleClicked.connect(lambda _idx: self._accept_selected())
        self.table.cellClicked.connect(self._on_cell_clicked)
        layout.addWidget(self.table, 1)

        button_row = QHBoxLayout()
        button_row.addStretch()

        btn_cancel = QPushButton("Abbrechen")
        btn_cancel.clicked.connect(self.reject)

        btn_apply = QPushButton("Uebernehmen")
        btn_apply.setProperty("class", "retro-btn-action")
        btn_apply.clicked.connect(self._accept_selected)

        button_row.addWidget(btn_cancel)
        button_row.addWidget(btn_apply)
        layout.addLayout(button_row)

    def _load_thumb(self, image_url):
        url = str(image_url or "").strip()
        if not url:
            return None

        try:
            if os.path.exists(url):
                px = QPixmap(url)
                if not px.isNull():
                    return px
                return None

            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6, context=ctx) as resp:
                data = resp.read()
            px = QPixmap()
            if px.loadFromData(data) and not px.isNull():
                return px
            return None
        except Exception as e:
            log_exception(__name__, e)
            return None

    def _populate_table(self):
        self.table.setRowCount(len(self.candidates))

        for row, item in enumerate(self.candidates):
            if not isinstance(item, dict):
                item = {}

            radio = QRadioButton()
            radio.clicked.connect(lambda _checked, r=row: self.table.selectRow(r))
            if row == 0:
                radio.setChecked(True)
            self.radio_group.addButton(radio, row)
            self.table.setCellWidget(row, 0, radio)

            thumb_label = QLabel("-")
            thumb_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            px = self._load_thumb(item.get("bild_url", ""))
            if px is not None:
                thumb_label.setPixmap(px.scaled(64, 64, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
            self.table.setCellWidget(row, 1, thumb_label)

            self.table.setItem(row, 2, QTableWidgetItem(str(item.get("produkt_name", "")).strip()))
            self.table.setItem(row, 3, QTableWidgetItem(str(item.get("varianten_info", "")).strip()))
            self.table.setItem(row, 4, QTableWidgetItem(str(item.get("ean", "")).strip()))
            self.table.setItem(row, 5, QTableWidgetItem(str(item.get("quelle", "lokal")).strip()))

            conf = item.get("confidence", "")
            try:
                conf_txt = f"{float(conf):.2f}"
            except Exception:
                conf_txt = str(conf or "")
            self.table.setItem(row, 6, QTableWidgetItem(conf_txt))

        self.table.resizeColumnsToContents()
        if self.table.rowCount() > 0:
            self.table.selectRow(0)

    def _on_cell_clicked(self, row, _col):
        btn = self.radio_group.button(row)
        if btn is not None:
            btn.setChecked(True)

    def _accept_selected(self):
        row = self.radio_group.checkedId()
        if row < 0:
            row = self.table.currentRow()
        if row < 0 or row >= len(self.candidates):
            return
        self.selected_candidate = self.candidates[row]
        self.accept()

    @staticmethod
    def choose(produkt_name, candidates, parent=None):
        dlg = EanLookupDialog(produkt_name, candidates, parent=parent)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            return dlg.selected_candidate
        return None
