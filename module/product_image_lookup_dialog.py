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


DIALOG_WIDTH = 980
DIALOG_HEIGHT = 560


def _compact_text(value, limit=140):
    text = " ".join(str(value or "").split())
    if len(text) <= int(limit):
        return text
    return text[: max(0, int(limit) - 3)].rstrip() + "..."


class ProductImageLookupDialog(QDialog):
    def __init__(self, produkt_name, candidates, parent=None):
        super().__init__(parent)
        self.produkt_name = str(produkt_name or '').strip()
        self.candidates = list(candidates or [])[:3]
        self.selected_candidate = None
        self.radio_group = QButtonGroup(self)
        self.radio_group.setExclusive(True)

        self.setWindowTitle('Produktbild aus Web-Suche waehlen')
        self.setFixedSize(DIALOG_WIDTH, DIALOG_HEIGHT)
        self.setStyleSheet('background-color: #1a1b26; color: #DADADA;')

        self._build_ui()
        self._populate_table()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        lbl = QLabel(
            f"Produkt: <b>{self.produkt_name or '-'}</b><br>"
            "Waehle genau einen Treffer aus und klicke auf 'Uebernehmen'."
        )
        lbl.setWordWrap(True)
        lbl.setStyleSheet('font-size: 13px; color: #a9b1d6;')
        layout.addWidget(lbl)

        self.lbl_empty = QLabel('')
        self.lbl_empty.setWordWrap(True)
        self.lbl_empty.setStyleSheet('font-size: 12px; color: #c0caf5;')
        layout.addWidget(self.lbl_empty)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(['Auswahl', 'Vorschau', 'Titel', 'Quelle', 'Sicherheit'])
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.verticalHeader().setDefaultSectionSize(92)
        self.table.setWordWrap(False)
        self.table.setTextElideMode(Qt.TextElideMode.ElideRight)
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

        btn_cancel = QPushButton('Abbrechen')
        btn_cancel.clicked.connect(self.reject)

        self.btn_apply = QPushButton('Uebernehmen')
        self.btn_apply.setProperty('class', 'retro-btn-action')
        self.btn_apply.clicked.connect(self._accept_selected)

        button_row.addWidget(btn_cancel)
        button_row.addWidget(self.btn_apply)
        layout.addLayout(button_row)

    def _load_thumb(self, image_url):
        url = str(image_url or '').strip()
        if not url:
            return None
        try:
            if os.path.exists(url):
                pixmap = QPixmap(url)
                return pixmap if not pixmap.isNull() else None
            ctx = ssl.create_default_context()
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8, context=ctx) as response:
                data = response.read()
            pixmap = QPixmap()
            if pixmap.loadFromData(data) and not pixmap.isNull():
                return pixmap
        except Exception as exc:
            log_exception(__name__, exc, extra={'image_url': url})
        return None

    def _populate_table(self):
        if not self.candidates:
            self.lbl_empty.setText('Es wurden keine passenden Bildtreffer gefunden.')
            self.table.setRowCount(0)
            self.btn_apply.setEnabled(False)
            return

        self.lbl_empty.setText('Bis zu 3 passende Bildtreffer wurden gefunden.')
        self.table.setRowCount(len(self.candidates))

        for row_index, candidate in enumerate(self.candidates):
            candidate = dict(candidate or {})
            radio = QRadioButton()
            radio.clicked.connect(lambda _checked, r=row_index: self.table.selectRow(r))
            if row_index == 0:
                radio.setChecked(True)
            self.radio_group.addButton(radio, row_index)
            self.table.setCellWidget(row_index, 0, radio)

            preview = QLabel('-')
            preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
            preview.setFixedSize(80, 80)
            pixmap = self._load_thumb(candidate.get('thumbnail_url') or candidate.get('image_url'))
            if pixmap is not None:
                preview.setPixmap(
                    pixmap.scaled(72, 72, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                )
            self.table.setCellWidget(row_index, 1, preview)

            title = str(candidate.get('title', '') or '').strip()
            snippet = str(candidate.get('snippet', '') or '').strip()
            display_title = _compact_text(title, 110)
            display_snippet = _compact_text(snippet, 80)
            combined_text = display_title if not display_snippet else f'{display_title} - {display_snippet}'
            title_item = QTableWidgetItem(combined_text)
            title_item.setToolTip(
                '\n'.join(
                    part for part in [title, snippet, str(candidate.get('source_page_url', '') or candidate.get('image_url', '') or '').strip()]
                    if str(part or '').strip()
                )
            )
            self.table.setItem(row_index, 2, title_item)

            source_domain = _compact_text(str(candidate.get('source_domain', '') or '').strip(), 36)
            source_item = QTableWidgetItem(source_domain or '-')
            source_item.setToolTip(str(candidate.get('source_page_url', '') or '').strip())
            self.table.setItem(row_index, 3, source_item)

            confidence = candidate.get('confidence', '')
            try:
                score_text = f"{float(confidence):.2f}"
            except Exception:
                score_text = str(confidence or '')
            score_item = QTableWidgetItem(score_text)
            ranking_score = candidate.get('ranking_score', '')
            if ranking_score not in ('', None):
                score_item.setToolTip(f'Ranking-Score: {ranking_score}')
            self.table.setItem(row_index, 4, score_item)

        self.table.setColumnWidth(0, 90)
        self.table.setColumnWidth(1, 110)
        self.table.setColumnWidth(2, 470)
        self.table.setColumnWidth(3, 170)
        self.table.setColumnWidth(4, 90)
        self.table.selectRow(0)

    def _on_cell_clicked(self, row, _col):
        button = self.radio_group.button(row)
        if button is not None:
            button.setChecked(True)

    def _accept_selected(self):
        row = self.radio_group.checkedId()
        if row < 0:
            row = self.table.currentRow()
        if row < 0 or row >= len(self.candidates):
            return
        self.selected_candidate = dict(self.candidates[row] or {})
        self.accept()

    @staticmethod
    def choose(produkt_name, candidates, parent=None):
        dialog = ProductImageLookupDialog(produkt_name, candidates, parent=parent)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            return dialog.selected_candidate
        return None