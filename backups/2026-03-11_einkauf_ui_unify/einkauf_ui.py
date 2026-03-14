import json

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QFormLayout,
    QFrame,
    QHeaderView,
    QLabel,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from module.scan_output_contract import EINKAUF_FIELDS

EINKAUF_FIELD_SECTIONS = (
    (
        "Bestellung",
        (
            ("bestellnummer", "Bestellnummer"),
            ("kaufdatum", "Kaufdatum"),
            ("shop_name", "Shop-Name (normiert)"),
            ("bestell_email", "Bestell-E-Mail"),
        ),
    ),
    (
        "Lieferung",
        (
            ("tracking_nummer_einkauf", "Tracking Code"),
            ("sendungsstatus", "Sendungsstatus"),
            ("lieferdatum", "Lieferdatum"),
        ),
    ),
    (
        "Kosten und Zahlung",
        (
            ("gesamt_ekp_brutto", "Gesamtpreis (brutto)"),
            ("versandkosten_brutto", "Versandkosten (brutto)"),
            ("nebenkosten_brutto", "Nebenkosten (brutto)"),
            ("rabatt_brutto", "Rabatt/Gutschrift (brutto)"),
            ("ust_satz", "USt.-Satz"),
            ("zahlungsart", "Zahlungsart (normiert)"),
        ),
    ),
)
EINKAUF_VISIBLE_FIELD_KEYS = tuple(key for _section, fields in EINKAUF_FIELD_SECTIONS for key, _label in fields)
EINKAUF_ITEM_COLUMNS = (
    ("produkt_name", "Produkt"),
    ("varianten_info", "Variante"),
    ("ean", "EAN"),
    ("menge", "Menge"),
    ("ekp_brutto", "Stueckpreis"),
)
EINKAUF_ITEM_CORE_KEYS = tuple(key for key, _label in EINKAUF_ITEM_COLUMNS)
EINKAUF_VISIBLE_FIELD_SET = set(EINKAUF_VISIBLE_FIELD_KEYS)
EINKAUF_TOP_LEVEL_ALLOWED = set(EINKAUF_FIELDS)


def _format_extra_value(value):
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def collect_einkauf_extra_fields(payload):
    payload = payload if isinstance(payload, dict) else {}
    rows = []

    for key, value in payload.items():
        if str(key).startswith("_") or key in EINKAUF_VISIBLE_FIELD_SET or key == "waren":
            continue
        if value in ("", None, [], {}):
            continue
        rows.append((str(key), _format_extra_value(value)))

    waren = payload.get("waren", [])
    if isinstance(waren, list):
        for row_index, item in enumerate(waren, start=1):
            if not isinstance(item, dict):
                continue
            for key, value in item.items():
                if str(key).startswith("_") or key in EINKAUF_ITEM_CORE_KEYS:
                    continue
                if value in ("", None, [], {}):
                    continue
                rows.append((f"Artikel {row_index} - {key}", _format_extra_value(value)))

    rows.sort(key=lambda pair: pair[0].lower())
    return rows


class EinkaufHeadFormWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.inputs = {}
        self._extra_rows = []
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        for section_title, fields in EINKAUF_FIELD_SECTIONS:
            frame = QFrame()
            frame.setStyleSheet("QFrame { background-color: #24283b; border: 1px solid #414868; border-radius: 6px; }")
            frame_layout = QVBoxLayout(frame)
            frame_layout.setContentsMargins(12, 12, 12, 12)
            frame_layout.setSpacing(8)

            title = QLabel(section_title)
            title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
            frame_layout.addWidget(title)

            form_layout = QFormLayout()
            form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
            form_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
            form_layout.setVerticalSpacing(8)

            for key, label_text in fields:
                line_edit = QLineEdit()
                line_edit.setStyleSheet("QLineEdit { background-color: #171824; border: 1px solid #414868; border-radius: 4px; padding: 6px; }")
                self.inputs[key] = line_edit
                form_layout.addRow(QLabel(label_text + ":"), line_edit)

            frame_layout.addLayout(form_layout)
            layout.addWidget(frame)

        self.extra_frame = QFrame()
        self.extra_frame.setStyleSheet("QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }")
        extra_layout = QVBoxLayout(self.extra_frame)
        extra_layout.setContentsMargins(12, 12, 12, 12)
        extra_layout.setSpacing(8)

        self.extra_title = QLabel("Zusatzwerte aus dem Payload")
        self.extra_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
        extra_layout.addWidget(self.extra_title)

        self.extra_hint = QLabel("Hier erscheinen erkannte Werte, die noch keinen festen Platz in der Hauptmaske haben.")
        self.extra_hint.setWordWrap(True)
        self.extra_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        extra_layout.addWidget(self.extra_hint)

        self.extra_form_layout = QFormLayout()
        self.extra_form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.extra_form_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        self.extra_form_layout.setVerticalSpacing(6)
        extra_layout.addLayout(self.extra_form_layout)
        self.extra_frame.setVisible(False)
        layout.addWidget(self.extra_frame)
        layout.addStretch()

    def _clear_extra_rows(self):
        while self.extra_form_layout.count():
            item = self.extra_form_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._extra_rows = []

    def set_payload(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            self.inputs[key].setText(str(payload.get(key, "") or ""))
        self.set_extra_values(payload)

    def set_extra_values(self, payload):
        self._clear_extra_rows()
        extra_rows = collect_einkauf_extra_fields(payload)
        if not extra_rows:
            self.extra_frame.setVisible(False)
            return

        for label_text, value_text in extra_rows:
            line_edit = QLineEdit()
            line_edit.setReadOnly(True)
            line_edit.setText(value_text)
            line_edit.setStyleSheet("QLineEdit { background-color: #171824; border: 1px solid #414868; border-radius: 4px; padding: 6px; color: #a9b1d6; }")
            self.extra_form_layout.addRow(QLabel(label_text + ":"), line_edit)
            self._extra_rows.append((label_text, line_edit))
        self.extra_frame.setVisible(True)

    def apply_to_payload(self, payload):
        updated = dict(payload or {})
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            updated[key] = self.inputs[key].text().strip()
        return updated

    def clear_values(self):
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            self.inputs[key].clear()
        self._clear_extra_rows()
        self.extra_frame.setVisible(False)


class EinkaufItemsTableWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._row_source_items = []
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.table = QTableWidget()
        self.table.setColumnCount(len(EINKAUF_ITEM_COLUMNS))
        self.table.setHorizontalHeaderLabels([label for _key, label in EINKAUF_ITEM_COLUMNS])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setDefaultSectionSize(40)
        self.table.setStyleSheet(
            "QTableWidget { background-color: #171824; border: 1px solid #414868; border-radius: 6px; gridline-color: #414868; }"
            "QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }"
        )
        layout.addWidget(self.table)

    def _cell_text(self, row, col):
        item = self.table.item(row, col)
        if item is None:
            return ""
        return str(item.text() or "").strip()

    def clear_items(self):
        self.table.setRowCount(0)
        self._row_source_items = []

    def set_items(self, items, ean_fill_callback=None):
        rows = items if isinstance(items, list) else []
        self.table.setRowCount(len(rows))
        self._row_source_items = []

        for row_index, raw_item in enumerate(rows):
            item = dict(raw_item or {})
            produkt_name = str(item.get("produkt_name", "") or "").strip()
            varianten_info = str(item.get("varianten_info", "") or "").strip()
            ean = str(item.get("ean", "") or "").strip()
            if not ean and callable(ean_fill_callback) and produkt_name:
                ean = str(ean_fill_callback(produkt_name, varianten_info) or "").strip()
                if ean:
                    item["ean"] = ean

            self._row_source_items.append(dict(item))
            values = {
                "produkt_name": produkt_name,
                "varianten_info": varianten_info,
                "ean": ean,
                "menge": str(item.get("menge", "1") or "1"),
                "ekp_brutto": str(item.get("ekp_brutto", "") or ""),
            }
            for col_index, (key, _label) in enumerate(EINKAUF_ITEM_COLUMNS):
                self.table.setItem(row_index, col_index, QTableWidgetItem(values.get(key, "")))

        self.table.resizeColumnsToContents()

    def get_items(self):
        rows = []
        for row_index in range(self.table.rowCount()):
            row_data = {
                "produkt_name": self._cell_text(row_index, 0),
                "varianten_info": self._cell_text(row_index, 1),
                "ean": self._cell_text(row_index, 2),
                "menge": self._cell_text(row_index, 3) or "1",
                "ekp_brutto": self._cell_text(row_index, 4),
            }
            if not any(row_data.values()):
                continue

            base = {}
            if row_index < len(self._row_source_items):
                base = dict(self._row_source_items[row_index] or {})
            base.update(row_data)
            rows.append(base)
        return rows

    def get_selected_context(self):
        row = self.table.currentRow()
        if row < 0:
            return None
        return {
            "row": row,
            "produkt_name": self._cell_text(row, 0),
            "varianten_info": self._cell_text(row, 1),
            "ean_col": 2,
        }

    def set_ean_for_row(self, row, ean):
        if row < 0 or row >= self.table.rowCount():
            return
        self.table.setItem(row, 2, QTableWidgetItem(str(ean or "").strip()))
        while len(self._row_source_items) <= row:
            self._row_source_items.append({})
        self._row_source_items[row]["ean"] = str(ean or "").strip()
