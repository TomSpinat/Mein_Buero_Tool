import os
import ssl
import urllib.request

from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QPixmap, QIcon
from PyQt6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QRadioButton,
    QVBoxLayout,
    QWidget,
    QScrollArea
)

from module.crash_logger import log_exception


DIALOG_WIDTH = 1000
DIALOG_HEIGHT = 700


def _compact_text(value, limit=140):
    text = " ".join(str(value or "").split())
    if len(text) <= int(limit):
        return text
    return text[: max(0, int(limit) - 3)].rstrip() + "..."


class MediaGridSelectionDialog(QDialog):
    """
    Zeigt die Suchergebnisse in einem 2x3 Grid (oder aehnliches Raster) zur manuellen Auswahl an.
    """
    def __init__(self, context_title, candidates, search_type="Bild", parent=None):
        super().__init__(parent)
        self.context_title = str(context_title or '').strip()
        self.candidates = list(candidates or [])[:6] # Zeige max 6 im Grid an
        self.search_type = search_type
        self.selected_candidate = None
        self.radio_group = QButtonGroup(self)
        self.radio_group.setExclusive(True)

        self.setWindowTitle(f'{self.search_type} aus Web-Suche waehlen')
        self.setFixedSize(DIALOG_WIDTH, DIALOG_HEIGHT)
        self.setStyleSheet('background-color: #1a1b26; color: #DADADA;')

        self._build_ui()
        self._populate_grid()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)

        lbl = QLabel(
            f"{self.search_type} fuer: <b>{self.context_title or '-'}</b><br>"
            "Waehle genau einen Treffer aus und klicke auf 'Uebernehmen'."
        )
        lbl.setWordWrap(True)
        lbl.setStyleSheet('font-size: 14px; color: #a9b1d6;')
        main_layout.addWidget(lbl)

        self.lbl_empty = QLabel('')
        self.lbl_empty.setWordWrap(True)
        self.lbl_empty.setStyleSheet('font-size: 13px; color: #c0caf5;')
        main_layout.addWidget(self.lbl_empty)

        # Scroll Area for the Grid (in case smaller screens or more items later)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("QScrollArea { border: 1px solid #414868; border-radius: 6px; background-color: #24283b; }")
        
        self.grid_container = QWidget()
        self.grid_container.setStyleSheet("background-color: transparent;")
        self.grid_layout = QGridLayout(self.grid_container)
        self.grid_layout.setSpacing(15)
        self.grid_layout.setContentsMargins(15, 15, 15, 15)
        
        scroll_area.setWidget(self.grid_container)
        main_layout.addWidget(scroll_area, 1)

        button_row = QHBoxLayout()
        button_row.addStretch()

        btn_cancel = QPushButton('Abbrechen')
        btn_cancel.clicked.connect(self.reject)

        self.btn_apply = QPushButton('Uebernehmen')
        self.btn_apply.setProperty('class', 'retro-btn-action')
        self.btn_apply.clicked.connect(self._accept_selected)

        button_row.addWidget(btn_cancel)
        button_row.addWidget(self.btn_apply)
        main_layout.addLayout(button_row)
        
        # Styles für Buttons
        self.setStyleSheet(self.styleSheet() + """
            QPushButton {
                background-color: #414868;
                color: #c0caf5;
                padding: 8px 16px;
                border: none;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #565f89;
            }
            QPushButton[class="retro-btn-action"] {
                background-color: #7aa2f7;
                color: #1a1b26;
            }
            QPushButton[class="retro-btn-action"]:hover {
                background-color: #8db0f8;
            }
            QPushButton:disabled {
                background-color: #2e3c64;
                color: #565f89;
            }
        """)

    def _load_thumb(self, image_url):
        url = str(image_url or '').strip()
        if not url:
            return None
        try:
            if os.path.exists(url):
                pixmap = QPixmap(url)
                return pixmap if not pixmap.isNull() else None
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8, context=ctx) as response:
                data = response.read()
            pixmap = QPixmap()
            if pixmap.loadFromData(data) and not pixmap.isNull():
                return pixmap
        except Exception as exc:
            log_exception(__name__, exc, extra={'image_url': url})
        return None

    def _create_candidate_widget(self, candidate, idx):
        candidate = dict(candidate or {})
        
        # Container per Item
        item_widget = QWidget()
        item_widget.setStyleSheet("""
            QWidget {
                background-color: #1f2335;
                border: 2px solid #292e42;
                border-radius: 8px;
            }
            QWidget:hover {
                border-color: #414868;
            }
        """)
        item_layout = QVBoxLayout(item_widget)
        item_layout.setContentsMargins(10, 10, 10, 10)
        item_layout.setSpacing(8)

        # 1. Row: Radio + Title Source
        top_row = QHBoxLayout()
        radio = QRadioButton()
        radio.setStyleSheet("background: transparent; border: none;")
        radio.clicked.connect(lambda _checked, r=idx: self._select_candidate(r))
        self.radio_group.addButton(radio, idx)
        if idx == 0:
            radio.setChecked(True)
            self._select_candidate(idx)
            
        top_row.addWidget(radio)
        
        source_domain = _compact_text(str(candidate.get('source_domain', '') or '').strip(), 25)
        lbl_source = QLabel(source_domain or 'Unbekannte Quelle')
        lbl_source.setStyleSheet("color: #7aa2f7; font-weight: bold; background: transparent; border: none; font-size: 12px;")
        lbl_source.setToolTip(str(candidate.get('source_page_url', '') or '').strip())
        top_row.addWidget(lbl_source, 1)
        top_row.addStretch()
        item_layout.addLayout(top_row)

        # 2. Row: Preview Image
        preview_container = QWidget()
        preview_container.setStyleSheet("background-color: #16161e; border-radius: 4px;")
        preview_container.setFixedSize(280, 180)
        preview_layout = QVBoxLayout(preview_container)
        preview_layout.setContentsMargins(4, 4, 4, 4)
        
        lbl_preview = QLabel("Lade Bild...")
        lbl_preview.setStyleSheet("color: #565f89; background: transparent; border: none;")
        lbl_preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        preview_layout.addWidget(lbl_preview)
        
        # Lade im Main-Thread (vereinfacht fuer diesen Dialog)
        pixmap = self._load_thumb(candidate.get('thumbnail_url') or candidate.get('image_url'))
        if pixmap is not None:
            scaled_pixmap = pixmap.scaled(270, 170, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            lbl_preview.setPixmap(scaled_pixmap)
            # Dimensions Tooltip 
            width = candidate.get('width', 0)
            height = candidate.get('height', 0)
            if width and height:
                lbl_preview.setToolTip(f"Original: {width} x {height} px")
        else:
            lbl_preview.setText("Bild nicht verfügbar")
            
        item_layout.addWidget(preview_container, 0, Qt.AlignmentFlag.AlignCenter)

        # 3. Row: Title/Snippet
        title = str(candidate.get('title', '') or '').strip()
        snippet = str(candidate.get('snippet', '') or '').strip()
        
        lbl_title = QLabel(_compact_text(title, 70))
        lbl_title.setWordWrap(True)
        lbl_title.setStyleSheet("color: #c0caf5; font-size: 12px; font-weight: bold; background: transparent; border: none;")
        lbl_title.setFixedHeight(34) # Zwinge feste Höhe für einheitliches Layout
        item_layout.addWidget(lbl_title)
        
        lbl_snippet = QLabel(_compact_text(snippet, 90))
        lbl_snippet.setWordWrap(True)
        lbl_snippet.setStyleSheet("color: #a9b1d6; font-size: 11px; background: transparent; border: none;")
        lbl_snippet.setFixedHeight(34)
        item_layout.addWidget(lbl_snippet)
        
        # Clickable Widget Background
        item_widget.mousePressEvent = lambda event, r=idx: self._select_radio_by_click(r)
        
        return item_widget

    def _populate_grid(self):
        if not self.candidates:
            self.lbl_empty.setText(f'Es wurden keine passenden {self.search_type}-Treffer gefunden.')
            self.btn_apply.setEnabled(False)
            return

        self.lbl_empty.setText(f'Bis zu {len(self.candidates)} passende Treffer wurden gefunden.')
        
        cols = 3
        for idx, candidate in enumerate(self.candidates):
            row = idx // cols
            col = idx % cols
            widget = self._create_candidate_widget(candidate, idx)
            self.grid_layout.addWidget(widget, row, col)

    def _select_radio_by_click(self, row):
        button = self.radio_group.button(row)
        if button is not None:
            button.setChecked(True)
            self._select_candidate(row)

    def _select_candidate(self, idx):
         # Highlight logic could be added here
         pass

    def _accept_selected(self):
        row = self.radio_group.checkedId()
        if row < 0 or row >= len(self.candidates):
            return
        self.selected_candidate = dict(self.candidates[row] or {})
        self.accept()

    @staticmethod
    def choose(context_title, candidates, search_type="Bild", parent=None):
        dialog = MediaGridSelectionDialog(context_title, candidates, search_type=search_type, parent=parent)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            return dialog.selected_candidate
        return None
