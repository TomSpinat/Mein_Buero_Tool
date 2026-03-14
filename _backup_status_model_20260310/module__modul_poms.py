import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QTableWidget, QTableWidgetItem, QHeaderView,
    QPushButton, QFrame, QComboBox, QCheckBox, 
    QApplication, QSizePolicy, QScrollArea
)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QColor, QFont, QIcon

from module.crash_logger import log_exception
class StatsCard(QFrame):
    def __init__(self, title, count_text, color_css, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"""
            QFrame {{
                background-color: {color_css};
                border-radius: 10px;
                color: white;
            }}
        """)
        self.setMinimumHeight(100)
        
        layout = QVBoxLayout(self)
        
        title_label = QLabel(title)
        title_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        title_label.setStyleSheet("color: rgba(255, 255, 255, 0.9);")
        
        self.count_label = QLabel(count_text)
        self.count_label.setFont(QFont("Arial", 24, QFont.Weight.Bold))
        self.count_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom)
        
        layout.addWidget(title_label)
        layout.addStretch()
        layout.addWidget(self.count_label)

    def update_value(self, new_value):
        self.count_label.setText(str(new_value))


class PomsModule(QWidget):
    def __init__(self, db_manager, settings, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.settings = settings
        self.current_filter = ""
        self.show_all = False
        
        self._init_ui()
        self.refresh_data()

    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        # --- Header ---
        header_layout = QHBoxLayout()
        title_label = QLabel("POMS Reborn")
        title_label.setFont(QFont("Arial", 24, QFont.Weight.Black))
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        
        # Action Buttons
        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedSize(100, 35)
        btn_refresh.setStyleSheet("""
            QPushButton {
                background-color: #f3f4f6;
                border: 1px solid #d1d5db;
                border-radius: 17px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #e5e7eb; }
        """)
        btn_refresh.clicked.connect(self.refresh_data)
        header_layout.addWidget(btn_refresh)
        main_layout.addLayout(header_layout)

        # --- Stats Container ---
        self.stats_layout = QHBoxLayout()
        self.stats_cards = {}
        
        # Colors mimicking the Tailwind gradients
        configs = [
            ("open_orders", "Open Orders", "#3b82f6"),
            ("sent_orders", "Sent Orders", "#6366f1"),
            ("out_for_delivery", "Out for Delivery", "#a855f7"),
            ("delivered_not_invoiced", "Delivered w/o Inv.", "#ef4444"),
            ("revenue_current", "Turnover (Mo)", "#10b981"),
            ("profit_current", "Profit (Mo)", "#14b8a6")
        ]
        
        for key, title, color in configs:
            card = StatsCard(title, "0", color)
            self.stats_cards[key] = card
            self.stats_layout.addWidget(card)
            
        main_layout.addLayout(self.stats_layout)

        # --- Toolbar ---
        toolbar_layout = QHBoxLayout()
        
        self.chk_show_all = QCheckBox("Show All (inkl. abgerechnete)")
        self.chk_show_all.stateChanged.connect(self.on_show_all_changed)
        toolbar_layout.addWidget(self.chk_show_all)
        toolbar_layout.addStretch()
        
        main_layout.addLayout(toolbar_layout)

        # --- Table ---
        self.table = QTableWidget()
        self.table.setColumnCount(12)
        self.table.setHorizontalHeaderLabels([
            "ID", "Date", "Shop", "Order #", "Item", "Tracking", "EK", "VK", "Win", 
            "Order Status", "Payment", "Invoice"
        ])
        
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                background-color: white;
                alternate-background-color: #f9fafb;
            }
            QHeaderView::section {
                background-color: #f3f4f6;
                padding: 4px;
                border: none;
                font-weight: bold;
            }
        """)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        
        main_layout.addWidget(self.table)

    def on_show_all_changed(self, state):
        self.show_all = (state == 2)
        self.refresh_data()

    def refresh_data(self):
        # 1. Update stats
        stats = self.db_manager.get_poms_stats()
        
        for k, v in stats.items():
            if k in self.stats_cards:
                if 'revenue' in k or 'profit' in k:
                    text = f"{v:.2f} â‚¬"
                else:
                    text = str(v)
                self.stats_cards[k].update_value(text)
                
        # 2. Update Table
        orders = self.db_manager.get_poms_orders(show_all=self.show_all, filter_type=self.current_filter)
        self.table.setRowCount(0)
        
        # State Mappings for logic -> index mapping
        order_states = ["WAITING_FOR_ORDER", "IN_STOCK", "DELIVERED", "CANCELLED"]
        payment_states = ["Offen", "Bezahlt", "Erstattet"]
        invoice_states = ["Keine Rechnung", "Rechnung vorhanden", "Gebucht"]

        for row_idx, o in enumerate(orders):
            self.table.insertRow(row_idx)
            
            # Helper to create uneditable items
            def make_item(text):
                item = QTableWidgetItem(str(text))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                return item

            # ID
            self.table.setItem(row_idx, 0, make_item(o['id']))
            # Date
            date_str = o['orderdate'].strftime('%Y-%m-%d') if o['orderdate'] else ""
            self.table.setItem(row_idx, 1, make_item(date_str))
            # Shop
            self.table.setItem(row_idx, 2, make_item(o['shop']))
            # Order #
            self.table.setItem(row_idx, 3, make_item(o['ordernumber']))
            # Item
            self.table.setItem(row_idx, 4, make_item(f"{o['menge']}x {o['item']}"))
            # Tracking
            self.table.setItem(row_idx, 5, make_item(o['tracking'] or ""))
            
            # Financials
            ek = o.get('ek') or 0.0
            vk = o.get('vk') or 0.0
            win = o.get('win') or 0.0
            
            self.table.setItem(row_idx, 6, make_item(f"{ek:.2f}"))
            self.table.setItem(row_idx, 7, make_item(f"{vk:.2f}"))
            
            win_item = make_item(f"{win:.2f}")
            if win > 0: win_item.setForeground(QColor("green"))
            elif win < 0: win_item.setForeground(QColor("red"))
            self.table.setItem(row_idx, 8, win_item)

            
            # Order Status Dropdown
            cb_order = QComboBox()
            cb_order.addItems(["Ordered (Waiting)", "Sent (In Stock)", "Delivered", "Canceled"])
            # Mapping our internal states to combo index
            try: cb_order.setCurrentIndex(order_states.index(o['orderstate']))
            except ValueError: cb_order.setCurrentIndex(0)
            cb_order.currentIndexChanged.connect(lambda idx, idx_row=o['id']: self.update_status(idx_row, 'orderstate', idx + 1))
            self.table.setCellWidget(row_idx, 9, cb_order)
            
            # Payment Status Dropdown
            cb_payment = QComboBox()
            cb_payment.addItems(payment_states)
            try: cb_payment.setCurrentIndex(payment_states.index(o['paymentstate']))
            except ValueError: cb_payment.setCurrentIndex(0)
            cb_payment.currentIndexChanged.connect(lambda idx, idx_row=o['id']: self.update_status(idx_row, 'paymentstate', idx + 1))
            self.table.setCellWidget(row_idx, 10, cb_payment)
            
            # Invoice Status Dropdown
            cb_invoice = QComboBox()
            cb_invoice.addItems(invoice_states)
            try: cb_invoice.setCurrentIndex(invoice_states.index(o['invoicestate']))
            except ValueError: cb_invoice.setCurrentIndex(0)
            cb_invoice.currentIndexChanged.connect(lambda idx, idx_row=o['id']: self.update_status(idx_row, 'invoicestate', idx + 1))
            self.table.setCellWidget(row_idx, 11, cb_invoice)

        self.table.resizeColumnsToContents()

    def update_status(self, id, field, value_id):
        # Update db and silently accept to avoid UI stutter
        # We process update_poms_status_bulk with a single ID
        self.db_manager.update_poms_status_bulk([id], field, value_id)
        # Optional: refresh stats only
        stats = self.db_manager.get_poms_stats()
        for k, v in stats.items():
            if k in self.stats_cards:
                if 'revenue' in k or 'profit' in k:
                    text = f"{v:.2f} â‚¬"
                else:
                    text = str(v)
                self.stats_cards[k].update_value(text)
