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
from module.status_model import (
    LEGACY_INVOICE_OPTIONS,
    LEGACY_ORDER_OPTIONS,
    LEGACY_PAYMENT_OPTIONS,
    normalize_inventory_status,
    normalize_invoice_status,
    normalize_payment_status,
)


class StatsCard(QFrame):
    def __init__(self, title, count_text, color_css, parent=None):
        super().__init__(parent)
        self.setStyleSheet(
            f"""
            QFrame {{
                background-color: {color_css};
                border-radius: 10px;
                color: white;
            }}
        """
        )
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

        header_layout = QHBoxLayout()
        title_label = QLabel("POMS Reborn")
        title_label.setFont(QFont("Arial", 24, QFont.Weight.Black))

        header_layout.addWidget(title_label)
        header_layout.addStretch()

        btn_refresh = QPushButton("Refresh")
        btn_refresh.setFixedSize(100, 35)
        btn_refresh.setStyleSheet(
            """
            QPushButton {
                background-color: #f3f4f6;
                border: 1px solid #d1d5db;
                border-radius: 17px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #e5e7eb; }
        """
        )
        btn_refresh.clicked.connect(self.refresh_data)
        header_layout.addWidget(btn_refresh)
        main_layout.addLayout(header_layout)

        self.stats_layout = QHBoxLayout()
        self.stats_cards = {}

        configs = [
            ("open_orders", "Open Orders", "#3b82f6"),
            ("sent_orders", "Sent Orders", "#6366f1"),
            ("out_for_delivery", "Out for Delivery", "#a855f7"),
            ("delivered_not_invoiced", "Delivered w/o Inv.", "#ef4444"),
            ("revenue_current", "Turnover (Mo)", "#10b981"),
            ("profit_current", "Profit (Mo)", "#14b8a6"),
        ]

        for key, title, color in configs:
            card = StatsCard(title, "0", color)
            self.stats_cards[key] = card
            self.stats_layout.addWidget(card)

        main_layout.addLayout(self.stats_layout)

        toolbar_layout = QHBoxLayout()

        self.chk_show_all = QCheckBox("Show All (inkl. abgerechnete)")
        self.chk_show_all.stateChanged.connect(self.on_show_all_changed)
        toolbar_layout.addWidget(self.chk_show_all)
        toolbar_layout.addStretch()

        main_layout.addLayout(toolbar_layout)

        self.table = QTableWidget()
        self.table.setColumnCount(12)
        self.table.setHorizontalHeaderLabels(
            [
                "ID",
                "Date",
                "Shop",
                "Order #",
                "Item",
                "Tracking",
                "EK",
                "VK",
                "Win",
                "Order Status",
                "Payment",
                "Invoice",
            ]
        )

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)

        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(
            """
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
        """
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        main_layout.addWidget(self.table)

    def on_show_all_changed(self, state):
        self.show_all = state == 2
        self.refresh_data()

    def _find_option_index(self, options, target_status):
        for idx, (_, status) in enumerate(options):
            if status == target_status:
                return idx
        return 0

    def refresh_data(self):
        stats = self.db_manager.get_poms_stats()

        for key, value in stats.items():
            if key in self.stats_cards:
                if "revenue" in key or "profit" in key:
                    text = f"{value:.2f} EUR"
                else:
                    text = str(value)
                self.stats_cards[key].update_value(text)

        orders = self.db_manager.get_poms_orders(show_all=self.show_all, filter_type=self.current_filter)
        self.table.setRowCount(0)

        order_labels = [label for (label, _) in LEGACY_ORDER_OPTIONS]
        payment_labels = [label for (label, _) in LEGACY_PAYMENT_OPTIONS]
        invoice_labels = [label for (label, _) in LEGACY_INVOICE_OPTIONS]

        for row_idx, order in enumerate(orders):
            self.table.insertRow(row_idx)

            def make_item(text):
                item = QTableWidgetItem(str(text))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                return item

            self.table.setItem(row_idx, 0, make_item(order["id"]))
            date_str = order["orderdate"].strftime("%Y-%m-%d") if order["orderdate"] else ""
            self.table.setItem(row_idx, 1, make_item(date_str))
            self.table.setItem(row_idx, 2, make_item(order["shop"]))
            self.table.setItem(row_idx, 3, make_item(order["ordernumber"]))
            self.table.setItem(row_idx, 4, make_item(f"{order['menge']}x {order['item']}"))
            self.table.setItem(row_idx, 5, make_item(order.get("tracking") or ""))

            ek = order.get("ek") or 0.0
            vk = order.get("vk") or 0.0
            win = order.get("win") or 0.0

            self.table.setItem(row_idx, 6, make_item(f"{ek:.2f}"))
            self.table.setItem(row_idx, 7, make_item(f"{vk:.2f}"))

            win_item = make_item(f"{win:.2f}")
            if win > 0:
                win_item.setForeground(QColor("green"))
            elif win < 0:
                win_item.setForeground(QColor("red"))
            self.table.setItem(row_idx, 8, win_item)

            cb_order = QComboBox()
            cb_order.addItems(order_labels)
            order_state = normalize_inventory_status(order.get("orderstate"))
            cb_order.setCurrentIndex(self._find_option_index(LEGACY_ORDER_OPTIONS, order_state))
            cb_order.currentIndexChanged.connect(
                lambda idx, idx_row=order["id"]: self.update_status(
                    idx_row,
                    "orderstate",
                    LEGACY_ORDER_OPTIONS[idx][1].value,
                )
            )
            self.table.setCellWidget(row_idx, 9, cb_order)

            cb_payment = QComboBox()
            cb_payment.addItems(payment_labels)
            pay_state = normalize_payment_status(order.get("paymentstate"))
            cb_payment.setCurrentIndex(self._find_option_index(LEGACY_PAYMENT_OPTIONS, pay_state))
            cb_payment.currentIndexChanged.connect(
                lambda idx, idx_row=order["id"]: self.update_status(
                    idx_row,
                    "paymentstate",
                    LEGACY_PAYMENT_OPTIONS[idx][1].value,
                )
            )
            self.table.setCellWidget(row_idx, 10, cb_payment)

            cb_invoice = QComboBox()
            cb_invoice.addItems(invoice_labels)
            inv_state = normalize_invoice_status(order.get("invoicestate"))
            cb_invoice.setCurrentIndex(self._find_option_index(LEGACY_INVOICE_OPTIONS, inv_state))
            cb_invoice.currentIndexChanged.connect(
                lambda idx, idx_row=order["id"]: self.update_status(
                    idx_row,
                    "invoicestate",
                    LEGACY_INVOICE_OPTIONS[idx][1].value,
                )
            )
            self.table.setCellWidget(row_idx, 11, cb_invoice)

        self.table.resizeColumnsToContents()

    def update_status(self, item_id, field, value_token):
        self.db_manager.update_poms_status_bulk([item_id], field, value_token)
        stats = self.db_manager.get_poms_stats()
        for key, value in stats.items():
            if key in self.stats_cards:
                if "revenue" in key or "profit" in key:
                    text = f"{value:.2f} EUR"
                else:
                    text = str(value)
                self.stats_cards[key].update_value(text)
