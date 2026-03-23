from module.money_tooltips import build_poms_row_money_tooltips, build_poms_stats_tooltips
from module.status_model import (
    POMS_INVOICE_OPTIONS,
    POMS_ORDER_OPTIONS,
    POMS_PAYMENT_OPTIONS,
)


class PomsViewService:
    """Duenne Uebersetzerschicht zwischen der sichtbaren POMS-Ansicht und unserem internen System."""

    STATUS_OPTIONS = {
        "inventory_status": POMS_ORDER_OPTIONS,
        "payment_status": POMS_PAYMENT_OPTIONS,
        "invoice_status": POMS_INVOICE_OPTIONS,
    }

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def get_stats(self):
        stats = self.db_manager.get_ops_dashboard_stats()
        stats["_money_tooltips"] = build_poms_stats_tooltips(stats)
        return stats

    def get_rows(self, search="", show_all=False, filter_type=""):
        rows = self.db_manager.get_ops_orders(
            search=search,
            show_all=show_all,
            filter_type=filter_type,
        )
        for row in rows:
            if isinstance(row, dict):
                row["_money_tooltips"] = build_poms_row_money_tooltips(row)
        return rows

    def get_options(self, field_name):
        return list(self.STATUS_OPTIONS.get(field_name, []))

    def get_labels(self, field_name):
        return [label for (label, _) in self.get_options(field_name)]

    def update_status(self, item_ids, field_name, value_token):
        if not isinstance(item_ids, (list, tuple, set)):
            item_ids = [item_ids]
        return self.db_manager.update_ops_status_bulk(item_ids, field_name, value_token)
