"""
Generiert wiederholbar Fake-Dokumente fuer lokale E2E-Tests.

Ausfuehren:
    python dev_tools/generate_fake_documents.py
"""

from __future__ import annotations

from datetime import datetime
from email.utils import format_datetime
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "test_daten"


def _font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = []
    if bold:
        candidates += ["arialbd.ttf", "segoeuib.ttf"]
    candidates += ["arial.ttf", "segoeui.ttf"]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _draw_invoice(
    path: Path,
    title: str,
    supplier: str,
    order_no: str,
    invoice_no: str,
    invoice_date: str,
    items: Iterable[dict],
    payment: str,
    tracking: str = "",
) -> None:
    image = Image.new("RGB", (1400, 1900), "white")
    draw = ImageDraw.Draw(image)

    f_h1 = _font(66, bold=True)
    f_h2 = _font(38, bold=True)
    f_t = _font(30)
    f_small = _font(24)

    y = 70
    draw.text((70, y), title, fill="black", font=f_h1)
    y += 110
    draw.text((70, y), supplier, fill="black", font=f_t)

    right_x = 820
    draw.text((right_x, 180), f"Rechnungsnr: {invoice_no}", fill="black", font=f_t)
    draw.text((right_x, 230), f"Bestellnr: {order_no}", fill="black", font=f_t)
    draw.text((right_x, 280), f"Datum: {invoice_date}", fill="black", font=f_t)
    if tracking:
        draw.text((right_x, 330), f"Tracking: {tracking}", fill="black", font=f_t)

    top = 460
    draw.line((70, top, 1330, top), fill="black", width=3)
    draw.text((80, top + 16), "Menge", fill="black", font=f_t)
    draw.text((250, top + 16), "Produkt", fill="black", font=f_t)
    draw.text((850, top + 16), "EAN", fill="black", font=f_t)
    draw.text((1140, top + 16), "Gesamt", fill="black", font=f_t)
    draw.line((70, top + 65, 1330, top + 65), fill="black", width=2)

    y = top + 90
    total = 0.0
    for row in items:
        menge = int(row["menge"])
        name = str(row["produkt_name"])
        ean = str(row.get("ean", "") or "")
        stueckpreis = float(row["ekp_brutto"])
        gesamt = menge * stueckpreis
        total += gesamt

        draw.text((80, y), f"{menge}x", fill="black", font=f_t)
        draw.text((250, y), name, fill="black", font=f_t)
        draw.text((850, y), ean, fill="black", font=f_small)
        draw.text((1140, y), f"{gesamt:,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", "."), fill="black", font=f_t)
        y += 60

    box_y = y + 40
    draw.rectangle((790, box_y, 1330, box_y + 180), outline="black", width=3)
    draw.text((820, box_y + 30), "Gesamt brutto", fill="black", font=f_h2)
    draw.text(
        (820, box_y + 92),
        f"{total:,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", "."),
        fill="black",
        font=f_h2,
    )

    draw.text((70, box_y + 260), f"Zahlungsart: {payment}", fill="black", font=f_t)
    draw.text((70, box_y + 315), "Dies ist ein FAKE-Dokument fuer lokale Tests.", fill="#444444", font=f_small)

    image.save(path)


def _draw_discord_ticket(
    path: Path,
    channel: str,
    buyer: str,
    lines: Iterable[str],
    payout: str,
) -> None:
    image = Image.new("RGB", (1280, 860), "#2b2d31")
    draw = ImageDraw.Draw(image)

    f_h = _font(56, bold=True)
    f_user = _font(54, bold=True)
    f_t = _font(44)
    f_emph = _font(48, bold=True)
    f_small = _font(34)

    y = 35
    draw.text((40, y), f"# {channel}", fill="#f2f3f5", font=f_h)
    y += 90
    draw.line((40, y, 1240, y), fill="#3f4147", width=2)
    y += 35
    draw.text((40, y), buyer, fill="#5865f2", font=f_user)
    y += 70
    draw.text((40, y), "Biete:", fill="#f2f3f5", font=f_emph)
    y += 70
    for line in lines:
        draw.text((40, y), line, fill="#57f287", font=f_t)
        y += 58
    y += 15
    draw.text((40, y), payout, fill="#faa61a", font=f_emph)
    y += 80
    draw.text((40, y), "Hinweis: ticket folgt", fill="#f2f3f5", font=f_small)
    y += 50
    draw.text((40, y), "Mock fuer internen Test", fill="#b5bac1", font=f_small)

    image.save(path)


def _write_eml(
    path: Path,
    subject: str,
    sender: str,
    receiver: str,
    body_text: str,
    body_html: str,
) -> None:
    now = datetime.now()
    date_header = format_datetime(now)
    content = (
        f"From: {sender}\n"
        f"To: {receiver}\n"
        f"Subject: {subject}\n"
        f"Date: {date_header}\n"
        "MIME-Version: 1.0\n"
        'Content-Type: multipart/alternative; boundary="boundary123"\n'
        "\n"
        "--boundary123\n"
        'Content-Type: text/plain; charset="utf-8"\n'
        "\n"
        f"{body_text}\n"
        "\n"
        "--boundary123\n"
        'Content-Type: text/html; charset="utf-8"\n'
        "\n"
        f"{body_html}\n"
        "\n"
        "--boundary123--\n"
    )
    path.write_text(content, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    _draw_discord_ticket(
        OUT_DIR / "10_MOCK_Discord_Ticket_FOLGT.png",
        channel="bounty-ticket-folgt-22031",
        buyer="@Buyer_NoOrderYet",
        lines=[
            "3x Sony PlayStation 5 Pro",
            "EAN: ",
            "VK: 919.00 EUR pro Stueck",
        ],
        payout="Payout: Instant nach Label",
    )

    _draw_invoice(
        OUT_DIR / "11_MOCK_Rechnung_Nachtraegliches_Matching.png",
        title="Rechnung",
        supplier="Future Deals GmbH, Bergstr. 10, 60311 Frankfurt",
        order_no="FD-2026-PS5-441",
        invoice_no="RG-2026-441",
        invoice_date="2026-03-08",
        tracking="DHL-4482810032",
        payment="Ueberweisung (DE12 5001)",
        items=[
            {
                "produkt_name": "Sony PlayStation 5 Pro",
                "ean": "0711719577293",
                "menge": 3,
                "ekp_brutto": 812.40,
            }
        ],
    )

    _draw_invoice(
        OUT_DIR / "12_MOCK_Rechnung_Ohne_Discord.png",
        title="Rechnung",
        supplier="Nordbuy Trading KG, Hafenweg 22, 20457 Hamburg",
        order_no="NB-2026-1042",
        invoice_no="RG-2026-1042",
        invoice_date="2026-03-08",
        tracking="UPS-1Z88A91E6600001234",
        payment="Kreditkarte (Visa 7811)",
        items=[
            {
                "produkt_name": "Logitech G Pro X Superlight 2",
                "ean": "5099206103738",
                "menge": 2,
                "ekp_brutto": 108.90,
            },
            {
                "produkt_name": "Razer Viper V3 Pro",
                "ean": "8887910063588",
                "menge": 1,
                "ekp_brutto": 139.00,
            },
        ],
    )

    _write_eml(
        OUT_DIR / "13_MOCK_Email_Bestellung_Matching.eml",
        subject="Bestellbestaetigung FD-2026-PS5-441",
        sender="orders@future-deals.example",
        receiver="you@example.com",
        body_text=(
            "Danke fuer Ihre Bestellung FD-2026-PS5-441.\n"
            "Artikel: 3x Sony PlayStation 5 Pro\n"
            "Tracking: DHL-4482810032\n"
            "Gesamt: 2.437,20 EUR\n"
        ),
        body_html=(
            "<html><body>"
            "<h2>Bestellbestaetigung</h2>"
            "<p>Bestellnummer: <b>FD-2026-PS5-441</b></p>"
            "<p>Artikel: 3x Sony PlayStation 5 Pro</p>"
            "<p>Tracking: DHL-4482810032</p>"
            "<p>Gesamt brutto: 2.437,20 EUR</p>"
            "</body></html>"
        ),
    )

    _write_eml(
        OUT_DIR / "14_MOCK_Email_Bestellung_Ohne_Discord.eml",
        subject="Order confirmation NB-2026-1042",
        sender="noreply@nordbuy.example",
        receiver="you@example.com",
        body_text=(
            "Order number NB-2026-1042\n"
            "2x Logitech G Pro X Superlight 2\n"
            "1x Razer Viper V3 Pro\n"
            "Tracking: UPS-1Z88A91E6600001234\n"
        ),
        body_html=(
            "<html><body>"
            "<h2>Order confirmation</h2>"
            "<p>Order number: <b>NB-2026-1042</b></p>"
            "<p>Items: 2x Logitech G Pro X Superlight 2, 1x Razer Viper V3 Pro</p>"
            "<p>Tracking code: UPS-1Z88A91E6600001234</p>"
            "</body></html>"
        ),
    )

    print(f"Fake-Dokumente erstellt in: {OUT_DIR}")


if __name__ == "__main__":
    main()
