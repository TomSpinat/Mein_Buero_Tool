"""
Erzeugt 3 gezielte Mock-Dokumente fuer den Matching-Test:
1) Discord-Ticket mit EAN
2) Bestelluebersicht mit Preisdaten
3) Rechnung mit Skonto (PNG + PDF)
"""

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path(__file__).resolve().parent / "test_daten"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _font(size: int, bold: bool = False):
    names = ["arialbd.ttf", "segoeuib.ttf"] if bold else []
    names += ["arial.ttf", "segoeui.ttf"]
    for name in names:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _draw_discord_ticket(path: Path):
    img = Image.new("RGB", (1280, 860), "#2b2d31")
    d = ImageDraw.Draw(img)

    h = _font(54, True)
    t = _font(42)
    e = _font(46, True)

    y = 35
    d.text((40, y), "# bounty-ps5-ean-441", fill="#f2f3f5", font=h)
    y += 95
    d.line((40, y, 1240, y), fill="#3f4147", width=2)
    y += 35
    d.text((40, y), "@Buyer_EAN_Test", fill="#5865f2", font=e)
    y += 72
    d.text((40, y), "Biete:", fill="#f2f3f5", font=e)
    y += 72
    d.text((40, y), "3x Sony PlayStation 5 Pro", fill="#57f287", font=t)
    y += 58
    d.text((40, y), "EAN: 0711719577293", fill="#57f287", font=t)
    y += 58
    d.text((40, y), "VK: 919.00 EUR pro Stueck", fill="#57f287", font=t)
    y += 85
    d.text((40, y), "Hinweis: ticket folgt", fill="#f2f3f5", font=_font(34))

    img.save(path)


def _draw_order_overview(path: Path):
    img = Image.new("RGB", (1400, 1800), "white")
    d = ImageDraw.Draw(img)

    h1 = _font(62, True)
    h2 = _font(38, True)
    t = _font(30)

    d.text((70, 60), "Bestelluebersicht", fill="black", font=h1)
    d.text((70, 150), "Future Deals GmbH", fill="black", font=t)

    d.text((860, 170), "Bestellnummer: FD-2026-PS5-441", fill="black", font=t)
    d.text((860, 220), "Kaufdatum: 2026-03-08", fill="black", font=t)
    d.text((860, 270), "Tracking: DHL-4482810032", fill="black", font=t)

    top = 410
    d.line((70, top, 1330, top), fill="black", width=3)
    d.text((80, top + 16), "Menge", fill="black", font=t)
    d.text((250, top + 16), "Produkt", fill="black", font=t)
    d.text((850, top + 16), "EAN", fill="black", font=t)
    d.text((1110, top + 16), "Stueckpreis", fill="black", font=t)
    d.line((70, top + 65, 1330, top + 65), fill="black", width=2)

    d.text((80, top + 95), "3x", fill="black", font=t)
    d.text((250, top + 95), "Sony PlayStation 5 Pro", fill="black", font=t)
    d.text((850, top + 95), "0711719577293", fill="black", font=_font(24))
    d.text((1110, top + 95), "812,40 EUR", fill="black", font=t)

    y = 720
    d.text((880, y), "Warenwert:", fill="black", font=t)
    d.text((1140, y), "2.437,20 EUR", fill="black", font=t)
    y += 45
    d.text((880, y), "Versandkosten:", fill="black", font=t)
    d.text((1140, y), "24,90 EUR", fill="black", font=t)
    y += 45
    d.text((880, y), "Nebenkosten:", fill="black", font=t)
    d.text((1140, y), "5,00 EUR", fill="black", font=t)
    y += 65
    d.text((880, y), "Gesamt brutto:", fill="black", font=h2)
    d.text((1120, y), "2.467,10 EUR", fill="black", font=h2)

    d.text((70, 1600), "Mock-Dokument fuer Modul-1-Tests", fill="#444444", font=_font(24))
    img.save(path)


def _draw_invoice_with_skonto(png_path: Path, pdf_path: Path):
    img = Image.new("RGB", (1400, 1900), "white")
    d = ImageDraw.Draw(img)

    h1 = _font(62, True)
    h2 = _font(38, True)
    t = _font(30)

    d.text((70, 60), "Rechnung", fill="black", font=h1)
    d.text((70, 145), "Future Deals GmbH", fill="black", font=t)

    d.text((840, 170), "Rechnungsnr: RG-2026-441-SK", fill="black", font=t)
    d.text((840, 220), "Bestellnummer: FD-2026-PS5-441", fill="black", font=t)
    d.text((840, 270), "Datum: 2026-03-12", fill="black", font=t)

    top = 430
    d.line((70, top, 1330, top), fill="black", width=3)
    d.text((80, top + 16), "Menge", fill="black", font=t)
    d.text((250, top + 16), "Produkt", fill="black", font=t)
    d.text((860, top + 16), "EAN", fill="black", font=t)
    d.text((1130, top + 16), "Gesamt", fill="black", font=t)
    d.line((70, top + 65, 1330, top + 65), fill="black", width=2)

    d.text((80, top + 95), "3x", fill="black", font=t)
    d.text((250, top + 95), "Sony PlayStation 5 Pro", fill="black", font=t)
    d.text((860, top + 95), "0711719577293", fill="black", font=_font(24))
    d.text((1130, top + 95), "2.437,20 EUR", fill="black", font=t)

    y = 760
    d.text((860, y), "Warenwert:", fill="black", font=t)
    d.text((1130, y), "2.437,20 EUR", fill="black", font=t)
    y += 45
    d.text((860, y), "Skonto (2%):", fill="black", font=t)
    d.text((1130, y), "-48,74 EUR", fill="black", font=t)
    y += 45
    d.text((860, y), "Versand:", fill="black", font=t)
    d.text((1130, y), "24,90 EUR", fill="black", font=t)
    y += 65
    d.text((860, y), "Gesamt brutto:", fill="black", font=h2)
    d.text((1105, y), "2.413,36 EUR", fill="black", font=h2)

    d.text((70, 1670), "Zahlungsart: Ueberweisung DE12 5001", fill="black", font=t)
    d.text((70, 1730), "Mock-Dokument mit Skonto fuer Nachreich-Test", fill="#444444", font=_font(24))

    img.save(png_path)
    img.save(pdf_path, "PDF", resolution=150.0)


def main():
    ticket = OUT_DIR / "15_MOCK_Discord_Ticket_mit_EAN.png"
    order = OUT_DIR / "16_MOCK_Bestelluebersicht_mit_Preisdaten.png"
    inv_png = OUT_DIR / "17_MOCK_Rechnung_mit_Skonto.png"
    inv_pdf = OUT_DIR / "17_MOCK_Rechnung_mit_Skonto.pdf"

    _draw_discord_ticket(ticket)
    _draw_order_overview(order)
    _draw_invoice_with_skonto(inv_png, inv_pdf)

    print("Erstellt:")
    print(ticket)
    print(order)
    print(inv_png)
    print(inv_pdf)


if __name__ == "__main__":
    main()
