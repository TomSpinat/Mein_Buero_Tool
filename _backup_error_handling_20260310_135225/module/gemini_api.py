"""
gemini_api.py
Dieses Modul kuemmert sich um die Kommunikation mit Google Gemini.
Es nimmt Bild/Text entgegen und gibt ein sauberes Python-Dictionary zurueck.
"""

import json
import re

import google.generativeai as genai


from module.crash_logger import log_exception
def test_api_key(api_key):
    """Prueft, ob der API-Key gueltig ist."""
    if not api_key:
        return False
    try:
        genai.configure(api_key=api_key)
        models = genai.list_models()
        for model in models:
            if "generateContent" in model.supported_generation_methods:
                return True
        return False
    except Exception as e:
        log_exception(__name__, e)
        return False


def _build_einkauf_prompt(custom_text):
    return f"""
        Du bist ein penibler und exakter Buchhaltungs-Assistent.
        Analysiere das beigefuegte Dokument (Rechnung, Screenshot, Bestellbestaetigung).
        Falls nur Text und kein Bild vorhanden ist, analysiere strikt nur den Text.

        EXTRA TEXT DES NUTZERS (Dieser ueberschreibt oder ergaenzt die Daten):
        \"{custom_text}\"

        WICHTIGE REGELN ZUR DATENEXTRAKTION:
        1. EXAKTHEIT: Erfinde NIEMALS Daten. Verwende niemals Platzhalter.
        2. FEHLENDE DATEN: Wenn ein Wert nicht klar ersichtlich ist, MUSS der Wert ein leerer String \"\" bleiben.
        3. AMAZON BESTELLUNGEN: Wenn du Amazon erkennst, setze als \"shop_name\" nur \"Amazon\".
        4. ZAHLUNGSARTEN: Letzte 4 Ziffern von Karte/IBAN extrahieren (z.B. \"Visa 1234\").
        5. BILDER: Suche im HTML nach src von <img> nahe Produkttiteln und gib die direkte URL als \"bild_url\" aus.
        6. DATUM: Fremdsprachige Datumsangaben ins Format YYYY-MM-DD umsetzen. Falls Jahr fehlt, aktuelles Jahr annehmen.
        7. MENGE: Menge ist kritisch. \"Qtd.: 9\" bedeutet Menge = 9, nicht 1.
        8. PREIS-LOGIK:
           - \"ekp_brutto\" je Artikel ist der Brutto-Stueckpreis nur fuer das Produkt.
           - Versand, Servicegebuehr, Payment Fee, Zoll usw. NICHT in \"ekp_brutto\" einrechnen.
           - Zusatzkosten in \"versandkosten_brutto\" oder \"nebenkosten_brutto\" ausgeben.
           - Rabatte/Gutschriften in \"rabatt_brutto\" ausgeben (positiver Zahlenwert als Abzug).
           - \"gesamt_ekp_brutto\" darf von Summe(Stueckpreis x Menge) abweichen, wenn Zusatzkosten vorhanden sind.
        9. FORMAT: Antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt.

        Erwartetes JSON-Format:
        {{
            \"bestellnummer\": \"Wert\",
            \"kaufdatum\": \"Wert (YYYY-MM-DD)\",
            \"shop_name\": \"Wert\",
            \"bestell_email\": \"Wert\",
            \"tracking_nummer_einkauf\": \"Wert\",
            \"paketdienst\": \"Wert\",
            \"lieferdatum\": \"Wert\",
            \"sendungsstatus\": \"Wert\",
            \"gesamt_ekp_brutto\": \"Wert (als Zahl)\",
            \"versandkosten_brutto\": \"Wert (als Zahl, optional)\",
            \"nebenkosten_brutto\": \"Wert (als Zahl, optional)\",
            \"rabatt_brutto\": \"Wert (als Zahl, optional, positiver Abzugswert)\",
            \"ust_satz\": \"Wert (als Zahl, z.B. 19.00)\",
            \"zahlungsart\": \"Wert\",
            \"waren\": [
                {{
                    \"produkt_name\": \"Wert\",
                    \"varianten_info\": \"Wert\",
                    \"ean\": \"Wert\",
                    \"menge\": \"Wert (als Ganzzahl)\",
                    \"ekp_brutto\": \"Wert (Brutto-Stueckpreis nur Produkt)\",
                    \"bild_url\": \"Wert (Absolute HTTP URL zum Produkt-Thumbnail)\"
                }}
            ]
        }}
    """


def _build_verkauf_prompt(custom_text):
    return f"""
        Du bist ein Buchhaltungs-Assistent.
        Analysiere das beigefuegte Discord-Verkaufsticket (Arbitrage Bounty).

        EXTRA TEXT DES NUTZERS:
        \"{custom_text}\"

        WICHTIGE REGELN ZUR DATENEXTRAKTION:
        1. EXAKTHEIT: Erfinde NIEMALS Daten.
        2. FEHLENDE DATEN: Wenn EAN oder andere Daten fehlen, lass sie leer (\"\").
        3. Ticket-Name/ID: Extrahiere den Ticket-Namen (z.B. \"drittserver-13209\").
        4. Kaeufer: Extrahiere den Nutzer-Tag, falls sichtbar.
        5. FORMAT: Antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt.

        Erwartetes JSON-Format:
        {{
            \"ticket_name\": \"Wert (Name des Tickets)\",
            \"kaeufer\": \"Wert (Nutzername des Kaeufers)\",
            \"zahlungsziel\": \"Wert (z.B. Instant)\",
            \"waren\": [
                {{
                    \"produkt_name\": \"Wert\",
                    \"ean\": \"Wert (GANZ WICHTIG!)\",
                    \"menge\": \"Wert (als Ganzzahl)\",
                    \"vk_brutto\": \"Wert (als Zahl, Stueckpreis brutto falls ersichtlich, sonst Gesamt durch Menge)\",
                    \"marge_gesamt\": \"Wert (als Zahl, falls angegeben)\"
                }}
            ]
        }}
    """


def process_receipt_with_gemini(api_key, image_path=None, custom_text="", scan_mode="einkauf"):
    """
    Sendet Bild/Text an Gemini und extrahiert die gewuenschten Felder.

    :param api_key: Gemini API Key
    :param image_path: Pfad zu einem Bild (optional)
    :param custom_text: Freitext (optional)
    :param scan_mode: "einkauf" oder "verkauf"
    :return: Dictionary mit extrahierten Schluesseln
    """
    if not api_key:
        raise ValueError("Kein API Key hinterlegt! Bitte in den Einstellungen eintragen.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = _build_einkauf_prompt(custom_text) if scan_mode == "einkauf" else _build_verkauf_prompt(custom_text)

    contents = [prompt]

    if image_path:
        try:
            image_file = genai.upload_file(path=image_path)
            contents.append(image_file)
        except Exception as exc:
            log_exception(__name__, exc)
            raise Exception(f"Fehler beim Hochladen des Bildes zu Google: {exc}")

    try:
        token_count = model.count_tokens(contents).total_tokens
    except Exception as e:
        log_exception(__name__, e)
        token_count = len(str(contents)) // 4

    try:
        response = model.generate_content(contents)
        cleaned_text = response.text.strip()
        cleaned_text = re.sub(r"^```json\s*", "", cleaned_text)
        cleaned_text = re.sub(r"\s*```$", "", cleaned_text)

        result_dict = json.loads(cleaned_text)
        result_dict["_token_count"] = token_count
        return result_dict

    except json.JSONDecodeError:
        raise Exception(f"Die KI hat keine sauberen JSON-Daten zurueckgeliefert. Antwort war: {response.text}")
    except Exception as exc:
        log_exception(__name__, exc)
        error_msg = str(exc).lower()
        if "429" in error_msg or "quota" in error_msg or "rate limit" in error_msg:
            raise Exception("Gemini Free-Tier Limit erreicht! Bitte warte eine Minute.")
        raise Exception(f"Fehler bei der Kommunikation mit Gemini: {exc}")
