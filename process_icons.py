from PIL import Image
import os

def process_icon(input_path, output_path, target_color=(226, 226, 230, 255)):
    """ 
    Konvertiert ein Schwarz-Weiß-Bild in ein Bild mit transparentem Hintergrund
    und färbt die weißen Linien in der gewünschten Farbe ein.
    """
    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return
        
    img = Image.open(input_path).convert("RGBA")
    datas = img.getdata()

    newData = []
    # target_color = (226, 226, 230) was the default text color in our theme
    # Let's make the icons stand out a bit more with pure white or light gray
    r, g, b, a_max = target_color
    
    for item in datas:
        # Helligkeit des Pixels bestimmen (0 = Schwarz, 255 = Weiß)
        brightness = (item[0] + item[1] + item[2]) / 3
        
        # Schwarz wird komplett transparent (alpha = 0)
        # Weiß behält seine Deckkraft (alpha entspr. Helligkeit)
        alpha = int(brightness)
        
        newData.append((r, g, b, alpha))

    img.putdata(newData)
    
    img.save(output_path, "PNG")
    print(f"Saved to {output_path}")

base_dir = r"C:\Users\timth\.gemini\antigravity\brain\33f6f2d6-3c90-458b-ac01-9023c9908c10"
dest_dir = r"c:\Users\timth\Desktop\Mein_Buero_Tool\assets\icons"

# Die neu generierten KI-Bilder einlesen
# Falls die Zeitstempel im Dateinamen abweichen, bitte anpassen:
process_icon(os.path.join(base_dir, "clean_app_icon_1772634118029.png"), os.path.join(dest_dir, "app_icon.png"), target_color=(255, 255, 255, 255))
process_icon(os.path.join(base_dir, "clean_save_icon_1772634132212.png"), os.path.join(dest_dir, "icon_save.png"), target_color=(255, 255, 255, 255))
process_icon(os.path.join(base_dir, "clean_settings_icon_1772634144884.png"), os.path.join(dest_dir, "icon_settings.png"), target_color=(255, 255, 255, 255))
