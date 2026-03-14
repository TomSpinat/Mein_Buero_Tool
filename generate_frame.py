import os
from PyQt6.QtGui import QImage, QPainter, QColor, QTransform
from PyQt6.QtCore import Qt, QRect

def create_frame_from_line():
    """Liest die trennlinie.png und generiert ein rahmen.png für Buttons."""
    input_path = "assets/trennlinie.png"
    output_path = "assets/rahmen.png"
    
    if not os.path.exists(input_path):
        print(f"Fehler: {input_path} nicht gefunden!")
        return

    # Lade die originale Linie
    line_img = QImage(input_path)
    
    # Die Maße der Linie (z.B. Breite=800, Höhe=20)
    w = line_img.width()
    h = line_img.height()
    
    target_size = min(max(h * 4, 200), w) if w > h else min(max(w * 4, 200), h)
    
    # Neues Bild erstellen (mit transparentem Hintergrund)
    frame_img = QImage(target_size, target_size, QImage.Format.Format_ARGB32)
    frame_img.fill(QColor(0, 0, 0, 0)) # Komplett Transparent
    
    painter = QPainter(frame_img)
    
    # Oben (Linie wie sie ist, zentriert)
    if w > h: # Horizontale Linie
        crop_x = (w - target_size) // 2
        painter.drawImage(0, 0, line_img, crop_x, 0, target_size, h)
        
        # Unten (180 Grad gedreht, oder einfach so)
        painter.drawImage(0, target_size - h, line_img, crop_x, 0, target_size, h)
        
        # Links (90 Grad gedreht)
        transform = QTransform().rotate(-90)
        rotated_img = line_img.transformed(transform)
        crop_y = (rotated_img.height() - target_size) // 2
        painter.drawImage(0, 0, rotated_img, 0, crop_y, h, target_size)
        
        # Rechts (90 Grad gedreht)
        painter.drawImage(target_size - h, 0, rotated_img, 0, crop_y, h, target_size)
    else:
        pass 

    painter.end()
    
    frame_img.save(output_path)
    print(f"Erfolg! Rahmen gespeichert unter: {output_path}")

if __name__ == "__main__":
    create_frame_from_line()
