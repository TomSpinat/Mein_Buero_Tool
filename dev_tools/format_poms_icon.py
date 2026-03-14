from PIL import Image, ImageChops

def format_poms_icon():
    template_path = r"C:\Users\timth\Desktop\Mein_Buero_Tool\assets\icons\dash_inbound.png"
    src_path = r"C:\Users\timth\.gemini\antigravity\brain\ab6c5041-247a-4290-92d0-ceecdb017132\poms_icon_1772926260596.png"
    dest_path = r"C:\Users\timth\Desktop\Mein_Buero_Tool\assets\icons\dash_poms.png"
    bg_color = (27, 33, 52)

    # 1. Create a blank template exactly like the existing icons
    # dash_inbound is a 640x640 PNG with transparent corners and a dark blue rounded square box.
    template = Image.open(template_path).convert("RGBA")
    blank_template = Image.new("RGBA", template.size, (0, 0, 0, 0))
    pixels_t = template.load()
    pixels_b = blank_template.load()
    for y in range(template.height):
        for x in range(template.width):
            r, g, b, a = pixels_t[x, y]
            if a > 0:
                pixels_b[x, y] = (bg_color[0], bg_color[1], bg_color[2], a)
                
    # 2. Extract the drawing from the generated AI icon
    src = Image.open(src_path).convert("RGBA")
    pixels_s = src.load()
    
    min_x, min_y = src.width, src.height
    max_x, max_y = 0, 0
    
    for y in range(src.height):
        for x in range(src.width):
            r, g, b, a = pixels_s[x, y]
            # Replace pure black (with tolerance) with transparency
            if r < 15 and g < 15 and b < 15:
                pixels_s[x, y] = (r, g, b, 0)
            else:
                if x < min_x: min_x = x
                if y < min_y: min_y = y
                if x > max_x: max_x = x
                if y > max_y: max_y = y

    # Catch if no pixels were found
    if min_x > max_x:
        min_x, min_y, max_x, max_y = 0, 0, src.width-1, src.height-1

    drawing = src.crop((min_x, min_y, max_x + 1, max_y + 1))
    
    # 3. Resize the drawing to match the scale of other icons.
    # A standard dash icon inner drawing seems to take up around 40% of the rounded box size.
    # The rounded box in 640x640 is around 500x500 pixels.
    # Let's scale the drawing to about 280x280.
    draw_w, draw_h = drawing.size
    aspect = draw_w / draw_h
    if draw_w > draw_h:
        new_w = 280
        new_h = int(280 / aspect)
    else:
        new_h = 280
        new_w = int(280 * aspect)
        
    drawing = drawing.resize((new_w, new_h), Image.Resampling.LANCZOS)
    
    # 4. Paste centered
    offset_x = (blank_template.width - new_w) // 2
    offset_y = (blank_template.height - new_h) // 2
    blank_template.paste(drawing, (offset_x, offset_y), drawing)
    
    # Save it
    blank_template.save(dest_path, "PNG")
    print(f"Saved matched icon to {dest_path}")

if __name__ == "__main__":
    format_poms_icon()
