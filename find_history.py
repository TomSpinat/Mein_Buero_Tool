import os, re
path = r'C:\Users\timth\AppData\Roaming\Code\User\History'
for r, d, f in os.walk(path):
    for file in f:
        try:
            full_path = os.path.join(r, file)
            if os.path.getsize(full_path) > 3000 and os.path.getmtime(full_path) > 1700000000:
                text = open(full_path, encoding='utf-8', errors='ignore').read()
                if 'class OrderEntryApp' in text and 'einkauf_ui' in text:
                    print(f'MATCH: {full_path} - {os.path.getsize(full_path)} bytes')
        except:
            pass
