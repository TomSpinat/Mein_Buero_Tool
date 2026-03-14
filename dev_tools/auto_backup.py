import os
import zipfile
import datetime

def create_backup():
    source_dir = r"c:\Users\timth\Desktop\Mein_Buero_Tool"
    backup_dir = r"c:\Users\timth\Desktop\Mein_Buero_Tool\backups\auto_backups"
    
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
        
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = os.path.join(backup_dir, f"backup_{timestamp}.zip")
    
    # Ordner, die nicht mit ins Backup sollen
    exclude_dirs = {'.git', '__pycache__', 'backups', 'refactor_backups', '.codex'}
    exclude_prefixes = ('_backup_',)
    
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(source_dir):
                # Ordner filtern
                dirs[:] = [d for d in dirs if d not in exclude_dirs and not d.startswith(exclude_prefixes)]
                
                for file in files:
                    file_path = os.path.join(root, file)
                    # Sicherstellen, dass die aktuelle ZIP-Datei nicht selbst gepackt wird
                    if file_path == zip_filename:
                        continue
                    
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
                    
        print(f"Backup erfolgreich erstellt: {zip_filename}")
    except Exception as e:
        print(f"Fehler beim Erstellen des Backups: {e}")

if __name__ == "__main__":
    create_backup()
