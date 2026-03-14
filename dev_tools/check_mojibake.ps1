$cp1252 = [System.Text.Encoding]::GetEncoding(1252)
function M($bytes) { return $cp1252.GetString([byte[]]$bytes) }

$patterns = @(
  (M (0xC3,0x83)),
  (M (0xC3,0x82)),
  (M (0xC3,0xA2)),
  [string][char]0xFFFD
)

$exclude = @('\__pycache__\', '\refactor_backups\', '\_backup_', '\docs\', '\dev_tools\check_mojibake.ps1')
$files = Get-ChildItem -Recurse -File -Include *.py,*.json,*.md,*.txt,*.ui,*.qss |
  Where-Object {
    $full = $_.FullName
    -not ($exclude | Where-Object { $full -like "*$_*" })
  }

$hits = Select-String -Path ($files | ForEach-Object FullName) -Pattern $patterns -Encoding UTF8
if (-not $hits) {
  Write-Output 'Keine typischen Mojibake-Treffer gefunden.'
  exit 0
}

$hits | ForEach-Object {
  $relative = $_.Path.Replace((Get-Location).Path + '\', '')
  '{0}:{1}: {2}' -f $relative, $_.LineNumber, $_.Line.Trim()
}
exit 1