#!/bin/bash
echo "=== WSL total memory ==="
free -m | head -3
echo
echo "=== Docker container usage ==="
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'
echo
echo "=== Windows host memory (from WSL) ==="
powershell.exe -Command "Get-Counter '\\Memory\\Available Bytes','\\Memory\\Committed Bytes' | Select-Object -ExpandProperty CounterSamples | Select-Object Path,CookedValue | Format-Table -AutoSize" 2>&1 | head -10
