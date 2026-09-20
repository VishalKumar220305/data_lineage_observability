# activate_project.ps1
#
# Run this at the start of every session instead of manually activating
# the venv and setting JAVA_HOME separately:
#     .\activate_project.ps1
#
# It activates the Python venv AND points this terminal session at
# Java 17 (needed because Spark 3.5.x doesn't support Java 21+ yet).
# This only affects the current terminal window -- your system-wide
# Java 25 install is untouched.

& ".\venv\Scripts\Activate.ps1"

$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path

# Project-local Hadoop winutils (needed for Spark's saveAsTable on Windows)
$env:HADOOP_HOME = "$PWD\tools\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;" + $env:Path

Write-Host "Project environment ready: venv active, Java 17 set, HADOOP_HOME set for this session." -ForegroundColor Green
Write-Host "Run 'java -version' to confirm it shows 17.x.x" -ForegroundColor DarkGray
