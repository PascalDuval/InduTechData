Write-Host "=== Demarrage environnement PySpark ==="

cd $PSScriptRoot

# Activer le venv
.\venv\Scripts\activate

# Variables Java / Spark / Hadoop
$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:SPARK_HOME="C:\spark"
$env:HADOOP_HOME="C:\hadoop"

# PATH propre
$env:PATH="$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"

# Forcer Python du venv pour Spark (CRUCIAL)
$env:PYSPARK_DRIVER_PYTHON="$PSScriptRoot\venv\Scripts\python.exe"
$env:PYSPARK_PYTHON="$PSScriptRoot\venv\Scripts\python.exe"

Write-Host "Python utilise par Spark :"
python -c "import sys; print(sys.executable)"

Write-Host ""
Write-Host "Environnement pret."