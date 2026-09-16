param(
    [ValidateRange(30, 86400)]
    [int]$IntervalSeconds = 60
)

# Re-runs the historical Spark aggregates at a safe demonstration cadence.
# Run this in a separate PowerShell window after Kafka Connect is healthy.
$projectRoot = Split-Path -Parent $PSScriptRoot
while ($true) {
    & spark-submit --packages com.mysql:mysql-connector-j:8.4.0 `
        --conf spark.eventLog.enabled=true `
        --conf spark.eventLog.dir=hdfs://localhost:9000/spark-history `
        (Join-Path $projectRoot "spark\batch_insights.py")
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "batch_insights.py failed (exit code $LASTEXITCODE); retrying after the interval."
    }
    Start-Sleep -Seconds $IntervalSeconds
}
