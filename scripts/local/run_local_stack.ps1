# Local stack runner for HVACPulse.
#
#   .\scripts\local\run_local_stack.ps1 -Task all
#
# Order matters: Kafka up -> topics -> simulate -> bronze -> silver -> gold.
# `-Task all` runs the full demo in that order (streaming jobs use
# trigger(availableNow) so each one drains its input and exits).
#
# Prerequisites: Docker Desktop, Python 3.10+ (or uv), Java 11/17 for Spark.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("up", "down", "topics", "simulate", "bronze", "silver", "gold",
                 "quality", "recommendations", "reconcile", "benchmark", "dashboard",
                 "maintain", "semantic", "test", "verify", "all", "plant-all", "plant-cycle",
                 "plant-silver", "features", "train", "control")]
    [string]$Task,

    [int]$Count = 2000,
    [double]$Rate = 100.0,
    [double]$AnomalyRate = 0.15,
    [string]$Topic = "hvac.telemetry",
    [int]$Partitions = 6,
    [switch]$SimulateControl,
    [string]$PythonExecutable = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ComposeFile = Join-Path $RepoRoot "infra\docker\compose\docker-compose.yml"

$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if ($PythonExecutable) { $Python = $PythonExecutable }
if (-not (Test-Path $Python)) { $Python = "python" }
$env:HVACPULSE_TOPIC = $Topic

function Invoke-Plant {
    param([string]$Module, [string[]]$Arguments)
    Push-Location $RepoRoot
    try {
        & $Python -m $Module @Arguments
        if ($LASTEXITCODE -ne 0) { throw "Plant stage failed: $Module" }
    } finally { Pop-Location }
}

function Invoke-Up {
    # Windows PowerShell turns redirected native stderr into an error record;
    # under Stop, a cold broker's startup WARN would abort the readiness retry.
    $ErrorActionPreference = "Continue"
    docker compose -f $ComposeFile up -d
    Write-Host "Waiting for Kafka..."
    $ready = $false
    foreach ($i in 1..30) {
        docker exec hvacpulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw "Kafka did not become ready in time." }
    docker compose -f $ComposeFile ps
}

function Invoke-Down {
    docker compose -f $ComposeFile down
}

function Invoke-Topics {
    Push-Location $RepoRoot
    try {
        # Broker-agnostic: works whether the broker is our hvacpulse-kafka
        # container or another compose project's broker on 9092.
        # Single quotes only: PowerShell mangles embedded double quotes in -c.
        & $Python -c @"
from kafka.admin import KafkaAdminClient, NewTopic
admin = KafkaAdminClient(bootstrap_servers='127.0.0.1:19092')
try:
    admin.create_topics([NewTopic('$Topic', num_partitions=$Partitions, replication_factor=1)])
    print('created topic $Topic')
except Exception as exc:
    if 'TopicAlreadyExists' in type(exc).__name__:
        print('topic $Topic already exists')
    else:
        raise
print(admin.describe_topics(['$Topic']))
"@
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Simulate {
    Push-Location $RepoRoot
    try {
        & $Python -m ingestion.producer --bootstrap-servers 127.0.0.1:19092 `
            --topic $Topic --count $Count --rate $Rate --anomaly-rate $AnomalyRate
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Bronze {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.bronze_ingest --once
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Silver {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.silver_transform --once
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Gold {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.gold_metrics
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Quality {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.quality_metrics
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Recommendations {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.recommendations_job
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Reconcile {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.reconciliation
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Benchmark {
    Push-Location $RepoRoot
    try {
        & $Python scripts\local\benchmark.py
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Dashboard {
    Push-Location $RepoRoot
    try {
        & $Python -m streamlit run dashboard\app.py
    } finally { Pop-Location }
}

function Invoke-Maintenance {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.maintenance --optimize --vacuum --retention-days 7
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Semantic {
    Push-Location $RepoRoot
    try {
        & $Python -m streaming.jobs.semantic_layer
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Verify {
    Push-Location $RepoRoot
    try {
        & $Python scripts\local\verify_local.py
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

function Invoke-Test {
    Push-Location $RepoRoot
    try {
        & $Python -m pytest tests/unit -m unit
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        & $Python contracts\check_compatibility.py contracts\avro
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally { Pop-Location }
}

switch ($Task) {
    "plant-all" {
        Invoke-Up
        Invoke-Topics
        $PlantArgs = @("bootstrap", "--count", "$Count")
        if ($SimulateControl) { $PlantArgs += "--simulate" }
        Invoke-Plant "streaming.jobs.plant_pipeline" $PlantArgs
    }
    "plant-cycle" {
        $PlantArgs = @("cycle")
        if ($SimulateControl) { $PlantArgs += "--simulate" }
        Invoke-Plant "streaming.jobs.plant_pipeline" $PlantArgs
    }
    "plant-silver" { Invoke-Plant "streaming.jobs.plant_silver" @("--once") }
    "features" { Invoke-Plant "streaming.jobs.plant_features" @("--once") }
    "train" { Invoke-Plant "streaming.jobs.plant_learning" @("train") }
    "control" {
        $PlantArgs = @("control")
        if ($SimulateControl) { $PlantArgs += "--simulate" }
        Invoke-Plant "streaming.jobs.plant_learning" $PlantArgs
    }
    "up"       { Invoke-Up }
    "down"     { Invoke-Down }
    "topics"   { Invoke-Topics }
    "simulate" { Invoke-Simulate }
    "bronze"   { Invoke-Bronze }
    "silver"   { Invoke-Silver }
    "gold"     { Invoke-Gold }
    "quality"  { Invoke-Quality }
    "recommendations" { Invoke-Recommendations }
    "reconcile" { Invoke-Reconcile }
    "benchmark" { Invoke-Benchmark }
    "dashboard" { Invoke-Dashboard }
    "maintain"  { Invoke-Maintenance }
    "semantic"  { Invoke-Semantic }
    "test"     { Invoke-Test }
    "verify"   { Invoke-Verify }
    "all" {
        Invoke-Up
        Invoke-Topics
        Invoke-Simulate
        Invoke-Bronze
        Invoke-Silver
        Invoke-Gold
        Invoke-Quality
        Invoke-Recommendations
        Invoke-Reconcile
        Invoke-Semantic
        Invoke-Maintenance
        Invoke-Verify
        Invoke-Benchmark
    }
}
