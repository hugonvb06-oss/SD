param(
    [Parameter(Mandatory=$true, Position=0)]
    [ValidateSet("central", "fo", "ws-mon", "ws-eng")]
    [string]$Module,
    [Parameter(Position=1, ValueFromRemainingArguments)]
    [string[]]$RestArgs = @()
)

$ROOT = Split-Path -Parent $PSScriptRoot
$java = "C:\Program Files\Java\jdk-23\bin\java.exe"
if (-not (Test-Path $java)) { $java = "java" }
$CP = "$ROOT\lib\jackson-databind-2.18.3.jar;$ROOT\lib\jackson-core-2.18.3.jar;$ROOT\lib\jackson-annotations-2.18.3.jar;$ROOT\lib\kafka-clients-3.9.1.jar;$ROOT\lib\sqlite-jdbc-3.53.4.0.jar;$ROOT\lib\slf4j-api-1.7.36.jar;$ROOT\lib\slf4j-simple-1.7.36.jar"

switch ($Module) {
    "central" {
        $javaArgs = @("-cp", "$ROOT\WM_Central\out;$CP", "ServidorCentral") + $RestArgs
        & $java @javaArgs
    }
    "fo" {
        $javaArgs = @("-cp", "$ROOT\WM_FO\out;$CP", "WM_FO") + $RestArgs
        & $java @javaArgs
    }
    "ws-mon" {
        $javaArgs = @("-cp", "$ROOT\WM_WS\bin", "WM_WS_M") + $RestArgs
        & $java @javaArgs
    }
    "ws-eng" {
        $javaArgs = @("-cp", "$ROOT\WM_WS\bin", "WM_WS_E") + $RestArgs
        & $java @javaArgs
    }
}
