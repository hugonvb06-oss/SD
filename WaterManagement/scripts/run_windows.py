#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mini lanzador de los modulos WaterManagement para Windows (sin IDE).

Uso:
  python scripts/run_windows.py central  [portSocket] [ipBroker] [portBroker] [rutaBD]
  python scripts/run_windows.py fo      [ipBroker] [portBroker] [idOperario]
  python scripts/run_windows.py ws-mon [idWS] [ubicacion] [caudalLMin] [portMonitor] [ipCentral] [portCentral]
  python scripts/run_windows.py ws-eng [ipMonitor] [portMonitor] [dirEstado]
"""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JAVA = r"C:\Program Files\Java\jdk-23\bin\java.exe"
if not os.path.exists(JAVA):
    JAVA = "java"

CASES = {
    "central": ([JAVA, "-cp", os.path.join(ROOT, "WM_Central", "out") + ";" + os.path.join(ROOT, "lib") + "\\*", "ServidorCentral"],
                []),
    "fo": ([JAVA, "-cp", os.path.join(ROOT, "WM_FO", "out") + ";" + os.path.join(ROOT, "lib") + "\\*", "WM_FO"],
          []),
    "ws-mon": ([JAVA, "-cp", os.path.join(ROOT, "WM_WS", "bin"), "WM_WS_M"],
               []),
    "ws-eng": ([JAVA, "-cp", os.path.join(ROOT, "WM_WS", "bin"), "WM_WS_E"],
               []),
}


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    module, args = argv[1], argv[2:]
    cmd, _ = CASES[module]
    if module == "central":
        cmd = [JAVA, "-cp", os.path.join(ROOT, "WM_Central", "out") + ";" + os.path.join(ROOT, "lib") + "\\*",
               "ServidorCentral"] + args
    elif module == "fo":
        cmd = [JAVA, "-cp", os.path.join(ROOT, "WM_FO", "out") + ";" + os.path.join(ROOT, "lib") + "\\*",
               "WM_FO"] + args
    elif module == "ws-mon":
        cmd = [JAVA, "-cp", os.path.join(ROOT, "WM_WS", "bin"), "WM_WS_M"] + args
    elif module == "ws-eng":
        cmd = [JAVA, "-cp", os.path.join(ROOT, "WM_WS", "bin"), "WM_WS_E"] + args
    return subprocess.run([str(x) for x in cmd]).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv))
