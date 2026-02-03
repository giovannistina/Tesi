# Tesi / experiments / equazione_differenziale / pipeline_global.py
# PIPELINE COMPLETA: Step 1 (Metriche) -> Step 2 (Parametri) -> Step 3 (Fairness)

import subprocess
import time
import sys
import os
from datetime import datetime

# --- CONFIGURAZIONE ---
# Pausa tra un modulo e l'altro (in secondi)
# 60 secondi = 1 minuto (Sufficiente per flush I/O e rilascio risorse)
WAIT_TIME_SECONDS = 60 

# Percorsi Dinamici
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Cartella corrente (/equazione_differenziale)

# Lista ordinata degli script da eseguire con il loro percorso relativo
SCRIPTS = [
    # --- STEP 1: Analisi Descrittiva e Popolarità ---
    "steps/1a_preprocessing.py",    # Estrae dati grezzi -> CSV
    "steps/1b_compute_Xt.py",       # Calcola popolarità X(t) e arricchisce eventi
    "steps/1c_analysis.py",         # Genera grafici descrittivi e Gini
    
    # --- STEP 2: Stima Parametri Fisici ---
    "steps/2a_parameter_estimation.py", # Stima Theta, Beta, Lambda (Assumption 1, 2, 3)

    # --- STEP 3: Analisi Fairness e Dominanza ---
    "steps/3a_fairness_analysis.py"     # Calcola Kendall Tau e Top-K Overlap
]

def print_log(message):
    """Stampa con timestamp e flush immediato (essenziale per nohup)"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)

def run_step(script_relative_path):
    # Costruiamo il percorso assoluto combinando BASE_DIR con il path relativo dello script
    script_path = os.path.join(BASE_DIR, script_relative_path)
    
    # 1. Verifica esistenza file
    if not os.path.exists(script_path):
        print_log(f"⚠️  ERRORE: Script non trovato: {script_path}")
        return False

    print_log(f"🚀 AVVIO MODULO: {script_relative_path}")
    print_log(f"   Percorso assoluto: {script_path}")
    print("-" * 60)
    
    start_time = time.time()
    
    # 2. Esecuzione Script
    # sys.executable garantisce che usiamo lo stesso ambiente python della pipeline
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=False  # Non crashare python, gestiamo noi il returncode
        )
        
        duration = time.time() - start_time
        print("-" * 60)
        
        if result.returncode == 0:
            print_log(f"✅ COMPLETATO: {script_relative_path} in {duration:.2f} s ({duration/60:.1f} min)")
            return True
        else:
            print_log(f"❌ FALLITO: {script_relative_path} ha restituito codice errore {result.returncode}")
            return False
            
    except Exception as e:
        print_log(f"❌ ECCEZIONE PYTHON: Errore nel lanciare lo script: {e}")
        return False

def main():
    print("=" * 60)
    print("      PIPELINE GLOBALE: (STEP 1, 2 & 3)      ")
    print("=" * 60)
    print_log(f"Directory base: {BASE_DIR}")
    print_log(f"Pausa tra script: {WAIT_TIME_SECONDS} secondi")
    
    total_start = time.time()
    
    for i, script in enumerate(SCRIPTS):
        success = run_step(script)
        
        # Se uno step fallisce, fermiamo tutto
        if not success:
            print_log("⛔ PIPELINE INTERROTTA. Risolvi l'errore sopra prima di ripartire.")
            sys.exit(1)
        
        # Se non è l'ultimo script, facciamo la pausa richiesta
        if i < len(SCRIPTS) - 1:
            print_log(f"⏳ PAUSA TECNICA: Attesa di {WAIT_TIME_SECONDS} secondi (1 min)...")
            time.sleep(WAIT_TIME_SECONDS)
            print_log("▶️  Ripresa esecuzione...")
            print("\n")

    total_duration = time.time() - total_start
    print("=" * 60)
    print_log(f"🎉 TUTTI I MODULI (STEP 1, 2 e 3) COMPLETATI CON SUCCESSO!")
    print_log(f"Tempo Totale Pipeline: {total_duration/60:.1f} minuti")
    print("=" * 60)

if __name__ == "__main__":
    main()