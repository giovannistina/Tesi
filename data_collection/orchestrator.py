# Tesi/data_collection/orchestrator.py


import subprocess
import time
import sys
import datetime
import os

# --- CONFIGURAZIONE ORARI ---
DURATA_ACQUISIZIONE = 20  # Minuti di lavoro per ogni sessione

# Mappa: ORA -> NOME FILE
# (Chiave: Ora del giorno 0-23, Valore: Nome del file output)
SCHEDULE = {
    0:  "1.txt",
    6:  "2.txt",
    12: "3.txt",
    18: "4.txt"
}

DATA_FOLDER = "data"
# ----------------------------

def get_next_schedule():
    """Calcola la prossima esecuzione programmata."""
    now = datetime.datetime.now()
    target_hours = sorted(SCHEDULE.keys())
    
    next_run = None
    file_to_use = None

    # Cerca un orario oggi che sia nel futuro
    for h in target_hours:
        candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate > now:
            next_run = candidate
            file_to_use = SCHEDULE[h]
            break
    
    # Se non c'è più nulla oggi (es. sono le 20:00), prendi il primo di domani
    if next_run is None:
        next_day = now + datetime.timedelta(days=1)
        first_hour = target_hours[0]
        next_run = next_day.replace(hour=first_hour, minute=0, second=0, microsecond=0)
        file_to_use = SCHEDULE[first_hour]

    return next_run, file_to_use

def main():
    print(f"--- ORCHESTRATORE SCHEDULATO ---")
    adesso = datetime.datetime.now()
    print(f"🕒 ORA SERVER RILEVATA: {adesso.strftime('%H:%M:%S')}")
    print(f"Orari target: {list(SCHEDULE.keys())}")
    print(f"Orari target: {list(SCHEDULE.keys())}")
    print(f"Durata per sessione: {DURATA_ACQUISIZIONE} minuti")
    print("-" * 40)

    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    while True:
        try:
            # 1. Calcola quando partire
            next_run, filename = get_next_schedule()
            now = datetime.datetime.now()
            wait_seconds = (next_run - now).total_seconds()
            
            # Formattazione per stampa
            hours, remainder = divmod(wait_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            print(f"\n[{now.strftime('%H:%M:%S')}] Prossimo avvio: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"💤 In attesa per {int(hours)}h {int(minutes)}m {int(seconds)}s... (File target: {filename})")

            # 2. Dormi fino all'ora X
            time.sleep(wait_seconds)

            # 3. È l'ora! Esegui lo script
            print(f"\n⏰ DRING! Sono le {next_run.strftime('%H:%M')}. Avvio raccolta su {filename}...")
            

            cmd = [
                sys.executable, 
                "ricerca_real_time.py", 
                str(DURATA_ACQUISIZIONE), 
                filename
            ]
            
            subprocess.run(cmd, check=True)
            
            print(f"✅ Sessione delle {next_run.hour}:00 completata.")
            
            # Piccola pausa di sicurezza per evitare di ripartire nello stesso secondo
            time.sleep(60)

        except KeyboardInterrupt:
            print("\n🛑 Orchestrator interrotto manualmente.")
            break
        except Exception as e:
            print(f"❌ ERRORE IMPREVISTO: {e}")
            print("Riprovo tra 1 minuto...")
            time.sleep(60)

if __name__ == "__main__":
    main()