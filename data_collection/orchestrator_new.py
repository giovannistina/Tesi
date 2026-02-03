# orchestrator_new.py
import subprocess
import sys
import time
import datetime
import os

# --- CONFIGURAZIONE COMANDI ---
# NOTA: Ora i comandi di scaricamento sono espliciti: "30 11.txt"
# Significa: Scarica per 30 minuti nel file 11.txt

TASKS = [
    # --- BLOCCO NOTTE (Chunk 11) ---
    {
        "time": "00:30",
        # CORRETTO: Durata 30 min, File 11.txt
        "cmd": f"nohup {sys.executable} -u ricerca_real_time.py 30 11.txt > logs/log_real_time_11.txt 2>&1 &"
    },
    {
        "time": "01:10",
        "cmd": 'nohup python -u crawl_timelines_limited.py 11 "giovanni-stina.bsky.social" "Compasso1!" > logs/log_crawl_11.txt 2>&1 &'
    },

    # --- BLOCCO MATTINA (Chunk 12) ---
    {
        "time": "06:30",
        # CORRETTO: Durata 30 min, File 12.txt
        "cmd": f"nohup {sys.executable} -u ricerca_real_time.py 30 12.txt > logs/log_real_time_12.txt 2>&1 &"
    },
    {
        "time": "07:10",
        "cmd": f"nohup {sys.executable} -u cleaner.py 12.txt > logs/log_cleaner_12.txt 2>&1 &"
    },
    {
        "time": "07:15",
        "cmd": 'nohup python -u crawl_timelines_limited.py 12 "isa-stina.bsky.social" "Cucciolo1!" > logs/log_crawl_12.txt 2>&1 &'
    },

    # --- BLOCCO PRANZO (Chunk 13) ---
    {
        "time": "12:30",
        # CORRETTO: Durata 30 min, File 13.txt
        "cmd": f"nohup {sys.executable} -u ricerca_real_time.py 30 13.txt > logs/log_real_time_13.txt 2>&1 &"
    },
    {
        "time": "13:10",
        "cmd": f"nohup {sys.executable} -u cleaner.py 13.txt > logs/log_cleaner_13.txt 2>&1 &"
    },
    {
        "time": "13:15",
        "cmd": 'nohup python -u crawl_timelines_limited.py 13 "gio-stina.bsky.social" "Giovanni1!" > logs/log_crawl_13.txt 2>&1 &'
    },

    # --- BLOCCO SERA (Chunk 14) ---
    {
        "time": "18:30",
        # CORRETTO: Durata 30 min, File 14.txt
        "cmd": f"nohup {sys.executable} -u ricerca_real_time.py 30 14.txt > logs/log_real_time_14.txt 2>&1 &"
    },
    {
        "time": "19:10",
        "cmd": f"nohup {sys.executable} -u cleaner.py 14.txt > logs/log_cleaner_14.txt 2>&1 &"
    },
    {
        "time": "19:15",
        "cmd": 'nohup python -u crawl_timelines_limited.py 14 "stinag2000.bsky.social" "Giovanni2!" > logs/log_crawl_14.txt 2>&1 &'
    },

    # --- FASI FINALI ---
    {
        "time": "19:30",
        "cmd": f"nohup {sys.executable} -u unisci_utenti.py > logs/log_unisci_utenti.txt 2>&1 &"
    },
    {
        "time": "20:00",
        "cmd": f"nohup {sys.executable} -u scarica_followers.py all_users.txt \"f-stina.bsky.social\" \"Francesco1!\" > logs/log_scarica_followers.txt 2>&1 &"
    },
    
    # --- AUTODISTRUZIONE ---
    {
        "time": "20:05",
        "cmd": "STOP_ORCHESTRATOR"
    }
]

def get_next_task_time(target_time_str):
    """Calcola il timestamp futuro per l'orario richiesto"""
    now = datetime.datetime.now()
    h, m = map(int, target_time_str.split(':'))
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return target

def main():
    print("--- ORCHESTRATOR CORRETTO (30min + Autodistruzione) ---")
    os.makedirs("logs", exist_ok=True)

    while True:
        # 1. Trova il prossimo task
        now = datetime.datetime.now()
        next_task = None
        min_wait = float('inf')

        for task in TASKS:
            target_dt = get_next_task_time(task["time"])
            wait_seconds = (target_dt - now).total_seconds()
            
            if wait_seconds < min_wait:
                min_wait = wait_seconds
                next_task = task
                next_task_dt = target_dt

        # 2. Attendi
        hours = int(min_wait // 3600)
        mins = int((min_wait % 3600) // 60)
        secs = int(min_wait % 60)
        
        # Anteprima comando
        if next_task['cmd'] == "STOP_ORCHESTRATOR":
            cmd_preview = "🛑 SPEGNIMENTO AUTOMATICO"
        else:
            # Pulisce la stringa per mostrarla a video
            cmd_preview = next_task['cmd'].split('>')[0].replace("nohup", "").strip()

        print("-" * 50)
        print(f"🕒 Ore: {now.strftime('%H:%M:%S')}")
        print(f"⏭️  Prossimo: {cmd_preview}...") 
        print(f"📅 Alle ore: {next_task['time']}")
        print(f"💤 Attesa: {hours}h {mins}m {secs}s...")
        
        time.sleep(min_wait)

        # 3. Esegui
        print(f"\n🚀 ESECUZIONE: {next_task['time']}")
        
        if next_task["cmd"] == "STOP_ORCHESTRATOR":
            print("\n" + "="*50)
            print("🛑 ORE 20:00 - FINE GIORNATA.")
            print("L'Orchestrator si chiude. I processi attivi continueranno.")
            print("="*50 + "\n")
            sys.exit(0)

        try:
            subprocess.Popen(next_task["cmd"], shell=True)
            print("✅ Comando inviato.")
        except Exception as e:
            print(f"❌ ERRORE nel lancio: {e}")
        
        time.sleep(65)

if __name__ == "__main__":
    main()