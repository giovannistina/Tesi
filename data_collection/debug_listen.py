import cbor2
from atproto import FirehoseSubscribeReposClient

def main():
    print("--- DIAGNOSTICA DATI ---")
    print("In attesa del primo messaggio dalla rete...")

    def custom_tag_hook(decoder, tag, shareable_index=None):
        return str(tag.value)

    def on_message_handler(message) -> None:
        try:
            # Decodifica il corpo
            data = cbor2.loads(message.body, tag_hook=custom_tag_hook)
            
            # STAMPA TUTTO QUELLO CHE TROVI
            print("\n--- MESSAGGIO RICEVUTO! ---")
            print(f"Chiavi trovate: {list(data.keys())}")
            print(f"Contenuto (prime righe): {str(data)[:200]}...")
            
            # Verifica se c'è 'repo'
            if 'repo' in data:
                print(f"✅ TROVATO 'repo': {data['repo']}")
            else:
                print("❌ 'repo' NON TROVATO in questo messaggio.")
                
            client.stop()
            
        except Exception as e:
            print(f"Errore lettura: {e}")

    client = FirehoseSubscribeReposClient()
    client.start(on_message_handler)

if __name__ == '__main__':
    main()