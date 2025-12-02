from atproto import Client
import getpass
import os

def main():
    print("Starting session creation...")
    client = Client()
    
    # Prompt for credentials
    username = input("USERNAME (e.g. name.bsky.social): ")
    password = getpass.getpass("PASSWORD: ")

    print("Attempting to log in...")
    
    try:
        # Perform login
        client.login(username, password)
        print("Login successful!")

        # NEW METHOD: export the session string correctly
        session_string = client.export_session_string()

        # Write to session.txt
        with open("session.txt", "w") as f:
            f.write(session_string)

        print("SUCCESS! 'session.txt' has been created in the current folder.")
        print("You can now run the other scripts.")
        print("DO NOT commit 'session.txt' to Git/GitHub as it contains your credentials.")

    except Exception as e:
        print("\nERROR DURING LOGIN:")
        print(e)
        print("Please check that your username and password are correct.")

if __name__ == "__main__":
    main()