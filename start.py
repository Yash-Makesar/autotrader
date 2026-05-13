"""
Start both the bot and dashboard together.
  python start.py
Press Ctrl+C to stop both.
"""

import subprocess, sys, threading, time

BOT  = [sys.executable, "gemini_paper_bot.py"]
DASH = [sys.executable, "dashboard.py"]

def stream(proc, prefix):
    for line in proc.stdout:
        print(f"{prefix} {line}", end="", flush=True)

def main():
    print("Starting Gold Bot + Dashboard…")
    print("Dashboard → http://localhost:5001")
    print("Press Ctrl+C to stop both.\n")

    bot  = subprocess.Popen(BOT,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    time.sleep(1)                 # let bot init before dashboard imports it
    dash = subprocess.Popen(DASH, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    threading.Thread(target=stream, args=(bot,  "\033[33m[BOT] \033[0m"), daemon=True).start()
    threading.Thread(target=stream, args=(dash, "\033[34m[DASH]\033[0m"), daemon=True).start()

    try:
        while bot.poll() is None and dash.poll() is None:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping…")
    finally:
        bot.terminate()
        dash.terminate()
        bot.wait()
        dash.wait()
        print("Both processes stopped.")

if __name__ == "__main__":
    main()
