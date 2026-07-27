import sys
import os
import time
import subprocess
import threading
import re
import webbrowser

print("=" * 75)
print("             TRADE HUB PRO - ONLINE TUNNEL LAUNCHER")
print("=" * 75)

base_dir = os.path.dirname(os.path.abspath(__file__))

# 1. Start Python Flask backend algo.py in a subprocess
print("\n[1/3] Starting Flask Trade Engine Server on http://127.0.0.1:5000...")
backend_proc = subprocess.Popen(
    [sys.executable, os.path.join(base_dir, "algo.py")],
    cwd=base_dir,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1
)

# Stream backend logs to console in background
def stream_backend_output(proc):
    for line in iter(proc.stdout.readline, ''):
        if not line:
            break
        text = line.strip()
        if any(k in text for k in ["[SUCCESS]", "[ERROR]", "[WARNING]", "Serving Flask", "Running on"]):
            print(f" [ENGINE] {text}")

threading.Thread(target=stream_backend_output, args=(backend_proc,), daemon=True).start()

# Wait 2 seconds for backend to start listening
time.sleep(2.0)

# 2. Launch Cloudflare Tunnel
print("\n[2/3] Launching Secure Cloudflare HTTPS Tunnel...")
cloudflared_exe = os.path.join(base_dir, "cloudflared.exe")

if not os.path.exists(cloudflared_exe):
    print(f"[ERROR] cloudflared.exe not found at {cloudflared_exe}!")
    print("Defaulting to Localhost mode at http://127.0.0.1:5000...")
    webbrowser.open("http://127.0.0.1:5000/")
    backend_proc.wait()
    sys.exit(1)

tunnel_proc = subprocess.Popen(
    [cloudflared_exe, "tunnel", "--protocol", "http2", "--edge-ip-version", "4", "--url", "http://127.0.0.1:5000"],
    cwd=base_dir,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1
)

tunnel_url = None

def find_tunnel_url(proc):
    global tunnel_url
    url_pattern = re.compile(r'https://[a-zA-Z0-9-]+\.trycloudflare\.com')
    for line in iter(proc.stdout.readline, ''):
        if not line:
            break
        match = url_pattern.search(line)
        if match:
            tunnel_url = match.group(0)
            break

url_thread = threading.Thread(target=find_tunnel_url, args=(tunnel_proc,), daemon=True)
url_thread.start()

# Wait up to 15 seconds for Cloudflare URL to be generated
print("[INFO] Establishing secure Cloudflare tunnel...")
for _ in range(30):
    if tunnel_url:
        break
    time.sleep(0.5)

if tunnel_url:
    # Copy to clipboard automatically using PowerShell
    try:
        subprocess.run(["powershell", "-command", f"Set-Clipboard -Value '{tunnel_url}'"], check=False)
        copied_msg = "COPIED TO YOUR CLIPBOARD AUTOMATICALLY! (Use Ctrl+V to paste)"
    except Exception:
        copied_msg = "Please copy the URL manually below."

    print("\n[3/3] Auto-publishing Tunnel URL to GitHub for seamless web access...")
    try:
        with open(os.path.join(base_dir, "backend_url.txt"), "w") as f:
            f.write(tunnel_url)
        subprocess.run(["git", "add", "backend_url.txt"], cwd=base_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "commit", "-m", "Auto-update backend tunnel URL"], cwd=base_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "push", "origin", "main"], cwd=base_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(" [SUCCESS] Tunnel URL successfully published to GitHub!")
    except Exception as e:
        print(f" [WARNING] Failed to auto-publish URL to GitHub: {e}")

    print("\n" + "=" * 75)
    print("                    SUCCESS! ONLINE TUNNEL IS ACTIVE")
    print("=" * 75)
    print(f"\n  YOUR CLOUDFLARE HTTPS TUNNEL URL:\n  >>>  {tunnel_url}  <<<")
    print(f"\n  CLIPBOARD: {copied_msg}")
    print("\n" + "-" * 75)
    print("  ONLINE DASHBOARD URL: https://tradehub.nhtrade.in/")
    print("\n  EASY 2-STEP CONNECT INSTRUCTIONS:")
    print("  1. Browser me 'https://tradehub.nhtrade.in/' open ho gaya hai.")
    print("  2. 'SERVER CONNECT' box me 'Ctrl + V' dabayein aur 'Save & Connect' click karein!")
    print("=" * 75 + "\n")

    # Open tradehub.nhtrade.in in browser
    webbrowser.open("https://tradehub.nhtrade.in/")
else:
    print("\n[WARNING] Cloudflare Tunnel did not output URL in time.")
    print("Opening local server at http://127.0.0.1:5000...")
    webbrowser.open("http://127.0.0.1:5000/")

print("Server is running. Do not close this window while trading.")
print("Press CTRL+C to stop.\n")

try:
    backend_proc.wait()
except KeyboardInterrupt:
    print("\nStopping Trade Hub Pro Engine...")
    backend_proc.terminate()
    tunnel_proc.terminate()
    sys.exit(0)
