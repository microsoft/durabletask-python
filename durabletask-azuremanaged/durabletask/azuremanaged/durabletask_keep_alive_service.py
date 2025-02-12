import threading
import time
import requests  # You could use grpc or another library depending on your setup

class KeepAliveService:
    def __init__(self, interval: int = 60, endpoint: str = "https://sdktest1-fgcac9hja3f8.northcentralus.durabletask.io"):
        self.interval = interval  # Time interval in seconds
        self.endpoint = endpoint  # The endpoint for sending no-op requests
        self._keep_alive_thread = threading.Thread(target=self._send_noop_periodically)
        self._keep_alive_thread.daemon = True  # Makes sure it ends when the main program ends
        self._keep_alive_thread.start()

    def _send_noop_periodically(self):
        while True:
            try:
                # Send a simple GET or POST request to a "ping" or no-op endpoint
                response = requests.get(self.endpoint)  # Replace with the appropriate method
                if response.status_code == 200:
                    print("No-op request sent successfully.")
                else:
                    print(f"No-op failed with status code {response.status_code}")
            except Exception as e:
                print(f"Error sending no-op: {e}")
            
            time.sleep(self.interval)  # Wait before sending another no-op

# Example Usage
keep_alive_service = KeepAliveService(interval=60, endpoint="https://sdktest1-fgcac9hja3f8.northcentralus.durabletask.io")
