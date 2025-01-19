import json
from flask import Flask, Response, request, jsonify, render_template
import time

app = Flask(__name__)

data_store = []

@app.route('/ingest', methods=['POST'])
def ingest():
    global data_store
    message = request.json
    print(f"Received data: {message}")
    data_store.append(message)
    return jsonify({"status": "success"}), 200

@app.route('/stream')
def stream():
    def event_stream():
        while True:
            if data_store:
                # Send only the raw JSON message
                message = data_store.pop(0)
                yield f"data: {json.dumps(message)}\n\n"
            time.sleep(1)  # Adjust delay if needed
    return Response(event_stream(), content_type="text/event-stream")

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
