import json
from flask import Flask, Response, request, jsonify, render_template
import time

app = Flask(__name__)

data_store = []


@app.route('/ingest', methods=['POST'])
def ingest():
    """
    Endpoint to receive data (from Spark or other sources) and store it in memory.
    """
    global data_store
    message = request.json
    print(f"Received data: {message}")

    # Add the received message to the data store
    data_store.append(message)

    return jsonify({"status": "success"}), 200

@app.route('/stream')
def stream():
    """
    Server-Sent Events (SSE) endpoint for streaming data to the frontend.
    """
    def event_stream():
        while True:
            if data_store:
                # Get the next message from the store and stream it
                message = data_store.pop(0)
                yield f"data: {json.dumps(message)}\n\n"
            time.sleep(1)  # Adjust delay if needed for streaming frequency
    return Response(event_stream(), content_type="text/event-stream")

@app.route('/')
def index():
    return render_template('streaming_map.html')

# @app.route('/')
# def home():
#     return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
