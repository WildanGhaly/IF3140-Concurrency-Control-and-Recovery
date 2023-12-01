from flask import Flask, jsonify, request
from flask_cors import CORS
from OCC import OCC
from TwoPhaseLocking import TwoPhaseLocking
from MVCC import MVCC, parse_input


app = Flask(__name__)
CORS(app)

# Define OCC algorithm route handler
@app.route('/occ', methods=['POST'])
def run_occ():
        data = request.get_json()
        input_seq = data.get('input_seq', '')
        try:
            occ = OCC(input_seq)
            occ.run()
            result = str(occ)
            return jsonify({'result': result}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 400

# Define Two Phase Locking algorithm route handler
@app.route('/twophase', methods=['POST'])
def process_sequence():
        try:
            data = request.json
            input_seq = data.get('input_seq')
            lock = TwoPhaseLocking(input_seq)
            lock.run()
            result_string = lock.result_string()
            return jsonify({'result': result_string})
        except Exception as e:
            return jsonify({'error': str(e)})
    

@app.route('/mvcc', methods=['POST'])
def process_mvcc():
        try:
            data = request.json
            input_seq = data.get('input_seq')
            sequence = parse_input(input_seq)
            lock = MVCC(sequence)
            lock.run()
            result_string = lock.result_string
            print(result_string)
            return jsonify({'result': result_string})
        except Exception as e:
            return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)