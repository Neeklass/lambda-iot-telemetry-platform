from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import logging
import random

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MACHINE_IDS = ['ROTOR_01', 'ROTOR_02', 'ROTOR_03', 'TURBINE_01', 'TURBINE_02', 'PUMP_01', 'PUMP_02']


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "iot-serving-api"}), 200


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    try:
        limit = int(request.args.get('limit', 100))
        
        alerts = []
        for i in range(min(limit, 20)):
            alerts.append({
                "timestamp": int((datetime.now() - timedelta(minutes=i)).timestamp()),
                "machine_id": random.choice(MACHINE_IDS),
                "temperature": round(random.uniform(80.5, 89.5), 2)
            })
        
        return jsonify({"alerts": alerts, "count": len(alerts)}), 200
        
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Alerts: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/metrics/realtime', methods=['GET'])
def get_realtime_metrics():
    """Gibt Echtzeit-Metriken zurück (Speed Layer Mock)."""
        metrics = {
            "total_alerts": random.randint(30, 50),
            "machines_critical": random.sample(MACHINE_IDS, k=random.randint(1, 3)),
            "avg_temperature": round(random.uniform(65, 75), 1),
            "max_temperature": round(random.uniform(85, 92), 1),
            "timestamp": datetime.now().isoformat()
        }
        return jsonify(metrics), 200
    except Exception as e:
        logger.error(f"Fehler bei Echtzeit-Metriken: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/batch/trends', methods=['GET'])
def get_batch_trends():
    """Gibt historische Trends aus dem Batch Layer zurück (Mock)."""
        days = int(request.args.get('days', 7))
        trends = {
            "daily_avg": [
                {"date": (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d"), 
                 "avg_temp": round(random.uniform(60, 75), 1)}
                for i in range(days)
            ]
        }
        return jsonify(trends), 200
    except Exception as e:
        logger.error(f"Fehler bei Batch-Trends: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/grafana/query', methods=['POST'])
def grafana_query():
    """
        data = request.get_json()
        target = data.get('targets', [{}])[0]
        query_type = target.get('target', 'alerts')
        
        # Zeitbereich aus Grafana-Request
        time_from = data.get('range', {}).get('from')
        time_to = data.get('range', {}).get('to')
        
        # Mock-Response für Grafana
        time_from = data.get('range', {}).get('from')
        time_to = data.get('range', {}).get('to')
        time.now().timestamp() * 1000)],
                    [82.1, int((datetime.now() - timedelta(minutes=1)).timestamp() * 1000)]
                ]
            }
        ]
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Fehler bei Grafana Query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/grafana/search', methods=['POST'])
def grafana_search():
    """Grafana Search Endpoint - Verfügbare Metriken"""
    metrics = ["alerts", "temperature", "machine_status", "batch_trends"]
    return jsonify(metrics), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
