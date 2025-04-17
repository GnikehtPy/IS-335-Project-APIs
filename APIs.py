from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import os

app = Flask(__name__)

# database connection
DB_PARAMS = {
    'dbname': 'rideshare_db',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}

GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')


def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def get_travel_estimate(pickup_address, dropoff_address):
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        'origin': pickup_address,
        'destination': dropoff_address,
        'key': GOOGLE_MAPS_API_KEY,
        'departure_time': 'now'
    }
    response = requests.get(url, params=params)
    data = response.json()
    if data['status'] == 'OK':
        leg = data['routes'][0]['legs'][0]
        distance_meters = leg['distance']['value']
        duration_seconds = leg['duration_in_traffic']['value']
        return distance_meters / 1000.0, duration_seconds / 60.0  # km and minutes
    else:
        raise Exception(f"Google Maps API error: {data['status']}")

@app.route('/ride-request', methods=['POST'])
def ride_request():
    data = request.json
    rider_id = data['rider_id']
    pickup_location_id = data['pickup_location_id']
    dropoff_location_id = data['dropoff_location_id']

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        conn.autocommit = False

        # make sure rider exists
        cur.execute("SELECT * FROM Riders WHERE rider_id = %s FOR SHARE", (rider_id,))
        if cur.fetchone() is None:
            raise Exception("Invalid rider ID")

        # Get pickup and dropoff addresses
        cur.execute("SELECT address FROM Locations WHERE location_id = %s", (pickup_location_id,))
        pickup_address = cur.fetchone()['address']
        cur.execute("SELECT address FROM Locations WHERE location_id = %s", (dropoff_location_id,))
        dropoff_address = cur.fetchone()['address']

        distance_km, duration_min = get_travel_estimate(pickup_address, dropoff_address)
        base_rate = 5.0
        price_per_km = 2.0
        estimated_price = base_rate + (price_per_km * distance_km)

        # Create ride request
        cur.execute('''
            INSERT INTO RideRequests (rider_id, pickup_location_id, dropoff_location_id, status, estimated_price, estimated_time)
            VALUES (%s, %s, %s, 'requested', %s, %s)
            RETURNING request_id, status, estimated_price, estimated_time
        ''', (rider_id, pickup_location_id, dropoff_location_id, estimated_price, int(duration_min)))

        result = cur.fetchone()
        conn.commit()
        return jsonify(result), 201

    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400

    finally:
        cur.close()
        conn.close()

@app.route('/ride-accept', methods=['POST'])
def ride_accept():
    data = request.json
    driver_id = data['driver_id']
    request_id = data['request_id']

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        conn.autocommit = False

        # Lock ride request
        cur.execute("""
            SELECT * FROM RideRequests
            WHERE request_id = %s AND status = 'requested'
            FOR UPDATE
        """, (request_id,))
        request_row = cur.fetchone()
        if not request_row:
            raise Exception("Invalid or already accepted request")

        # Lock driver
        cur.execute("SELECT * FROM Drivers WHERE driver_id = %s AND status = 'online' FOR UPDATE", (driver_id,))
        driver_row = cur.fetchone()
        if not driver_row:
            raise Exception("Driver unavailable or not online")

        # change request's status
        cur.execute("UPDATE RideRequests SET status = 'accepted' WHERE request_id = %s", (request_id,))

        # insert into Rides
        cur.execute('''
            INSERT INTO Rides (driver_id, rider_id, pickup_location_id, dropoff_location_id, start_time, status)
            VALUES (%s, %s, %s, %s, NOW(), 'in_progress')
            RETURNING ride_id, status
        ''', (
            driver_id,
            request_row['rider_id'],
            request_row['pickup_location_id'],
            request_row['dropoff_location_id']
        ))

        ride = cur.fetchone()
        conn.commit()
        return jsonify(ride), 201

    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400

    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)
