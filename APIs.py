def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

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

        # Ensure rider exists
        cur.execute("SELECT 1 FROM Riders WHERE rider_id = %s FOR SHARE", (rider_id,))
        if cur.fetchone() is None:
            raise Exception("Invalid rider ID")

        # Create ride request
        cur.execute('''
            INSERT INTO RideRequests (rider_id, pickup_location_id, dropoff_location_id, status)
            VALUES (%s, %s, %s, 'requested')
            RETURNING request_id, status
        ''', (rider_id, pickup_location_id, dropoff_location_id))

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

        # Update request status
        cur.execute("UPDATE RideRequests SET status = 'accepted' WHERE request_id = %s", (request_id,))

        # Insert into Rides
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