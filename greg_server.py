from flask import Flask, request, jsonify
from flask_cors import CORS
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import logging
from greg_engine import greg_recommend

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

geolocator = Nominatim(user_agent="greg_lumen_app")
geocode_cache = {}


def geocode_address_granular(strada=None, ncivico=None, citta=None, provincia=None, cap=None, country="US", retry=3):
    cache_key = f"{strada}_{ncivico}_{citta}_{provincia}_{cap}_{country}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]

    query_parts = []

    if ncivico and strada:
        query_parts.append(f"{ncivico} {strada}")
    elif strada:
        query_parts.append(strada)

    if citta:
        query_parts.append(citta)

    if provincia:
        query_parts.append(provincia)

    if cap:
        query_parts.append(cap)

    if country:
        query_parts.append(country)

    full_query = ", ".join(query_parts)

    if not full_query:
        return None, None

    for attempt in range(retry):
        try:
            logger.info(f"Geocoding attempt {attempt + 1}: {full_query}")
            location = geolocator.geocode(full_query, timeout=10)

            if location:
                coords = (location.latitude, location.longitude)
                geocode_cache[cache_key] = coords
                logger.info(f"Geocoding SUCCESS: {coords}")
                return coords
            else:
                if ncivico and attempt == 0:
                    logger.info("Fallback: riprovo senza numero civico")
                    return geocode_address_granular(strada, None, citta, provincia, cap, country, retry=1)

        except Exception as e:
            logger.error(f"Geocoding error: {e}")
            time.sleep(1)

    return None, None


@app.route("/ricercaGeografica", methods=['POST'])
def match_volunteers():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Body mancante'}), 400

        strada = data.get('strada')
        ncivico = data.get('nCivico')
        citta = data.get('citta')
        provincia = data.get('provincia')
        cap = data.get('cap')

        # Ora category è opzionale, se è None o vuoto va bene lo stesso
        category = data.get('category')
        subcategory = data.get('subcategory')

        # Validazione minima geografica (Serve sempre almeno una zona)
        if not (cap or citta):
            return jsonify({'error': 'Almeno CAP o Città obbligatori'}), 400

        logger.info(f"Richiesta granulare: {strada} {ncivico}, {citta} ({provincia}) {cap} [Cat: {category}]")

        lat, lon = geocode_address_granular(
            strada=strada,
            ncivico=ncivico,
            citta=citta,
            provincia=provincia,
            cap=cap
        )

        if lat is None or lon is None:
            if cap:
                lat, lon = geocode_address_granular(cap=cap)

            if lat is None or lon is None:
                return jsonify({
                    'error': 'Indirizzo non trovato',
                    'volunteerEmails': []
                }), 404

        # Passiamo category (anche se None) al motore
        results = greg_recommend(lat, lon, category, subcategory, top_k=5)

        if not results or not isinstance(results, list):
            return jsonify({'volunteerEmails': []}), 200

        email_list = [r.get('email', '') for r in results if r.get('email')]

        return jsonify({
            'volunteerEmails': email_list,
            'debug_coords': {'lat': lat, 'lon': lon}
        }), 200

    except Exception as e:
        logger.error(f"Errore server: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
