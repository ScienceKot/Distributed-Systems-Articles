# Importing all needed modules.
from flask import Flask, request
from pymemcache.client import base

# Defining the Memcached clients.
memcache_client1 = base.Client(("localhost", 11211))
memcache_client2 = base.Client(("localhost", 11212))
memcache_client3 = base.Client(("localhost", 11213))

# Defining the Cache Ring.
HASH_RING = {
    0 : memcache_client1,
    120 : memcache_client2,
    240 : memcache_client3
}


def find_memcache_service(request_body : dict) -> int:
    '''
        This function returns based on the request body the responsible service for this request.
    :param request_body: dict
        The content of the request.
    :return: int
        The index of the Memcached service responsible of the request.
    '''
    chosen_service_index = 0
    min_difference = 360

    # Extracting the hash of the user id and calculating the hash mode 360.
    hash_payload = int(hash(request_body["user_id"]))
    hash_mod_360 = hash_payload % 360

    # Finding the degree with the lowest distance from hash mode 360.
    for index in HASH_RING:
        # Calculating the difference between the degree and hash mod 360.
        difference = abs(hash_mod_360 - index)
        if difference < min_difference:
            # Updating the nearest degree.
            min_difference = difference
            chosen_service_index = index
    return chosen_service_index

# Creating the Flask application.
app = Flask(__name__)


@app.route("/save", methods=["POST"])
def save():
    '''
        This endpoint processes the save requests.
    '''
    # Extracting the request body.
    request_body = request.json

    # Getting the index (degree) of the responsible Memcached service.
    memcache_index = find_memcache_service(request_body)

    # Sending the request to the Memcached service.
    HASH_RING[memcache_index].set(request_body["user_id"],
                                  request_body,
                                  expire=30)
    return {
        "message" : "Saved!"
    }, 200

@app.route("/cache", methods=["GET"])
def cache():
    '''
        This endpoint processes the caching requests.
    '''
    # Extracting the request body.
    request_body = request.json

    # Getting the index (degree) of the responsible Memcached service.
    memcache_index = find_memcache_service(request_body)

    # Getting the cached value from the responsible Memcached service.
    cached_value = HASH_RING[memcache_index].get(request_body["user_id"])

    # Returning the requested value or the error message.
    if cached_value:
        return cached_value, 200
    else:
        return {
            "message" : "No such data"
        }, 404

# Running the main service.
app.run()