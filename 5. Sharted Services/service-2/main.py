# Importing all needed services.
from flask import Flask, request
from crud import CRUDUser

# Defining the service credentials.
service_info = {
    "host" : "127.0.0.1",
    "port" : 8001,
    "leader" : False,
    "degree" : 240
}

# Creating the CRUD functionalities.
crud = CRUDUser(service_info["leader"], service_info["degree"])

# Creating the main flask application.
app = Flask(__name__)

@app.route("/user", methods = ["POST"])
def add_user():
    # Getting the headers of the request.
    headers = dict(request.headers)

    # Checking if the request should be processed or not.
    if not crud.leader and ("Token" not in headers or headers["Token"] != "Leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Adding the new user the data store.
        return_dict, status_code = crud.create(request.json)
        return return_dict, status_code

@app.route("/user", methods=["GET"])
def get_users():
    # Extracting the users list.
    user_list = request.json["users_id"]

    # Getting the users' data from the data store.
    return_dict, status_code = crud.get_users(user_list)
    return return_dict, status_code

@app.route("/user/<index>", methods=["GET"])
def get_user(index):
    # Getting the user from the data store.
    return_dict, status_code = crud.get_user(index)
    return return_dict, status_code

@app.route("/user/<index>", methods=["PUT"])
def update_user(index):
    # Extracting the headers from the request.
    headers = dict(request.headers)

    # Checking if the request should be processed or not.
    if not crud.leader and ("Token" not in headers or headers["Token"] != "Leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Extracting the new field values for the user.
        new_user_data = request.json

        # Updating the user's fields.
        return_dict, status_code = crud.update_user(index, new_user_data)
        return return_dict, status_code

@app.route("/user/<index>", methods=["DELETE"])
def delete_user(index):
    # Extracting the headers from the request.
    headers = dict(request.headers)

    # Checking if the request should be processed or not.
    if not crud.leader and ("Token" not in headers or headers["Token"] != "Leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Deleting the user from the data store.
        return_dict, status_code = crud.delete_user(index)
        return return_dict, status_code

# Running the web application.
app.run(
    host = service_info["host"],
    port = service_info["port"]
)