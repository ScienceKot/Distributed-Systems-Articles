# Importing all needed modules.
from flask import Flask, request
from crud import CRUDUser


# Defining the service information.
service_info = {
    "host" : "127.0.0.1",
    "port" : 8000,
    "leader" : True
}

# Defining the followers information.
followers = [
    {
        "host" : "127.0.0.1",
        "port" : 8001
    },
    {
        "host" : "127.0.0.1",
        "port" : 8002
    }
]

# Creating the data store.
crud = CRUDUser(service_info["leader"], followers)

# Creating the flask application.
app = Flask(__name__)


@app.route("/user", methods = ["POST"])
def add_user():
    '''
        This function handles the create requests.
    '''
    # Extracting the headers and checking if request can be processes.
    headers = dict(request.headers)
    if not crud.leader and ("Token" not in headers or headers["Token"] != "leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Trying to create a user in the data store.
        return_dict, status_code = crud.create(request.json)
        return return_dict, status_code


@app.route("/user", methods=["GET"])
def get_users():
    '''
        This function handles the read some requests.
    '''
    # Getting the users ids.
    user_list = request.json["users_id"]
    # Getting the response and status code from the tha data store.
    return_dict, status_code = crud.get_users(user_list)
    return return_dict, status_code


@app.route("/user/<index>", methods=["GET"])
def get_user(index):
    '''
        THis function handles the read one requests.
    '''
    # Getting the response and the status code from the data store.
    return_dict, status_code = crud.get_user(index)
    return return_dict, status_code


@app.route("/user/<index>", methods=["PUT"])
def update_user(index):
    '''
        This function handles the update requests.
    '''
    # Extracting the headers and checking if request can be processes.
    headers = dict(request.headers)
    if not crud.leader and ("Token" not in headers or headers["Token"] != "leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Getting the new credentials from the request.
        new_user_data = request.json
        # Trying to update the user's information and getting the response and status code from the data store.
        return_dict, status_code = crud.update_user(index, new_user_data)
        return return_dict, status_code


@app.route("/user/<index>", methods=["DELETE"])
def delete_user(index):
    '''
        This function handles the delete requests.
    '''
    # Extracting the headers and checking if request can be processes.
    headers = dict(request.headers)
    if not crud.leader and ("Token" not in headers or headers["Token"] != "leader"):
        return {
            "message" : "Access denied!"
        }, 403
    else:
        # Trying to delete the user and getting the response and status code from data store.
        return_dict, status_code = crud.delete_user(index)
        return return_dict, status_code


# Running the flask application.
app.run(
    host = service_info["host"],
    port = service_info["port"]
)