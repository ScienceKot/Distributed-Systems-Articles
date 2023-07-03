# Importing all needed modules.
import uuid
import requests


class CRUDUser:
    def __init__(self, leader : bool, followers : dict =None):
        '''
            The constructor of the CrudUser.
        :param leader: bool
            The parameter deciding if the service is a leader or not.
        :param followers: dict, default = None
            The dictionary containing the credentials of the services.
        '''
        self.users = {}
        self.leader = leader
        if self.leader:
            self.followers = followers

    def create(self, user_dict : dict):
        '''
            The create function from CRUD, This function adds a service to the datstore.
        :param user_dict: dict
            The dictionary containing the information about users.
        :return: dict, int
        '   The response and the status code.
        '''
        # If the service is a leader the service adds the user to the data store.
        if self.leader:
            index = str(uuid.uuid4())
            user_dict["id"] = index
        if user_dict["id"] not in self.users:
            # If the service is a follower the service and the user is not registered
            # the user is added to the data store.
            self.users[user_dict["id"]] = user_dict

            # If the service is a leader then forwards the request to the followers.
            if self.leader:
                for follower in self.followers:
                    requests.post(f"http://{follower['host']}:{follower['port']}/user",
                                  json = user_dict,
                                  headers = {"Token" : "Leader"})

            # Returning the response.
            return user_dict, 200
        else:
            return {
                       "message" : "Error: User already exists!"
                   }, 400

    def get_user(self, index : str):
        '''
            This function returns a user by the index from the data store..
        :param index: str
            The index of the requested user.
        :return: dict, int
        '   The response and the status code.
        '''
        # Return the user's information if it exists.
        if index in self.users:
            return self.users[index], 200
        else:
            # Return the error message if user doesn't exists.
            return {
                       "message" : "Missing user!"
                   }, 404

    def get_users(self, user_id_list : list):
        '''
            This function returns a list of users from the data store.
        :param user_id_list: list
            The list with user ids.
        :return: dict, int
        '   The response and the status code.
        '''
        # Fining the set of missing ids from the list.
        missing_ids = set(user_id_list).difference(list(self.users.keys()))

        # If there are missing ids then an error code is returned.
        if len(missing_ids) > 0:
            return {
                       "message" : "Missing ids!",
                       "ids" : list(missing_ids)
                   }, 404
        else:
            # If all ids are present in the data store a list with user credentials are returned.
            user_info = {index : self.users[index] for index in user_id_list}
            return user_info, 200

    def update_user(self, index : str, user_dict : dict):
        '''
            This function updates the user's information with the provided information.
        :param index: str
            The index of the user in the data store.
        :param user_dict: dict
            The new users information.
        :return: dict, int
        '   The response and the status code.
        '''
        # Checking if the user id is registered.
        if index in self.users:
            # Updating the user's information.
            self.users[index].update(user_dict)

            # If the service is the leader the it forward the information to the followers.
            if self.leader:
                for follower in self.followers:
                    requests.put(f"http://{follower['host']}:{follower['port']}/user/{index}",
                                 json = self.users[index],
                                 headers = {"Token" : "Leader"})

            # Returning the response and the status code.
            return self.users[index], 200
        else:
            return {
                       "message" : "Missing user!"
                   }, 404

    def delete_user(self, index : str):
        '''
            This function deletes a user from the data store by index.
        :param index: str
            The index of the user in the data store.
        :return: dict, int
        '   The response and the status code.
        '''
        # Checking if the user id is registered.
        if index in self.users:
            # Deleting the user from the data store..
            user_dict = self.users.pop(index)

            # If the service is the leader the it forward the request to the followers.
            if self.leader:
                for follower in self.followers:
                    requests.delete(f"https://{follower['host']}:{follower['port']}/user/{index}",
                                    headers = {"Token" : "Leader"})

            # Returning the response and the status code.
            return user_dict, 200
        else:
            return {
                       "message" : "Missing user!"
                   }, 404