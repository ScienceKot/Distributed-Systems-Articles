# Importing all needed modules.
import uuid
import requests

class CRUDUser:
    def __init__(self, leader : bool, degree : int, followers : list = None) -> None:
        '''
            The constructor of the CRUD class.
        :param leader: bool
            True from leader and False for follower.
        :param degree: int
            The degree for which the service is responsible.
        :param followers: list, default = None
            The list with the followers credentials.
        '''
        self.users = {}
        self.leader = leader
        self.deegree = degree
        if self.leader:
            self.followers = followers

    @classmethod
    def get_responsible_degrees(cls, string : str) -> list:
        '''
            This function finds the nearest responsible degrees for a string uuid.
        :param string: str
            The value for with to compute the hash and find the nearest deegrees.
        :return: list
            The list with the nearest degrees for the string uuid.
        '''
        angles_list = [120, 240, 360]
        # Computing the hash of the string.
        hash_str = hash(string)
        # Calculating the hash mod 360
        hash_mod_360 = hash_str % 360
        # Calculating the distance between hash mod 360 and all angles.
        dist_list = [abs(hash_mod_360 - angle) for angle in angles_list]
        # Find the nearest 2 angles with the lowest difference.
        responsible_indexes = sorted(range(len(dist_list)), key=dist_list.__getitem__)
        responsible_angles = [angles_list[responsible_indexes[index]] for index in range(2)]
        return responsible_angles

    def get_selected_followers(self, string : str) -> list:
        '''
            This function return the list of services responsible for the request.
        :param string: str
            The value which we use to compute the hash mode 360.
        :return: list
            The list of responsible services.
        '''
        selected_services = []
        # Getting the responsible angles.
        responsible_angles = self.get_responsible_degrees(string)

        # Adding itself if in responsible degrees is presented the deegree of the service.
        if self.deegree in responsible_angles:
            selected_services.append("me")
            responsible_angles.remove(self.deegree)
        # Adding the rest of responsible services.
        for angle in responsible_angles:
            for follower in self.followers:
                if follower["degree"] in responsible_angles:
                    selected_services.append(follower)
        return selected_services

    def create(self, user_dict : dict):
        '''
            This function adds the new user to the data store and forwards the request to other services if it's the leader.
        :param user_dict: dict
            The dictionary containing the user's information.
        :return: dict, integer
            The response and the status code of the request.
        '''
        resp_followers = []
        # If the service is the leader then we are creating a new uuid for the record and getting the list of
        # responsible services.
        if self.leader:
            index = str(uuid.uuid4())
            user_dict["id"] = index
            resp_followers = self.get_selected_followers(index)
        if user_dict["id"] not in self.users:
            # Checking the conditions for adding the data to the data store.
            if (self.leader and "me" in resp_followers) or not self.leader:
                self.users[user_dict["id"]] = user_dict
                # Removing the "me" from the responsible services list.
                if "me" in resp_followers:
                    resp_followers.remove("me")

            # Forwarding the request to the responsible followers.
            if self.leader:
                for follower in resp_followers:
                    requests.post(f"http://{follower['host']}:{follower['port']}/user",
                                  json = user_dict,
                                  headers = {"Token" : "Leader"})

            return user_dict, 200
        else:
            # Returning the error response if the user already exists.
            return {
                       "message" : "Error: User already exists!"
                   }, 400

    def get_user(self, index : str):
        '''
            This function returns the information
        :param index: str
            The index of the user.
        :return: dict, int
            The response and the status code of the request.
        '''
        if index in self.users:
            return self.users[index], 200
        else:
            return {
                       "message" : "Missing user!"
                   }, 404

    def get_users(self, user_id_list : list):
        '''
            This function returns the data of the selected users.
        :param user_id_list: list
            The list of the users ids.
        :return: dict, int
            The response and the status code of the request.
        '''
        # Finding the missing ids in the data stores.
        missing_ids = set(user_id_list).difference(list(self.users.keys()))
        # Retuning the error response and status code in case of at least one missing id.
        if len(missing_ids) > 0:
            return {
                       "message" : "Missing ids!",
                       "ids" : list(missing_ids)
                   }, 404
        else:
            # Returning the dictionary with the requested user information.
            user_info = {index : self.users[index] for index in user_id_list}
            return user_info, 200

    def update_user(self, index : str, user_dict : dict):
        '''
            This function updates the user's information in the data store.
        :param index: str
            The index of user to update the information.
        :param user_dict: dict
            The dictionary with the new values for the fields to update.
        :return: dict, int
            The response and the status code of the request.
        '''
        resp_followers = []
        if self.leader:
            # Getting the responsible followers.
            resp_followers = self.get_selected_followers(index)
        if index in self.users:
            # Checking the conditions for updating the data to the data store.
            if (self.leader and "me" in resp_followers) or not self.leader:
                self.users[index].update(user_dict)
                # Removing the "me" from the responsible services list.
                if "me" in resp_followers:
                    resp_followers.remove("me")
            # Forwarding the request to the responsible followers.
            if self.leader:
                for follower in resp_followers:
                    requests.put(f"http://{follower['host']}:{follower['port']}/user/{index}",
                                 json = self.users[index],
                                 headers = {"Token" : "Leader"})

            return self.users[index], 200
        else:
            # Returning the error response if the user already doesn't exist.
            return {
                       "message" : "Missing user!"
                   }, 404

    def delete_user(self, index : str):
        '''
            This function removes the user from the data store.
        :param index: str
            The index of the user to delete from the data store.
        :return: dict, int
            The response and the status code of the request.
        '''
        resp_followers = []
        if self.leader:
            # Getting the responsible followers.
            resp_followers = self.get_selected_followers(index)
        if index in self.users:
            # Checking the conditions for updating the data to the data store.
            if (self.leader and "me" in resp_followers) or not self.leader:
                user_dict = self.users.pop(index)
                # Removing the "me" from the responsible services list.
                if "me" in resp_followers:
                    resp_followers.remove("me")
            # Forwarding the request to the responsible followers.
            if self.leader:
                for follower in resp_followers:
                    requests.delete(f"http://{follower['host']}:{follower['port']}/user/{index}",
                                    headers = {"Token" : "Leader"})

            return user_dict, 200
        else:
            # Returning the error response if the user already doesn't exist.
            return {
                       "message" : "Missing user!"
                   }, 404