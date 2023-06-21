import requests


class CacheRoundRobin:
    def __init__(self, caches : dict) -> None:
        '''
            The constructor of the Cache Round Robin load balancer.
        :param caches: dict
            The dictionary contains the credentials of the caches services.
        '''
        self.caches = caches
        self.caches_list = list(self.caches.keys())
        self.responsible_cache = self.caches_list[0]

    def turn(self):
        '''
            This function turns the round robin and gives the responsibility to make requests to other person.
        '''
        # Getting the responsible cache service index.
        resp_index = self.caches_list.index(self.responsible_cache)

        # Setting the next cache service as the responsible one using the previous index.
        if resp_index + 1 == len(self.caches_list):
            self.responsible_cache = self.caches_list[0]
        else:
            self.responsible_cache = self.caches_list[resp_index]

    def get_value(self, payload : dict):
        '''
            This function makes the request to the responsible service.
        :param payload: dict
            The payload of the request.
        :return: dict or None
            The response payload.
        '''
        # Making the request to the responsible service.
        response = requests.get(
            f"https://{self.caches[self.responsible_cache]['host']}:{self.caches[self.responsible_cache]['port']}/cache",
            json = payload
        )
        # Checking if the status of the request.
        if response.status_code == 200:
            # Turning the round robin and returning the response in case of successful request.
            self.turn()
            return response.json()
        else:
            # Getting another service from cache services list and making it temporarily responsible for requests
            resp_index = self.caches_list.index(self.responsible_cache)
            if resp_index + 1 == len(self.caches_list):
                next_cache = self.caches_list[0]
            else:
                next_cache = self.caches_list[resp_index]

            # Making the request.
            response = requests.get(
                f"https://{self.caches[next_cache]['host']}:{self.caches[next_cache]['port']}/cache",
                json = payload
            )
            # Turning the round robin.
            self.turn()

            # Returning the result.
            if response.status_code == 200:
                return response.json()
            else:
                return None