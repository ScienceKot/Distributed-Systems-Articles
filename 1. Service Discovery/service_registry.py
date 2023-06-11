# Importing all needed modules.
import time
import threading


class ServiceRegistry:
    def __init__(self):
        '''
            The constructor of the Service Registry.
        '''
        # Setting up the service and heartbeat registry.
        self.services = {}
        self.heartbeats = {}
        self.heartbeats_lock = threading.Lock()
        self.time_threashold = 30

    def create(self, request_body : dict):
        '''
            This function adds a service in service registries.
        :param request_body: dict
            The request body.
        :returns: dict, int
            The response returned.
            The status code.
        '''
        # Getting the name of the service.
        name = request_body["name"]

        # Checking the presence of the service.
        if name in self.services:
            return {
                       "message" : "Service already registered!"
                   }, 208
        else:
            # Adding the service to the service and heartbeat registries.
            self.services[name] = request_body
            self.heartbeats_lock.acquire()
            self.heartbeats[name] = time.time()
            self.heartbeats_lock.release()
            return request_body, 200

    def read_all(self):
        '''
            This function returns the dictionary of the all registered services.
        :return: dict, int
            The service registry.
            The status code.
        '''
        return self.services, 200

    def read_some(self, services_list : list):
        '''
            This function returns the dictionary with the information of the requested services.
        :param services_list: list
        :return: dict, int
            The dictionary with the service information or the error message.
            The status code.
        '''
        # Getting the missing services in the service registry from the requested ones.
        services_dif = set(services_list).difference(self.services.keys())

        # If there are missing services then the list of missing services is returned.
        if len(services_dif) > 0:
            return {
                "missing_services" : list(services_dif)
            }, 404
        else:
            # If all services are present the information of those services is returned.
            return {
                service_name : self.services[service_name]
                for service_name in services_list
            }, 200

    def update(self, request_body : dict):
        '''
            This function updates the information about a service.
        :param request_body: dict
            The body of the request.
        :return: dict, int
            The new values of the service.
            The status code.
        '''
        # Getting the name of the service.
        name = request_body["name"]

        # If the service is present then it's information is updated.
        if name in self.services:
            self.services[name].update(request_body)
            return self.services[name], 200
        else:
            # If the service is missing the error message is returned.
            return {
                "message" : "No such service!"
            }, 404

    def delete(self, request_body : dict):
        '''
            This function deletes a service from service registry.
        :param request_body: dict
            The body of the request.
        :return: dict, int
            The new values of the service.
            The status code.
        '''
        # Getting the name of the service from request body.
        name = request_body["name"]
        if name in self.services:
            # Getting the service information and deleting it from service registry.
            service_info = self.services[name]
            del self.services[name]
            
            # Deleting the service from heartbeats.
            self.heartbeats_lock.acquire()
            del self.heartbeats[name]
            self.heartbeats_lock.release()
            return service_info, 200
        else:
            # Returning the error message if the requested service isn't present in registry.
            return {
                "message" : "No such service!"
            }, 404

    def add_heartbeat(self, service_name : str):
        '''
            This function updates the heartbeat timestamp for a service
        :param service_name: str
            The name of service sending the heartbeat request.
        :return: dict, int
            The new values of the service.
            The status code.
        '''
        # Checking if the service is registered.
        if service_name not in self.heartbeats:
            # Returning the error message and 404 status code.
            return {
                "message" : "Not registered service!"
            }, 404
        else:
            # Updating the last heartbeat timestamp.
            self.heartbeats_lock.acquire()
            self.heartbeats[service_name] = time.time()
            self.heartbeats_lock.release()

            # Returning the success message and the 200 status code.
            return {
                "message" : "Heartbeat received!"
            }, 200

    def check_heartbeats(self):
        '''
            This function checks the last heartbeats of the registered services every 10 seconds
            and prints the one that didn't send a heartbeat request more than 30 seconds.
        '''
        while True:
            self.heartbeats_lock.acquire()
            # Iterating through services and checking how long a go the service sent the last heartbeat
            # request.
            for service in self.heartbeats:
                if time.time() - self.heartbeats[service] > self.time_threashold:
                    print(f"Service - {service} seems to be dead!")
            self.heartbeats_lock.release()
            time.sleep(10)