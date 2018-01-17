import logging.config
import requests
import xml.etree.ElementTree as ET
import uuid
import json
import re
import ConfigParser

from time import sleep

logging.config.fileConfig("config/logging.conf")


##
# This is a class for interaction with the Nifi Api version 1.0.x
##
class NifiApi:

    PROCESSOR_RUNNING = "RUNNING"
    PROCESSOR_STOPPED = "STOPPED"
    CONTROLLER_ENABLED = "ENABLED"
    CONTROLLER_DISABLED = "DISABLED"

    def __init__(self, base_url):
        self.url = base_url
        self.logger = logging.getLogger(__name__)

    def write_sensitive_properties(self, pg_id, sensitive_file):
        config = ConfigParser.RawConfigParser()
        config.optionxform = str  # Preserve case
        config.read(sensitive_file)  # This file shouldn't be checked in
        processors = self.get_processors_by_pg(pg_id)
        for processor in processors["processors"]:
            self.logger.debug("Processor: {}".format(processor["component"]["name"]))
            if config.has_section(processor["component"]["name"]):
                self.logger.debug("Found. Setting properties")
                properties = {}
                for name, value in config.items(processor["component"]["name"]):
                    properties[name] = value
                    self.logger.debug("{} = {}".format(name, value))
                rtn = self.set_processor_properties(processor, properties)
                self.logger.debug("set_processor_properties returned {}".format(json.dumps(rtn)))
            for key, value in processor['component']['config']['properties'].items():
                if value is None:
                    continue
                self.logger.debug("Checking properties. {}/{}".format(key, value))
                # If our value is a uuid then it's likely a controller service. Update those properties.
                if re.search('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', value):
                    self.recurse_update_controller(value, config)

    def recurse_update_controller(self, controller_id, config):
        self.logger.debug("Updating controller id {}".format(controller_id))
        controller_service = self.get_controller_service(controller_id)
        if controller_service is None:
            self.logger.warning("Could not find controller service with id: {}".format(controller_id))
            return

        self.logger.debug("Updating {}".format(json.dumps(controller_service['component']['name'])))
        state = controller_service['component']['state']
        if state == 'ENABLED':
            self.logger.debug("Disabling controller")
            controller_service = self.update_controller_status(controller_service, self.CONTROLLER_DISABLED)
            self.logger.debug("{}".format(json.dumps(controller_service)))

        controller_name = controller_service['component']['name']
        config_section = controller_name
        if 'config_section' in controller_service['component']['properties']:
            config_section = controller_service['component']['properties']['config_section']
        self.logger.info("using section: {}".format(config_section))
        if config.has_section(config_section):
            controller_obj = {
                "component": {
                    "id": controller_service["component"]["id"],
                    "properties": {

                    }
                },
                "revision": {
                    "version": controller_service["revision"]["version"]
                }
            }
            for name, value in config.items(config_section):
                if not name.startswith("_"):
                    self.logger.debug("Controller Setting {}={}".format(name, value))
                    controller_obj["component"]["properties"][name] = value
            self.logger.debug("controller properties {}".format(json.dumps(controller_obj)))
            controller_service = self.update_controller_service(controller_obj)
            if controller_service is not None:
                self.logger.debug("update controller returned: {}".format(json.dumps(controller_service)))
            else:
                self.logger.warning("update controller returned None. Did it fail?")
        rtn = self.update_controller_status(controller_service, self.CONTROLLER_ENABLED)

    def get_root_process_group(self):
        """
        Retrieve the root process group
        :return: JSON object of the root process group.
        """
        return self.remote_get('/flow/process-groups/root', None)

    def empty_all_queues(self, pgf):
        """
        This function will empty all queues in the specified process group AND all nested process groups.
        :param pgf: JSON object containing the processGroupFlow object
        :return: This method doesn't return anything
        """
        for connection in pgf["processGroupFlow"]["flow"]["connections"]:
            if connection["status"]["aggregateSnapshot"]["flowFilesQueued"] > 0:
                self.logger.debug("Sending drop request for connection id: {}".format(connection["component"]["id"]))
                drop_request = self.empty_flowfile_queue(connection["component"]["id"])
                if drop_request is None:
                    self.logger.error(
                        "Drop request for connection {} returned None!".format(connection["component"]["id"]))
                    continue
                self.logger.debug("drop request {}".format(json.dumps(drop_request)))
                while not drop_request["dropRequest"]["finished"]:
                    self.logger.debug("Drop request not finished. Waiting 5 sec and will try again.")
                    sleep(5)
                    drop_request = self.get_flowfile_queue_drop_status(connection["component"]["id"],
                                                                       drop_request["dropRequest"]["id"])
                self.logger.debug("Drop request finished.")

        # Empty all queues for nested process groups
        for pg in pgf["processGroupFlow"]["flow"]["processGroups"]:
            flow_pg = self.get_process_group_by_id(pg['id'])
            self.empty_all_queues(flow_pg)

    def status_change_all_ports(self, pgf, status):
        """
        This function changes the status of all input and output ports contained in the specified process group to the
        specified value.
        :param pgf:  JSON object containing the processGroupFlow object
        :param status: Processor status values of RUNNING or STOPPED (see constants)
        :return: True if successful, False otherwise.
        """
        if pgf["processGroupFlow"]["flow"]["inputPorts"]:
            for port in pgf["processGroupFlow"]["flow"]["inputPorts"]:
                port = self.remote_get("/input-ports/", port["component"]["id"])
                self.logger.debug("port: {}".format(json.dumps(port)))
                if not self.update_port(port, "input", status):
                    return False
        if pgf["processGroupFlow"]["flow"]["outputPorts"]:
            for port in pgf["processGroupFlow"]["flow"]["outputPorts"]:
                if not self.update_port(port, "output", status):
                    return False

        return True

    def status_change_controller(self, controller_id, state):
        """
        Change the status on the specified controller_id to the specified value.
        :param controller_id: UUID of the controller we want to update.
        :param state: ENABLED, DISABLED
        :return: True if the operation was successful, False otherwise.
        """
        controller_service = self.get_controller_service(controller_id)
        self.logger.info("Controller {}/{}, setting state to {} from {}".format(controller_id,
                                                                                controller_service['component']['name'],
                                                                                state,
                                                                                controller_service['component'][
                                                                                    'state']))
        if controller_service['component']['state'] != state:
            return self.update_controller_status(controller_service, state) != None
        else:
            return True

    def iterate_and_change_controllers(self, processor, cstate):
        """
        Iterate over all the configuration properties of a processor. If the value looks like
        a UUID, then check if it's a controller that needs to be updated.
        :param processor:
        :param cstate:
        :return:
        """
        for key, value in processor['component']['config']['properties'].items():
            if value is not None:
                if re.search('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', value):
                    self.logger.debug("Found {} that needs to be updated".format(key))
                    if self.get_controller_service(value):
                        if not self.status_change_controller(value, cstate):
                            self.logger.error("Status changing a controller failed.")
                            return False
                    else:
                        self.logger.warning("Probably not a controller uuid: {}".format(value))
        return True

    def status_change_all_processors(self, pgf, status, cstate):
        """
        This function changes the state of all processor that are contained in the given process group.
        :param pgf: JSON object containing the processGroupFlow object
        :param state: Processor state: RUNNING, STOPPED (see constants)
        :param cstate Controller state. ENABLED, DISABLED (see constants)
        :return: True if successful, False otherwise
        """

        for processor in pgf["processGroupFlow"]["flow"]["processors"]:
            # If we are enabling, that needs to be done BEFORE starting the processors.
            if cstate is not None and cstate == self.CONTROLLER_ENABLED:
                self.iterate_and_change_controllers(processor, cstate)
            # Now change the processor status if it's different
            if processor['component']['state'] != status:
                if not self.change_processor_status(processor, status):
                    self.logger.error("Failed to change status {}/{}".format(processor["component"]["name"],
                                                                             json.dumps(processor)))
                    return False
            # If we are disabling, that needs to be done AFTER stopping the processors.
            if cstate is not None and cstate == self.CONTROLLER_DISABLED:
                self.iterate_and_change_controllers(processor, cstate)

        # If there are any input/output ports, make sure to change them.
        if not self.status_change_all_ports(pgf, status):
            self.logger.error("Status changing ports failed.")
            return False

        # Now recursively status change any nested process groups.
        for pg in pgf["processGroupFlow"]["flow"]["processGroups"]:
            flow_pg = self.get_process_group_by_id(pg['id'])
            if not self.status_change_all_processors(flow_pg, status, cstate):
                return False

        return True

    def empty_flowfile_queue(self, id):
        """
        This function requests a drop of all items in the flowfile queue for the id of the given connection.
        :param id: The connection component id to request the drop
        :return: JSON object returned from the nifi api call or None if it fails.
        """
        rtn = self.remote_post_data('/flowfile-queues/{}/drop-requests'.format(id), None)
        if rtn is not None:
            return rtn.json()
        else:
            return None

    def get_flowfile_queue_drop_status(self, id, drop_req_id):
        """
        This function gets the status of a drop request made against a connection id.
        :param id: Connection id
        :param drop_req_id: Drop request id
        :return: JSON object returned from the nifi api call or None if it fails.
        """
        rtn = self.remote_get('/flowfile-queues/{}/drop-requests/{}'.format(id, drop_req_id), None)
        if rtn is not None:
            return rtn
        else:
            return None

    def update_port(self, port, input_or_output, state):
        """
        Update the state of a single input or output port
        :param port: the JSON port object
        :param input_or_output: literally, the string "input" or "output"
        :param state: Processor state RUNNING, STOPPED (see constants)
        :return:
        """
        new_port = {
            "component": {
                "id": port["component"]["id"],
                "state": state
            },
            "revision": {
                "version": port["revision"]["version"]
            }
        }
        return self.remote_put_data('/{}-ports/{}'.format(input_or_output, port["component"]["id"]), new_port)

    def update_controller_status(self, controller, state):
        """
        Update the state of a single controller service
        :param controller: JSON object of the controller
        :param state: ENABLED,DISABLED (see constants)
        :return: JSON output from the api call
        """
        controller_status = {
            "component": {
                "id": controller["component"]["id"],
                "state": state
            },
            "revision": {
                "version": controller["revision"]["version"]
            }
        }
        return self.update_controller_service(controller_status)

    def update_controller_service(self, controller):
        """
        Update a single controller service. This is called usually when changing properties of a controller.
        :param controller: JSON object of the controller
        :return: JSON output of the api call.
        """
        return self.remote_put_data('/controller-services/{}'.format(controller["component"]["id"]), controller)

    def create_controller_json(self, name, properties):
        """
        Create a controller service
        :param name: Name of the service.
        :param properties: dict of properties to set on the controller.
        :return: JSON return of the api call
        """
        controller = {
            "component": {
                "name": name,
                "properties": {}
            },
            "revision": {
                "version": 0
            }
        }

        for name, value in properties:
            if name == "type":
                controller["component"]["type"] = value
            else:
                controller["component"]["properties"][name] = value
        logging.debug("{}".format(json.dumps(controller)))
        return controller

    def create_controller_for_process_group(self, name, properties, process_group_id):
        """
        Create a controller service for a specific process group.
        :param name: Name of the controller service
        :param properties: dict of properties to set on the controller.
        :param process_group_id: Id of the process group to associate it to.
        :return: JSON return from the api call
        """
        controller = self.create_controller_json(name, properties)
        rtn = self.remote_post_data('/process-groups/{}/controller-services'.format(process_group_id), controller)
        if rtn is not None:
            return rtn.json()
        else:
            return None

    def create_controller_service(self, name, properties):
        """
        Create a global level controller service.
        :param name: Name of the controller service
        :param properties: dict of properties to set on the controller.
        :return: JSON return from the api call
        """
        controller = self.create_controller_json(name, properties)
        rtn = self.remote_post_data('/controller/controller-services', controller)
        if rtn is not None:
            return rtn.json()
        else:
            return None

    def controller_exists(self, process_group_id, controller_name):
        """
        Check if a controller with given controller_name exists in the specified process group
        :param process_group_id: id of the process group
        :param controller_name: name of the controller to look for
        :return: true if exists, false otherwise
        """
        self.logger.debug("Checking if controller exists: {}/{}".format(process_group_id, controller_name))
        controller_services = self.get_controller_services(process_group_id)
        for controller in controller_services:
            self.logger.debug("controller name {}".format(controller["component"]["name"]))
            if controller["component"]["name"] == controller_name:
                self.logger.debug("Controller found!")
                return True
        self.logger.debug("Could not find controller")
        return False

    def get_controller_service(self, controller_service_id):
        json = None
        if controller_service_id is not None:
            json = self.remote_get('/controller-services/', controller_service_id)
        return json

    def get_controller_services(self, process_group_id):
        """
        Get all controller services associated with the given process group.
        :param process_group_id: id of the proces group. If None, retrieve global controller services.
        :return: JSON return from the api call.
        """
        json = None
        # If no process_group is specified, grab the global controller services
        if process_group_id is None:
            json = self.remote_get('/flow/controller/controller-services', None)
        else:
            json = self.remote_get('/flow/process-groups/{}/controller-services'.format(process_group_id), None)
        if json is not None:
            return json["controllerServices"]
        else:
            return None

    def set_processor_properties(self, processor, properties):
        """
        Set properties on a specific processor
        :param processor: JSON processor object
        :param properties: dict of properties to set
        :return: JSON return from api call
        """
        update_json = {
            "id": processor["id"],
            "component": {
                "config": {
                    "properties": {

                    }
                },
                "id": processor["component"]["id"]
            },
            "revision": {
                "version": processor["revision"]["version"]
            }
        }
        update_json["component"]["config"]["properties"] = properties
        return self.update_processor(update_json)

    def instantiate_template(self, process_group_id, instantiate_templ_req_entity):
        """
        Convenience function for instantiating a template.
        :param process_group_id: id of the process group to instantiate the template in. Usually, this is the root pg.
        :param instantiate_templ_req_entity: the JSON instantiate template request object (see do_instantiate_template)
        :return: JSON return from the api call.
        """
        return self.remote_post_data('/process-groups/{}/template-instance'.format(process_group_id),
                                     instantiate_templ_req_entity)

    def remove_process_group(self, process_group):
        """
        Remove the given process group
        :param process_group: JSON process group object. (Not process group flow...)
        :return:
        """
        return self.remote_delete('/process-groups/{}/?version={}'.format(process_group['id'],
                                                                          process_group['revision']['version']),
                                  None)

    def upload_template(self, process_group_id, filename):
        """
        Upload a template
        :param process_group_id: id of the process group being loaded
        :param filename: filename containing the XML template.
        :return: JSON return from the api call
        """
        return self.remote_post(self.url + '/process-groups/{}/templates/upload'.format(process_group_id),
                                filename,
                                'application/xml')

    def delete_template(self, id):
        """
        Delete a template
        :param id: id of the template to delete
        :return: JSON return from the api call
        """
        return self.remote_delete('/templates/', id)

    def load_template_from_file(self, file):
        """
        Convenience function to read an XML template file and return the root.
        :param file: File that contains the XML template.
        :return: Root object of the XML tree.
        """
        tree = ET.parse(file)
        return tree.getroot()

    def get_remote_template(self, name):
        """
        Get template that matches the given name
        :param name: Name of the template to look for
        :return: None if it isn't found or the JSON object returned from the api call.
        """
        template = self.remote_get('/flow/templates', None)
        if template is None:
            return None
        for t in template["templates"]:
            if t["template"]["name"] == name:
                return t
        return None

    def get_process_group_by_id(self, id):
        """
        Returns the process group FLOW JSON object by process group id.
        :param id: process group id
        :return: JSON object returned from the api
        """
        return self.remote_get('/flow/process-groups/', id)

    def remote_put_data(self, path, data):
        """
        Convenience function to do an HTTP PUT
        :param path: URL path
        :param data: JSON object
        :return: JSON return from the api call or None if it failed.
        """
        response = requests.put(self.url + path, json=data, headers={'Accept': 'application/json',
                                                                     'Content-Type': 'application/json'})
        # Sometimes it returns 201 (created) or 200
        if response.status_code > 299:
            self.logger.error('POST Error. Status code {} returned. Message {}'.format(response.status_code,
                                                                                       response.text))
            return None
        else:
            return response.json()

    def remote_post_data(self, path, data):
        """
        Convenience function to do an HTTP POST
        :param path: URL path
        :param data: JSON object to POST
        :return: JSON object from api call or None.
        """
        if data is None:
            response = requests.post(self.url + path)
        else:
            response = requests.post(self.url + path, json=data)

        # Sometimes it returns 201 (created) or 200
        if response.status_code > 299:
            self.logger.error('POST Error. Status code {} returned. {}'.format(response.status_code, response.text))
            return None
        else:
            return response

    def start_by_id(self, id):
        """
        Start a processor by it's id.
        :param id: processor id
        :return: JSON object returned from the api call.
        """
        return self.schedule_by_id(id, self.PROCESSOR_RUNNING)

    def stop_by_id(self, id):
        """
        Stop a processor by it's id.
        :param id: processor id
        :return: JSON object returned from the api call
        """
        return self.schedule_by_id(id, self.PROCESSOR_STOPPED)

    def schedule_by_id(self, id, state):
        """
        More generic function to set the state of a processor
        :param id: id of the processor to modify
        :param state: RUNNING, STOPPED (see constants)
        :return: JSON return from api call.
        """
        schedule_components_entity = {
            'id': id,
            'state': state
        }
        return self.remote_post_data('/flow/process-groups/{}'.format(id), schedule_components_entity)

    def find_process_group(self, name):
        """
        Do a search for a process group by the given name. Verifies the return results match the process group name
        exactly.
        :param name: name of the process group
        :return: JSON representing the process group retrieved.
        """
        response = self.remote_get('/flow/search-results?q=' + name, None)
        if response is not None and \
                        'searchResultsDTO' in response and \
                        'processGroupResults' in response['searchResultsDTO']:
            pg_results = response['searchResultsDTO']['processGroupResults']
            rtn_result = None
            for r in pg_results:
                if r["name"] == name:
                    self.logger.debug("Matched name! {}".format(r["name"]))
                    if rtn_result is not None:
                        self.logger.debug("MORE THAN ONE MATCH")
                    else:
                        rtn_result = r
                else:
                    self.logger.debug("Did not match name {}".format(r["name"]))

            if rtn_result is not None:
                pg = self.remote_get('/process-groups/', rtn_result['id'])
                if pg is not None:
                    return pg
            else:
                self.logger.debug(
                    "Could not find an exact match. Here were the results: {}".format(json.dumps(pg_results)))

        return None

    def get_processors_by_pg(self, pg_id):
        """
        Get the processors by the process group id.
        :param pg_id: process group id
        :return: JSON Array of processors contained in the given process group or None
        """
        processors = self.remote_get('/process-groups/{}/processors'.format(pg_id), None)
        if processors is None:
            return None
        return processors

    def find_processor_by_pg(self, pg_id, processor_name):
        """
        Iterate over all the processors in the given process group to find the one specified by name.
        :param pg_id: process group id
        :param processor_name: name of the processor to look for. Once it finds it, return. This means multiple
        matches will only return the first one.
        :return: JSON object
        """
        processors = self.get_processors_by_pg(pg_id)
        if processors is None:
            return None
        for processor in processors['processors']:
            if processor['status']['name'] == processor_name:
                return processor
        return None

    def update_processor(self, processor):
        """
        Convenience method for updating a single processor
        :param processor: JSON processor object.
        :return: JSON return
        """
        return self.remote_put_data('/processors/{}'.format(processor['id']), processor)

    def change_processor_status(self, processor, status):
        """
        Change the status of a specific processor.
        :param processor: JSON object containing the processor
        :param status: RUNNING, STOPPED (see constants)
        :return: JSON output from the api call
        """
        modified_processor = {
            'revision': {
                'version': processor["revision"]["version"],
                'clientId': str(uuid.uuid4())
            },
            'status': {
                'runStatus': status
            },
            'component': {
                'id': processor['id'],
                'state': status
            },
            'id': processor['id']
        }
        logging.debug("Update processor payload: {}".format(json.dumps(modified_processor)))
        return self.update_processor(modified_processor)

    def remove_and_upload_template(self, pg_id, template, templ_name):
        """
        Remove existing template with the specified name and upload the new one.
        :param pg_id: Process group id
        :param template: XML for the template to upload
        :param templ_name: Name of the template
        :return: XML output from the API. I think this is the only API call that returns XML
        """
        remote_template = self.get_remote_template(templ_name)
        if remote_template is not None:
            self.logger.debug('Remote Template Found. id: {}'.format(remote_template["id"]))
            response = self.delete_template(remote_template["id"])
            if response is not None:
                self.logger.debug('Template deleted.')
        else:
            self.logger.debug('Remote template not found.')

        self.logger.debug('Attempting to upload template')
        response = self.upload_template(pg_id, template)
        if response is None:
            self.logger.debug('Template upload failed.')
            return None
        template_entity = ET.fromstring(response.text)
        return template_entity

    def do_instantiate_template(self, pg_id, template_id, x, y):
        """
        Instantiate (add to the canvas) a template. x and y coordinates are optional. If not specified,
        0.0, 0.0 will be used.
        :param pg_id: Parent process group id
        :param template_id: Id of the template to instantiate
        :param x: (optional) x coordinate on the canvas
        :param y: (optional) y coordinate on the canvas
        :return: JSON response from the api call.
        """
        if x is None:
            x = 0.0
        if y is None:
            y = 0.0
        # Import the template
        itre = {
            'templateId': template_id,
            'originX': x,
            'originY': y
        }
        self.logger.debug("instantiate template: {}".format(json.dumps(itre)))
        response = self.instantiate_template(pg_id, itre)
        if response is None:
            self.logger.error('Instantiate Template failed!')
            return None
        self.logger.debug(response.text)
        return response.json()

    def remote_post(self, url, filename, accept_mime_type):
        """
        Low level function used to do a POST to the API
        :param url: Nifi API url
        :param filename: Filename containing data to post.
        :param accept_mime_type: optional, defaults to application/json
        :return: JSON response from the server.
        """
        if accept_mime_type is None:
            accept_mime_type = 'application/json'
        response = requests.post(url,
                                 files={'template': open(filename, 'rb')},
                                 headers={'Accept': accept_mime_type})

        # Sometimes it returns 201 (created) or 200
        if response.status_code > 299:
            self.logger.error('POST Error. Status code {} returned. {}'.format(response.status_code, response.text))
            return None
        else:
            return response

    def remote_delete(self, path, id):
        """
        Low level function to do an HTTP DELETE
        :param path: URL path
        :param id: (optional) the id of the item to delete
        :return: JSON response of the api call.
        """
        if id is None:
            response = requests.delete(self.url + path)
        else:
            response = requests.delete(self.url + path + id)
        if response.status_code != 200:
            self.logger.error('DELETE Error. Status code {} returned. {}'.format(response.status_code, response.text))
            return None
        else:
            return response

    def remote_get(self, path, id):
        """
        Low level function to do an HTTP GET.
        :param path: URL path
        :param id: (optional) Id of the specific object to get
        :return: JSON response of the api call.
        """
        if id is None:
            response = requests.get(self.url + path, headers={'Accept': 'application/json'})
        else:
            response = requests.get(self.url + path + id, headers={'Accept': 'application/json'})
        if response.status_code != 200:
            self.logger.error('GET Error. Status code {} returned. {}'.format(response.status_code, response.text))
            return None
        else:
            return response.json()
