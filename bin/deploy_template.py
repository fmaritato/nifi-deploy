#!/usr/bin/python

import getopt
import logging
import logging.config
import sys
import json
import configparser
import random

from nifiapi.nifiapi import NifiApi

logging.config.fileConfig("config/logging.conf")
logger = logging.getLogger(__name__)


##
# This script will deploy a template to a nifi server.
#
# Usage:
# deploy_template -u http://localhost:8080/nifi-api -t /path/to/template.xml --start
#
# At a high level this is what this script will do:
# * Load the template XML file
# * Stop existing process group processors
# * Make sure connection flow file queues are empty
# * Delete existing process group
# * Delete existing template
# * Upload and Instantiate new template
# * Update and enable any controller services
# * Modify processors that have sensitive properties
# * Start all processors
# * Start all input/output ports
##
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:t:", ['start', 'sensitive='])
    except getopt.GetoptError as e:
        logger.error(str(e))
        sys.exit(2)

    start = False
    template = None
    url = None
    sensitive_file = "config/sensitive.cfg"
    for opt, arg in opts:
        if opt == "-u":
            url = arg
        elif opt == "-t":
            template = arg
        elif opt == "--start":
            start = True
        elif opt == "--sensitive":
            sensitive_file = arg
        else:
            sys.exit(2)

    nifiapi = NifiApi(url)

    config = configparser.RawConfigParser()
    config.optionxform = str  # Preserve case

    root_process_group = nifiapi.get_root_process_group()
    if root_process_group is None:
        logger.error("Could not get the root process group!")
        exit()

    root_process_group_id = root_process_group["processGroupFlow"]["id"]
    logger.info("Root process group id: {}".format(root_process_group_id))

    logger.info("Loading template from file {}".format(template))
    root = nifiapi.load_template_from_file(template)

    templ_name_elem = root.find('name')
    templ_name = templ_name_elem.text
    pg_name_elem = root.find('snippet/processGroups/name')
    pg_name = pg_name_elem.text
    logger.debug('Will look for template name: {}'.format(templ_name))

    # Remove the process group from the canvas.
    pg = nifiapi.find_process_group(pg_name)
    if pg is None:
        logger.info("Could not find existing process group.")
    else:
        logger.info('Process group found. Id {}'.format(pg['id']))
        flow_pg = nifiapi.get_process_group_by_id(pg['id'])

        # First stop all processors. We need to call the /flow/process-group/id endpoint to get this info
        logger.info('Changing status on all processors to {}'.format(nifiapi.PROCESSOR_STOPPED))
        nifiapi.status_change_all_processors(flow_pg, nifiapi.PROCESSOR_STOPPED, nifiapi.CONTROLLER_DISABLED)

        # Make sure all connection queues are empty
        logger.info('Empying all queues')
        nifiapi.empty_all_queues(flow_pg)

        # Now try to remove the process group
        logger.info('Attempting removal of process group')
        response = nifiapi.remove_process_group(pg)
        if response is None:
            logger.error('Removing the process group failed!')
            sys.exit(3)
        else:
            logger.info('Remove process group succeeded. Now will try to import from template.')

    # Remove existing template with same name/id and upload the new one
    template_entity = nifiapi.remove_and_upload_template(root_process_group_id, template, templ_name)
    if template_entity is None:
        logger.error("remove and upload returned None.")
        sys.exit(3)
    template_id = template_entity.find('template/id').text
    logger.info('Template upload succeeded. Entity {}'.format(template_id))

    # Now instantiate (add to the canvas) the new template.
    x = random.uniform(0, 200)
    y = random.uniform(0, 200)
    response = nifiapi.do_instantiate_template(root_process_group_id, template_id, x, y)
    if response is None:
        logger.error("Instantiate template failed!")
        sys.exit(3)
    # logger.debug("{}".format(json.dumps(response)))
    logger.info("Template instantiated. Configuring controller services...")

    # Grab the ProcessGroup by it's id.
    new_pg_id = response["flow"]["processGroups"][0]["component"]["id"]
    new_pg = nifiapi.get_process_group_by_id(new_pg_id)
    logger.debug(json.dumps(new_pg))

    # recursively go through nested process groups and perform actions
    # like write sensitive properties and update controllers
    recurse_process_groups(new_pg, config, sensitive_file, nifiapi)

    if start:
        logger.info("Now starting all processor and ports.")
        # refetch the process group in case any of the processors were modified we will need the new revision version.
        pg = nifiapi.get_process_group_by_id(new_pg_id)
        nifiapi.status_change_all_processors(pg, nifiapi.PROCESSOR_RUNNING, nifiapi.CONTROLLER_ENABLED)
    logger.info("Done")


def recurse_process_groups(flow_pg, config, sensitive_file, nifiapi):
    flow_pg_id = flow_pg["processGroupFlow"]["id"]
    nifiapi.write_sensitive_properties(flow_pg_id, sensitive_file)
    for pg in flow_pg["processGroupFlow"]["flow"]["processGroups"]:
        new_flow_pg = nifiapi.get_process_group_by_id(pg["component"]["id"])
        recurse_process_groups(new_flow_pg, config, sensitive_file, nifiapi)

def update_controllers(pg_id, config, nifiapi):
    controller_services = nifiapi.get_controller_services(pg_id)
    for controller in controller_services:
        controller_name = controller["component"]["name"]
        logging.debug("updating controller {}".format(controller_name))
        controller_file_name = controller_name.replace(" ", '_')
        data = config.read("config/controller/{}.cfg".format(controller_file_name))
        if len(data) == 0:
            logging.error("Could not read controller config file {}".format(controller_file_name))
            return
        controller_obj = {
            "component": {
                "id": controller["component"]["id"],
                "properties": {

                }
            },
            "revision": {
                "version": controller["revision"]["version"]
            }
        }
        for name, value in config.items(controller_name):
            if not name.startswith("_"):
                controller_obj["component"]["properties"][name] = value
        logging.debug("{}".format(controller_obj))
        controller_rtn = nifiapi.update_controller_service(controller_obj)

        if controller_rtn is None:
            logger.error("There was an error updating the controller: {}".format(controller_name))
        else:
            # Services like the DistributedMapCacheClientService require a global service to be running
            # Check for that in the config and create/start if necessary.
            required_service = None
            if config.has_option(controller_name, "_requires_service"):
                required_service = config.get(controller_name, "_requires_service")
            if required_service is not None:
                config.read("config/controller/{}.cfg".format(required_service))
                if not nifiapi.controller_exists(None, required_service):
                    required_svc_rtn = \
                        nifiapi.create_controller_service(name=required_service,
                                                          properties=config.items(required_service))
                    if required_svc_rtn is None:
                        logger.error("ERROR Creating required service! {}".format(required_service))
                    else:
                        logger.info("Required service created {}/{}".format(required_service,
                                                                            json.dumps(required_svc_rtn)))
                        nifiapi.update_controller_status(required_svc_rtn, nifiapi.CONTROLLER_ENABLED())
            # We need to (UNFORTUNATELY) update the state separately from the properties.
            nifiapi.update_controller_status(controller_rtn, nifiapi.CONTROLLER_ENABLED())


##############################
if __name__ == "__main__":
    main()
