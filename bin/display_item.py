#!/usr/bin/python

import getopt
import logging
import sys
import json

##
# This script is used to just display the json for a given process group name and processor name.
##
from nifiapi.nifiapi import NifiApi

logger = logging.getLogger(__name__)


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:", ['kv=', 'process-group=', 'processor='])
    except getopt.GetoptError as e:
        logger.error(str(e))
        sys.exit(2)

    url = None
    processor_name = None
    process_group_name = None

    for opt, arg in opts:
        if opt == "--processor":
            processor_name = arg
        elif opt == '--process-group':
            process_group_name = arg
        elif opt == "-u":
            url = arg

    nifiapi = NifiApi(url)
    logging.debug("Looking for process group: {}".format(process_group_name))
    process_group = nifiapi.find_process_group(process_group_name)
    logging.info("Process Group: {}".format(json.dumps(process_group, indent=4)))
    if process_group is None:
        logging.error("Process group {} not found".format(processor_name))
        return
    if processor_name is not None:
        flow_pg = nifiapi.get_process_group_by_id(process_group['id'])
        logging.info("Process Group: {}".format(json.dumps(flow_pg, indent=4)))
        for processor in flow_pg["processGroupFlow"]["flow"]["processors"]:
            if processor["component"]["name"] == processor_name:
                logging.info("Processor: {}".format(json.dumps(processor, indent=4)))


##############################
if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    main()