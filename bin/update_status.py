#!/usr/bin/python

import getopt
import logging
import sys
import json

##
# Change the state of all processors in a process group.
##
from nifiapi.nifiapi import NifiApi

logger = logging.getLogger(__name__)


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:n:u:", ["start", "stop", "enable", "disable"])
    except getopt.GetoptError as e:
        logger.error(str(e))
        sys.exit(2)

    process_group_name = None
    start = False
    stop = False
    enable = False
    url = None
    controller_state = None

    for opt, arg in opts:
        if opt == "-n":
            processor_name = arg
        elif opt == '-p':
            process_group_name = arg
        elif opt == "-u":
            url = arg
        elif opt == "--start":
            start = True
        elif opt == "--stop":
            stop = True
        elif opt == '--enable':
            enable = True
            controller_state = NifiApi.CONTROLLER_ENABLED
        elif opt == '--disable':
            controller_state = NifiApi.CONTROLLER_DISABLED
        else:
            sys.exit(2)

    if process_group_name is None:
        print("-p [process_group_name] is required.")
        sys.exit(2)

    if not start and not stop and not enable:
        print("One of --enable, --start or --stop is required.")
        sys.exit(2)

    nifiapi = NifiApi(url)

    process_group = nifiapi.find_process_group(process_group_name)
    if process_group is None:
        return None
    pgf = nifiapi.get_process_group_by_id(process_group["id"])

    state = NifiApi.PROCESSOR_STOPPED
    if start:
        state = NifiApi.PROCESSOR_RUNNING
        controller_state = NifiApi.CONTROLLER_ENABLED

    nifiapi.status_change_all_processors(pgf, state, controller_state)



##############################
if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    main()
