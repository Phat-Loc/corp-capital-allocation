# -*- coding: utf-8 -*-
"""

"""

import argparse
import sys
import logging
import subprocess
import time

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"

_logger = logging.getLogger(__name__)


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="A governing process to monitor and relaunch sub-processes")
    parser.add_argument(
        dest="replicas",
        help="The number of subprocess to run",
        type=int)
    parser.add_argument(
        dest="command",
        help="The command to run",
        type=str)
    parser.add_argument(
        "-c_args",
        dest="command_args",
        help="The arguments to the command",
        type=str)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    if args.loglevel:
        setup_logging(args.loglevel)
    else:
        setup_logging(loglevel=logging.WARNING)

    processes = set()

    def is_proc_alive(p):
        return p.poll() is None

    while True:
        # Look for dead processes
        dead_processes = []
        for proc in processes:
            if not is_proc_alive(proc):
                dead_processes.append(proc)

        # Remove them
        for proc in dead_processes:
            processes.remove(proc)

        # Spawn processes
        for i in range(args.replicas - len(processes)):
            sproc_args = args.command_args.split(' ')
            proc = subprocess.Popen([args.command, *sproc_args])
            processes.add(proc)

        # Sleep
        time.sleep(120)
        _logger.info('Governing sub processes')


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
