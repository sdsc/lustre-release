#! /bin/env python
#

from signal import signal, SIGUSR2
from sys import exit
from time import sleep

# TODO: indentation policy ?
def continue_and_exit(signum, frame):
    """
    Continue the execution of the program after catching a signal
    """
    gdb.execute("continue")
    exit(0)

def poll(repeat, sleep_time=1):
    sleep(sleep_time)
    return repeat - 1

def wait_for_signal():
    gdb.execute("break llapi_hsm_action_end")
    signal(SIGUSR2, continue_and_exit)
    gdb.execute("run")
    repeat = 20
    while repeat > 0:
        repeat = poll(repeat)
    gdb.execute("continue")

wait_for_signal()
