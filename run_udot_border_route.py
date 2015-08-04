__author__ = 'wzhang@tss'

import os
import subprocess
from udot_border_route import get_parameters

# # input parameters
# config = get_parameters()
# section = "System"
# compiler = config.get(section, "COMPILER")
# script = os.path.join(os.path.dirname(os.path.realpath(__file__)),"udot_border_route.pyc")
# subprocess.call("{0} {1}".format(compiler,script), shell=False)

script = os.path.join(os.path.dirname(os.path.realpath(__file__)),"udot_border_route.py")
subprocess.call("{0} -m py_compile {1}".format("Python",script), shell=False)
