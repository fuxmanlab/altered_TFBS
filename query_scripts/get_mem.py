# A little Python script that returns the amount of memory
# on the computer in GB after calling vmstat. 
# An optional multiplier can be used.

import shlex
import subprocess
import argparse
import math

parser = argparse.ArgumentParser()
parser.add_argument('-frac',   help="Optional multiplier.", type=float, default=1.0)
args = parser.parse_args()    
    



cmd=shlex.split('vmstat -s -S M')
p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
# let it run...
stdout,stderr = p.communicate()  

# Get the memory in megabytes
kb = float(stdout.split('\n')[0].split()[0])

# Convert to GB, multiply by the fraction, and print the
# GB value rounded down.
gb = int(math.floor(kb / 1024 * args.frac))

print('%sG' % gb)
