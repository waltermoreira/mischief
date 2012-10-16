from os.path import dirname, abspath, join
import os
import math

cwd = dirname(abspath(__file__))
HET2_DEPLOY = abspath(join(cwd, '../../../'))

# degrees to radians
DEGREES = math.pi/180.0
# radians to degrees
RADIANS = 180.0/math.pi
# arcsecs to radians
ARCSECONDS = math.pi/(180.0*60*60)

# seconds to mjd
SECONDS = 1.0/(24*60*60)

DEPLOY_PATH = os.environ['HET2_DEPLOY']