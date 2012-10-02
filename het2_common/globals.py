from os.path import dirname, abspath, join

cwd = dirname(abspath(__file__))
HET2_DEPLOY = abspath(join(cwd, '../../../'))

# degrees to radians
DEGREES = math.pi/180.0
# radians to degrees
RADIANS = 180.0/math.pi

# seconds to mjd
SECONDS = 1.0/(24*60*60)
