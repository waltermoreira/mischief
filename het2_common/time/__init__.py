import datetime
import het2_time
import pySlalibC as s

NDP = 6
HET2_Time = het2_time.HET2_Time_instance()

# Copy the namespace of the object HET2_Time to the globals of this
# module, so everything we have in C++ het2_time is here too.
global_dict = globals()
for method in dir(HET2_Time):
    if not method.startswith('__'):
        f = getattr(HET2_Time, method)
        global_dict[method] = f

# Now, rewrite some functions to make them more comfortable to use
# from Python
        
def getUTCFromIndexTime(idx):
    """Return the UTC time corresponding to the index time of the
    current day::
    
       getUTCFromIndexTime(0) -> x

    such that x.hour, x.min, x.sec, x.usec correspond to the index
    time ``idx``.
    """
    utc = het2_time.utcp()
    HET2_Time.getUTCFromIndexTime(utc, idx)
    return utc.value()

def getUTCstrFromIndexTime(idx):
    """Return UTC time for index time ``idx`` in iso format."""
    now = datetime.datetime.utcnow()
    utc = getUTCFromIndexTime(idx)
    utc_idx = now.replace(hour=utc.hour,
                          minute=utc.min,
                          second=utc.sec,
                          microsecond=int(utc.usec))
    return utc_idx.time().isoformat()

class CalendarError(Exception):
    pass
    
def mjd_to_utc(mjd):
    iyear = s.intp()
    imonth = s.intp()
    iday = s.intp()
    frac = s.doublep()
    stat = s.intp()
    s.slaDjcl(mjd, iyear, imonth, iday, frac, stat)
    if stat.value():
        raise CalendarError("couldn't convert MJD %s to UTC" %mjd)
    sign = s.charp()
    ihmsf = s.ivec(4)
    s.slaDd2tf(NDP, frac.value(), sign, ihmsf)
    return (iyear.value(), imonth.value(), iday.value(),
            ihmsf[0], ihmsf[1], ihmsf[2], ihmsf[3])

def mjd_to_datetime(mjd):
    return datetime.datetime(*mjd_to_utc(mjd))

def datetime_to_mjd(utc):
    mjd = s.doublep()
    stat = s.intp()
    s.slaCldj(utc.year, utc.month, utc.day, mjd, stat)
    if stat.value() != 0:
        raise CalendarError('slaCldj cannot convert %s to MJD' %utc.isoformat())
    total_sec = utc.microsecond/1e6 + utc.second
    return mjd.value() + ((total_sec/60.0 + utc.minute)/60.0 + utc.hour)/24.0

def utc_to_tt(utc):
    """Convert utc to terrestrial time"""
    return utc + s.slaDtt(utc)/(24*60*60)
    
def rad_to_hms(rad):
    sign = s.charp()
    ihmsf = s.ivec(4)
    s.slaDr2tf(NDP, rad, sign, ihmsf)
    return (sign.value(), ihmsf[0], ihmsf[1], ihmsf[2], ihmsf[3])

def sexagesimal_to_rad(string):
    start = s.intp()
    start.assign(1)
    result = s.doublep()
    stat = s.intp()
    s.slaDafin(string, start, result, stat)
    return (stat.value(), result.value(), start.value()-1)

def rad_to_degminsec(rad):
    sign = s.charp()
    idmsf = s.ivec(4)
    s.slaDr2af(NDP, rad, sign, idmsf)
    return (sign.value(), idmsf[0], idmsf[1], idmsf[2], idmsf[3])

def rad_to_hourminsec(rad):
    sign = s.charp()
    ihmsf = s.ivec(4)
    s.slaDr2tf(NDP, rad, sign, ihmsf)
    return ('', ihmsf[0], ihmsf[1], ihmsf[2], ihmsf[3])

def mjd_to_indextime(mjd):
    utc = mjd_to_utc(mjd)
    utc_t = het2_time.UTC_t()
    (utc_t.year, utc_t.month, utc_t.day,
     utc_t.hour, utc_t.min, utc_t.sec, utc_t.usec) = utc
    utcp = het2_time.utcp()
    utcp.assign(utc_t)
    return getIndexTimeFromUTC(utcp)
    