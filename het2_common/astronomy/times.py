import pySlalibC as s
import datetime

NDP = 6

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
    return utc + pySlalibC.slaDtt(utc)/(24*60*60)
    
def rad_to_hms(rad):
    sign = s.charp()
    ihmsf = s.ivec(4)
    s.slaCr2tf(NDP, rad, sign, ihmsf)
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
