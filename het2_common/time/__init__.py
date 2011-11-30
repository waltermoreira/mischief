import het2_time
import datetime

HET2_Time = het2_time.HET2_Time_instance()

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

def getCurrentIndexTime():
    return HET2_Time.getCurrentIndexTime()