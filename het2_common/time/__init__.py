import het2_time
import datetime

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
