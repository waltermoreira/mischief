import het2_common.time as t

def test_current_index():
    n = t.getCurrentIndexTime()	
    assert n >= 0
    assert n <= 86401

def test_get_utc_str():
    s = t.getUTCstrFromIndexTime(0)
    assert s == '18:00:00'
    s = t.getUTCstrFromIndexTime(100.23)
    assert s == '18:01:40.230000'
