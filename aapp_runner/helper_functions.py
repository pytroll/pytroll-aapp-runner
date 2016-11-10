def overlapping_timeinterval(start_end_times, timelist):
    """From a list of start and end times check if the current time interval
    overlaps with one or more"""

    starttime, endtime = start_end_times
    for tstart, tend in timelist:
        if ((tstart <= starttime and tend > starttime) or
                (tstart < endtime and tend >= endtime)):
            return tstart, tend
        elif (tstart >= starttime and tend <= endtime):
            return tstart, tend

    return False
