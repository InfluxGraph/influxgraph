def calculate_interval(start_time, end_time):
    """Calculates wanted data series interval according to start and end times

    Returns interval in seconds
    """
    time_delta = end_time - start_time
    deltas = {
        # 3 days -> 1min
        259200 : 60,
        # 7 days -> 5min
        604800 : 300,
        # 14 days -> 10min
        1209600 : 600,
        # 28 days -> 15min
        2419200 : 900,
        # 2 months -> 30min
        4838400 : 1800,
        # 4 months -> 1hour
        9676800 : 3600,
        # 12 months -> 3hours
        31536000 : 7200,
        # 4 years -> 12hours
        126144000 : 43200,
        }
    for delta in sorted(deltas.keys()):
        total_seconds = (time_delta.microseconds + (
            time_delta.seconds + time_delta.days * 24 * 3600) * 10**6) / 10**6
        if total_seconds <= delta:
            return deltas[delta]
    # 1 day default, or if time range > 4 year
    return 86400
