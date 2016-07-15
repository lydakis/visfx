def parse_range(daterange):
    import datetime as dt
    if 'd' in daterange:
        return dt.timedelta(days=int(daterange.replace('d', '')))
    elif 'w' in daterange:
        return dt.timedelta(weeks=int(daterange.replace('w', '')))
    elif 'm' in daterange:
        return dt.timedelta(weeks=4*int(daterange.replace('m', '')))
    elif 'y' in daterange:
        return dt.timedelta(weeks=48*int(daterange.replace('y', '')))
    else:
        raise

def parse_dates(end_date, daterange):
    import datetime as dt
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
    start_date = end_date - parse_range(daterange)
    return start_date.isoformat(), end_date.isoformat()

def modify_record(record, append=None, update=None):
    if append != None:
        record.update(append)
    if update != None:
        record[update[0]] = update[1]
    return record
