
def extract_vin_key_value(row):
    """
    This method maps vin_number, make, year, and incident_type from the corresponding row.
    This function will return vin_number, (make, year, incident_tuype)
    eg: [('VXIO456XLBB630221', ('Nissan', '2003', 'I'))
    """
    data = row.split(",")
    
    # assign vin number
    vin_num = data[2]
    # asign car maker
    make = data[3]
    # assign manufacture year
    year = data[5]
    # assign type of incident
    incident_type = data[1]
    
    return (vin_num , (make, year, incident_type))

def populate_make(vals):
    """
    Perform group aggregation to populate make and year to all the records
    Returns lists of make, year, and incident_type
    eg: [('Nissan', '2003', 'I'),
    """
    list_val = []
    for val in vals:
        # iterate through the list and append if make value exists
        if val[0] != '' :
            make = val[0]
        # iterate through the list and append if year value exists
        if val[1] != '' :
            year = val[1]
        # append make, year and incident_type which is 'val[2]'
        list_val.append((make, year, val[2]))
    return list_val

def extract_make_key_value(list_val):
    """
    Count number of occurrence for accidents(incident_type == "A") for the vehicle make and year
    returns a tuple type
    ('Nissan-2003', 0)             
    """
    if list_val[2] == "A":
        return list_val[0]+ '-' + list_val[1], 1
    else:
        return list_val[0]+ '-' + list_val[1], 0