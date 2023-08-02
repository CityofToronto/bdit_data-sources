import pandas, numpy

locations = pandas.read_excel('ecocounter_data.xlsx', sheet_name='locations')

locations.to_csv('locations.csv',index=False)

print(locations)

counts = pandas.read_excel('ecocounter_data.xlsx', sheet_name='counts')

# lengthen
counts = counts.melt(
    id_vars='timestamp',
    value_vars=locations['flow_id'],
    var_name='flow_id',
    value_name='volume'
)
# remove nulls
counts = counts[pandas.notnull(counts.volume)]

# reorder columns
counts = counts[['flow_id','timestamp','volume']]

# set back to int now the NaNs are gone
counts['volume'] = counts['volume'].astype('int')

print(counts)

counts.to_csv('counts.csv',index=False)