'''
Tools to assist display.py

Filename: display_tools.py
Cohort: Insight Data Engineering SEA '19C
Name: Carl Ulberg
'''


# These next two functions convert a number from 0-600 to a corresponding date.
# The month value is somewhat realistic to represent the number of worldwide
#       earthquakes per month, and will be displayed for the user.
# The day value is what I actually used when building the database (a mistake
#       when initially creating it). This is used to query the database
def convert_value_to_month(selected_value):
    starting_year = 1970

    my_year = starting_year + selected_value//12
    my_month = (selected_value % 12) + 1
    return '{}-{:02d}'.format(my_year, my_month)


def convert_value_to_date(selected_value):
    '''
    The augmented earthquakes were entered with arbitrary dates
    between 1980-1985
    '''
    my_year = 1980 + selected_value//120
    tmpval = selected_value % 120
    my_month = tmpval//10 + 1
    my_day = (tmpval % 10) + 1

    return int('{:4d}{:02d}{:02d}'.format(my_year, my_month, my_day))


def make_eq_dict(df, method):
    '''
    return a data dict that will be put into a dcc.Graph figure
    '''
    data_dict = dict(
        type='scattergeo',
        locationmode='USA-states',
        lon=df['Longitude'],
        lat=df['Latitude'],
        mode='markers',
        name='Algorithm {}'.format(method),
        marker=dict(
            size=2,
            opacity=0.8
        )
    )
    return data_dict
