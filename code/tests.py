import unittest
from preprocessing import group
from pandas import DataFrame, to_datetime
from pandas.testing import assert_frame_equal
from numpy import array, random, tile, repeat

# add path to code 

# create some artificial data to test grouping function
aggschema = {
    'Credits': [1001, 1002, 1003],
    'Deposits': [2001, 2005]
}

dates = array([to_datetime('2015-01-01')]*10 +\
              [to_datetime('2015-02-01')]*10)

regns = array([100]*5 + [101]*5 + [100]*5 + [101]*5)

values = array([800, 900, 500, 600, 700,
                300, 400, 250, 1000, 150,
                450, 700, 500, 1050, 200,
                350, 750, 50, 600, 400])

old_codes = tile(array([1001, 1002, 1003, 2001, 2005]), 4)

data_101 = DataFrame({'REGN': regns,
                     'DT': dates,
                     'NUM_SC': old_codes,
                     'IITG': values})

data_102 = DataFrame({'REGN': regns,
                     'DT': dates,
                     'CODE': old_codes,
                     'SIM_ITOGO': values})

# dataframe to check if the aggregation was correct
check_101  = DataFrame({'DT': tile(
                        array([to_datetime('2015-01-01')]*2 +\
                         [to_datetime('2015-02-01')]*2), 
                        2),
                        'REGN': repeat(array([100, 101]), 4),
                        'new_code': ['Credits', 'Deposits']*4,
                        'IITG': [2200, 1300, 1650, 1250, 950, 1150, 1150, 1000]})

check_101.index = check_101['DT']
check_101.drop(columns='DT', inplace=True)

check_102 = check_101.rename(columns={'IITG':'SIM_ITOGO'})
            
class Test_GroupFunction(unittest.TestCase):
    def test_grouping_101(self):
        out_101 = group(data=data_101,
                    aggschema=aggschema,
                    form=101)
        assert_frame_equal(out_101, check_101)
    
    def test_grouping_102(self):
        out_102 = group(data=data_102,
                    aggschema=aggschema,
                    form=102)
        assert_frame_equal(out_102, check_102)
        
        

if __name__ == '__main__':
    unittest.main()