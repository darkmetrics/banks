import json
import unittest
from pandas import DataFrame, to_datetime
from pandas.testing import assert_frame_equal
from numpy import array, random, tile, repeat

from parameters import test_group_dict
from preprocessing import group, positive_negative_dictionaries, group_one_form                          

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


# create DataFrame to check group_one_form function
test_group_df = DataFrame().from_dict(test_group_dict)
test_group_df.index = to_datetime(test_group_df['DT'])
test_group_df.drop(columns='DT', inplace=True)

# get grouping dict
with open('parameters.json', mode='r') as f:
    dict_of_dicts = json.load(f)

bs_dict = dict_of_dicts['BS_old']

# make test DataFrame with low-level functions
# get dicts for postitive and negative values
BS_old_group_dict_positive, BS_old_group_dict_negative = \
            positive_negative_dictionaries(dict_of_dicts, "BS_old")

BS_old_positive_grouping = group(data=test_group_df, 
                                 aggschema=BS_old_group_dict_positive, 
                                 form=101)

BS_old_negative_grouping = group(data=test_group_df, 
                                 aggschema=BS_old_group_dict_negative, 
                                 form=101)         

BS_old_grouping = BS_old_positive_grouping.merge(BS_old_negative_grouping, 
                                                 on = ["REGN", "new_code", "DT"], 
                                                 how = "outer",
                                                 suffixes = ("_positive", "_negative"))               

BS_old_grouping.fillna(0, inplace=True)
# get net value for each aggregated account
BS_old_grouping["IITG"] = BS_old_grouping.IITG_positive - BS_old_grouping.IITG_negative


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
        
# testing function group_one_form from preprocessing module
class Test_group_one_form_Function(unittest.TestCase):
    def test_old_bs(self):
        grouped_bs_old = group_one_form(test_group_df, 
                                        "BS_old", 
                                        dict_of_dicts)       
        assert_frame_equal(grouped_bs_old, BS_old_grouping)

if __name__ == '__main__':
    unittest.main()