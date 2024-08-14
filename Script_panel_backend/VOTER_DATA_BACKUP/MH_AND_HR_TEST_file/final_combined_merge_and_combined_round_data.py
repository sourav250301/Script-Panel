import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from MH_AND_HR_TEST_file.combine_Round_data import COM_Round_data as df1
from MH_AND_HR_TEST_file.merge_indivisual_round_data import final_merge_indivisual_round_df as df2


final_combined_df=pd.concat([df1, df2],ignore_index=True)
