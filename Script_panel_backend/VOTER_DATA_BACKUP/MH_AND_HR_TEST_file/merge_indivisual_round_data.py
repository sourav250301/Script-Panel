import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from MH_AND_HR_TEST_file.BC_Round_data import BC_Round_df as df1
from MH_AND_HR_TEST_file.EP_Round_data import EP_Round_df as df2
from MH_AND_HR_TEST_file.PP_Round_data import PP_Round_df as df3

final_merge_indivisual_round_df=pd.concat([df1, df2, df3],ignore_index=True)


