import importlib
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from ALL_ROUND_DATA.BC_Round_read_and_calculation import final_df_BC as df1
from ALL_ROUND_DATA.EP_Round_read_and_calculation import final_df_EP as df2
from ALL_ROUND_DATA.PP_Round_read_and_calculation import final_df_PP as df3
from ALL_ROUND_DATA.COM_Round_read_and_merge import final_df_COM as df4

print(" ")
print("combining all round data start .....😊😊😊😊")
final_combined_df = df1.join(df2, on=['AC_ID', 'PC_ID'], how='left') \
                             .join(df3, on=['AC_ID', 'PC_ID'], how='left') \
                             .join(df4, on=['AC_ID','PC_ID'], how='left') \

# final_combined_df.show(10)
def one_state_all_round():
    end_df = final_combined_df.toPandas()
    return end_df

print("combining completed successfully .....😊😊😊😊")


