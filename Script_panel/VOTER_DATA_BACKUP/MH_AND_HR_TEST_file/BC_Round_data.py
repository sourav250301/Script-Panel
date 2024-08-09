import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from MH_AND_HR_TEST_file.read_BC_Round_data_from_database import dataframe as df
from FUNCTION.Function_for_calculation import main_processing_function
print("BC round calculation start ....")
if df is not None:
    df=main_processing_function(df)
    
df["ROUND"]="BC" 
df.drop(columns=['DN','PART_NO','Start_Time','DTMF_REP','RES2','CASTE','GENDER','AGE','RES_ACWISE_TOTAL','RANK_PER_ACWISE','RES_PCWISE_TOTAL','RANK_PER_PCWISE','RES_SEQWISE_TOTAL','RANK_PER_SEQ_WISE_AC','RES_SEQWISE_TOTAL_PC','RANK_PER_SEQ_WISE_PC'],inplace=True)
df.drop_duplicates(subset=['AC_ID','PC_ID','N_PARTY','RES1'],inplace=True)   
print(df.shape[0]) 

print("BC round calculation completed ....")
    
BC_Round_df=df
