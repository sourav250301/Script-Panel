query = """
       select  * from CH_TEST_DATA_BC_AC_WISE
     LEFT join CH_TEST_DATA_BC_PC_WISE
    on CH_TEST_DATA_BC_AC_WISE.AC_ID=CH_TEST_DATA_BC_PC_WISE.AC_ID
    and CH_TEST_DATA_BC_AC_WISE.PC_ID=CH_TEST_DATA_BC_PC_WISE.PC_ID
    and CH_TEST_DATA_BC_AC_WISE.N_PARTY=CH_TEST_DATA_BC_PC_WISE.N_PARTY
    and CH_TEST_DATA_BC_AC_WISE.RES1=CH_TEST_DATA_BC_PC_WISE.RES1
"""
query2='''
   SELECT * FROM CH_TEST_DATA_BC_AC_WISE
'''

QUERY_3='''
     SELECT * FROM CH_TEST_DATA_BC_PC_WISE

'''