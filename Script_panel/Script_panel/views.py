from django.http import HttpResponse
from django.shortcuts import render
import sys
sys.path.append('/path/to/VOTER_DATA_BACKUP')
from VOTER_DATA_BACKUP.FUNCTION.test import end_df

def button(request):
    print('Rendering initial page')
    return render(request, 'home_page.html')

def run_script(request):
    print('Running the Python script')

    # Call the end_df() function and convert it to a list of lists for table rendering
    df_result = end_df().collect()  # Assuming end_df returns a PySpark DataFrame

    table_data = [row.asDict().values() for row in df_result]  # Convert to list of dictionaries, then extract values

    # Store the table data in the context
    context = {'table_data': table_data}

    return render(request, 'home_page.html', context)
