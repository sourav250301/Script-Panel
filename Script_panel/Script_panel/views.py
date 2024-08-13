from django.http import HttpResponse
from django.shortcuts import render
from django.core.paginator import Paginator
import sys
import pandas as pd
import time

sys.path.append('/path/to/VOTER_DATA_BACKUP')
from VOTER_DATA_BACKUP.FUNCTION.test import end_df

def button(request):
    return render(request, 'home_page.html')

def run_script(request):
    if request.method == 'POST':
        # Get the Pandas DataFrame
        df = end_df()  # Assuming end_df returns a Pandas DataFrame

        # Sort the DataFrame by specific columns
        sorted_df = df.sort_values(by=['AC_ID', 'PC_ID'])

        # Convert DataFrame to list of dictionaries
        table_data = sorted_df.to_dict(orient='records')
        
        # Dynamically get table headers
        headers = sorted_df.columns.tolist()

        # Implement pagination
        paginator = Paginator(table_data, 10)  # Show 10 items per page
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)

        context = {
            'table_data' : table_data,
            'page_obj': page_obj,
            'headers': headers,
            'success_message': 'Data updated successfully!'
        }
        
        return render(request, 'home_page.html', context)

    return render(request, 'home_page.html')
