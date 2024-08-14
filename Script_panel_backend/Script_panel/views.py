from django.shortcuts import render
from django.core.paginator import Paginator
import sys

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

        # Convert DataFrame to a list of lists for easier rendering in the template
        table_data = sorted_df.values.tolist()

        # Get table headers
        headers = sorted_df.columns.tolist()

        # Set up pagination
        paginator = Paginator(table_data, 50)  # 50 items per page
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        totalpages = page_obj.paginator.num_pages
        context = {
            'headers': headers,
            'page_obj': page_obj,
            'totalpages': [n+1 for n in range(totalpages)],
            'success_message': 'Data updated successfully!',
        }

        return render(request, 'home_page.html', context)

    return render(request, 'home_page.html')
