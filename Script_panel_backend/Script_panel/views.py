from django.shortcuts import render
from django.core.paginator import Paginator
import sys

sys.path.append('/path/to/VOTER_DATA_BACKUP')
from VOTER_DATA_BACKUP.FUNCTION.test import end_df

def button(request):
    return render(request, 'home_page.html')

def run_script(request):
    if request.method == 'POST' or 'filter_state' in request.GET or 'filter_pc_no' in request.GET or 'page' in request.GET:
        df = end_df()  # Assuming end_df returns a Pandas DataFrame

        # Apply the first filter (State)
        state_filter = request.GET.get('filter_state')
        if state_filter:
            df = df[df['state'] == state_filter]
            print("After state filter:", df)

        # Get unique values for PC_NO after the state filter is applied and sort them
        df['PC_NO'] = df['PC_NO'].astype(str)  # Ensure PC_NO is treated as a string
        pc_nos = sorted(df['PC_NO'].unique()) if state_filter else []

        # Apply the second filter (PC_NO)
        pc_no_filter = request.GET.get('filter_pc_no')
        if state_filter and pc_no_filter:
            df = df[df['PC_NO'] == pc_no_filter]
            print("After PC_NO filter:", df)

        # Sort the DataFrame by specific columns
        sorted_df = df.sort_values(by=['state', 'PC_NO'])

        # Convert DataFrame to a list of lists for easier rendering in the template
        table_data = sorted_df.values.tolist()

        # Get table headers
        headers = sorted_df.columns.tolist()

        # Set up pagination
        paginator = Paginator(table_data, 30)  # 30 items per page
        page_number = request.GET.get('page')  # Get the page number from the request
        page_obj = paginator.get_page(page_number)
        totalpages = page_obj.paginator.num_pages

        # Get unique values for the first filter
        states = end_df()['state'].unique()

        # If state is selected, ensure PC_NO values are filtered and sorted
        if state_filter:
            # Filter by state to get the relevant PC_NO values
            df_filtered = end_df()[end_df()['state'] == state_filter]
            pc_nos = sorted(df_filtered['PC_NO'].astype(str).unique())

        context = {
            'headers': headers,
            'page_obj': page_obj,
            'totalpages': range(1, totalpages + 1),
            'success_message': 'Data updated successfully!' if request.method == 'POST' else '',
            'states': states,
            'pc_nos': pc_nos,
            'selected_state': state_filter,
            'selected_pc_no': pc_no_filter,
        }

        return render(request, 'home_page.html', context)

    return render(request, 'home_page.html', {})
