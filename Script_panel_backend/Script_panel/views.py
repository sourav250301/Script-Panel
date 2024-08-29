from django.shortcuts import render
from django.core.paginator import Paginator
import sys

# Adjust the path to where the FUNCTION module is located
sys.path.append('/path/to/VOTER_DATA_BACKUP')
from VOTER_DATA_BACKUP.FUNCTION.test import end_df

def button(request):
    return render(request, 'home_page.html')

def run_script(request):
    # Check if the request is a POST, refresh, or any filter or pagination is applied
    if request.method == 'POST' or 'refresh' in request.GET or 'filter_state' in request.GET or 'filter_pc_no' in request.GET or 'filter_res1' in request.GET or 'page' in request.GET:
        df = end_df()  # Assuming end_df returns a Pandas DataFrame

        # Initialize variables
        pc_nos = []  # Initialize pc_nos to an empty list

        # Handle the refresh action
        if 'refresh' in request.GET:
            state_filter = ''
            pc_no_filter = ''
            res1_filter = ''

            # Get the total number of rows in the unfiltered DataFrame
            total_rows = len(df)

            # Reset the context to default values
            context = {
                'headers': [],
                'page_obj': None,
                'totalpages': range(1, 2),  # Show only one page (initial state)
                'success_message': 'Filter refreshed',
                'states': sorted(end_df()['state'].unique()),  # Reload the states for initial selection
                'pc_nos': [],
                'res1_values': [],
                'selected_state': state_filter,
                'selected_pc_no': pc_no_filter,
                'selected_res1': res1_filter,
                'total_rows': 0,  # Reset total_rows display
            }

        else:
            df_filtered = df.copy()  # Initialize df_filtered with the original DataFrame

            # Apply the first filter (State)
            state_filter = request.GET.get('filter_state')
            if state_filter:
                df_filtered = df_filtered[df_filtered['state'] == state_filter]

                # Sort PC_NO as integers for correct ordering
                df_filtered['PC_NO'] = df_filtered['PC_NO'].astype(int).astype(str)

                pc_nos = sorted(df_filtered['PC_NO'].unique(), key=lambda x: int(x)) if state_filter else []

            # Apply the second filter (PC_NO)
            pc_no_filter = request.GET.get('filter_pc_no')
            if pc_no_filter:
                df_filtered = df_filtered[df_filtered['PC_NO'] == pc_no_filter]

            # Apply the third filter (RES1)
            res1_filter = request.GET.get('filter_res1')
            if res1_filter:
                df_filtered = df_filtered[df_filtered['RES1'] == res1_filter]

            # Sort the DataFrame by specific columns (after all filters are applied)
            sorted_df = df_filtered.sort_values(by=['state', 'PC_NO', 'RES1'])

            # Get the total number of rows
            total_rows = len(sorted_df)

            # Convert DataFrame to a list of lists for easier rendering in the template
            table_data = sorted_df.values.tolist()

            # Get table headers
            headers = sorted_df.columns.tolist()

            # Set up pagination
            paginator = Paginator(table_data, 30)  # 30 items per page
            page_number = request.GET.get('page')  # Get the page number from the request
            page_obj = paginator.get_page(page_number)
            totalpages = page_obj.paginator.num_pages

            # Get unique values for the first filter (state)
            states = sorted(end_df()['state'].unique())  # Ensure states are sorted

            # Ensure PC_NO values are filtered and sorted numerically if state is selected
            if state_filter:
                pc_nos = sorted(df_filtered['PC_NO'].astype(int).astype(str).unique(), key=lambda x: int(x))

            # Get unique values for RES1 after applying the previous filters
            res1_values = sorted(df_filtered['RES1'].unique()) if df_filtered['RES1'].notna().any() else []

            # Determine the appropriate success message
            success_message = ''
            if request.method == 'POST':
                success_message = 'Data updated successfully!'
            elif state_filter or pc_no_filter or res1_filter:
                success_message = 'Filter applied'

            context = {
                'headers': headers,
                'page_obj': page_obj,
                'totalpages': range(1, totalpages + 1),
                'success_message': success_message,
                'states': states,
                'pc_nos': pc_nos,  # Use pc_nos which is now always initialized
                'res1_values': res1_values,
                'selected_state': state_filter,
                'selected_pc_no': pc_no_filter,
                'selected_res1': res1_filter,
                'total_rows': total_rows,  # Add total_rows to the context
            }

        return render(request, 'home_page.html', context)
