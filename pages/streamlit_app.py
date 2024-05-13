# # import requests
# # import streamlit as st

# # def get_dags(url):
# #     username = 'admin'
# #     password = 'Atharva123'
# #     auth = (username, password)
# #     headers = {"Content-type": "application/json"}
# #     response = requests.get(url, headers=headers, auth=auth)
# #     return response.json()


# # def run_dag(dag_id):
# #     url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
# #     username = 'admin'
# #     password = 'Atharva123'
# #     auth = (username, password)
# #     headers = {"Content-type": "application/json"}
# #     response = requests.post(url, headers=headers, auth=auth)
# #     if response.status_code == 200:
# #         st.success("DAG run successfully triggered!")
# #     else:
# #         st.error("Failed to trigger DAG run.")

# # import uuid
# # import datetime

# # def trigger_dag(url):
# #     url = "http://localhost:8080/api/v1/dags/update_CSV/dagRuns"
# #     username = 'admin'
# #     password = 'Atharva123'
# #     auth = (username, password)
# #     headers = {"Content-type": "application/json"}
# #     dag_run_id = str(uuid.uuid4())
# #     logical_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
# #     payload = {
# #         "conf": {},
# #         "dag_run_id": dag_run_id,
# #         "data_interval_end": logical_date,
# #         "data_interval_start": logical_date,
# #         "logical_date": logical_date,
# #         "note": "string"
# #     }
# #     response = requests.post(url, headers=headers, auth=auth, json=payload)
# #     if response.status_code == 200:
# #         st.success("DAG run successfully triggered!")
# #     else:
# #         st.error("Failed to trigger DAG run.")


# # def main():
# #     st.title("DAGs Dashboard")

# #     show_dags = st.button("Show DAGs")
# #     if show_dags:
# #         url = "http://localhost:8080/api/v1/dags?limit=100&only_active=true&paused=false"
# #         dags = get_dags(url)
# #         st.write("## Available DAGs:")
# #         for dag in dags["dags"]:
# #             st.write(f"**DAG ID:** {dag['dag_id']}")

  
# #     st.title("Preprocessing")
# #     st.checkbox("Pre-Processed file")
# #     show=st.button("Generate Results")
# #     if show:
# #         url="http://localhost:8080/api/v1/dags/update_CSV/dagRuns"
# #         trigger_dag(url)
# #         # st.write("Dag triggered")

        
# #             # if dag['dag_id'] == 'update_CSV':
# #             #     if st.button(f"Run DAG: {dag['dag_id']}"):
# #             #         run_dag(dag['dag_id'])

# # if __name__ == "__main__":
# #     main()
# import requests
# import streamlit as st

# def get_dags(url):
#     username = 'admin'
#     password = 'Atharva123'
#     auth = (username, password)
#     headers = {"Content-type": "application/json"}
#     response = requests.get(url, headers=headers, auth=auth)
#     return response.json()

# def run_dag(dag_id):
#     url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
#     username = 'admin'
#     password = 'Atharva123'
#     auth = (username, password)
#     headers = {"Content-type": "application/json"}
#     response = requests.post(url, headers=headers, auth=auth)
#     if response.status_code == 200:
#         st.success("DAG run successfully triggered!")
#     else:
#         st.error("Failed to trigger DAG run.")

# import uuid
# import datetime

# def trigger_dag(url):
#     url = "http://localhost:8080/api/v1/dags/update_CSV/dagRuns"
#     username = 'admin'
#     password = 'Atharva123'
#     auth = (username, password)
#     headers = {"Content-type": "application/json"}
#     dag_run_id = str(uuid.uuid4())
#     logical_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#     payload = {
#         "conf": {},
#         "dag_run_id": dag_run_id,
#         "data_interval_end": logical_date,
#         "data_interval_start": logical_date,
#         "logical_date": logical_date,
#         "note": "string"
#     }
#     response = requests.post(url, headers=headers, auth=auth, json=payload)
#     if response.status_code == 200:
#         st.success("DAG run successfully triggered!")
#     else:
#         st.error("Failed to trigger DAG run.")

# def main():
#     st.title("DAGs Dashboard")

#     show_dags = st.button("Show DAGs")
#     if show_dags:
#         url = "http://localhost:8080/api/v1/dags?limit=100&only_active=true&paused=false"
#         dags = get_dags(url)
#         st.write("## Available DAGs:")
#         for dag in dags["dags"]:
#             st.write(f"**DAG ID:** {dag['dag_id']}")

#     st.title("Preprocessing") 
#     st.checkbox("Pre-Processed file")
#     show = st.button("Generate Results")
#     if show:
#         url = "http://localhost:8080/api/v1/dags/update_CSV/dagRuns"
#         trigger_dag(url)
#         st.success("DAG triggered successfully!")
#         st.markdown("<h2 style='text-align: center;'>Download Updated CSV</h2>", unsafe_allow_html=True)
#         st.download_button(
#             label="Press to Download",
#             data="http://localhost:8080/files/updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx",
#             file_name="updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx",  # Update the file name here
#             key='download-csv'
#         )

# if __name__ == "__main__":
#     main()


import requests
import streamlit as st
import pandas as pd
import uuid
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


EXCEL_FILE_PATH = 'FamilyOfficeEntityDataSampleV1.1.xlsx'
PREPROCESSED_FILE_PATH = EXCEL_FILE_PATH.replace('.xlsx', '_preprocessed.xlsx')
PREPROCESSED_FILE_PATH_Y = EXCEL_FILE_PATH.replace('.xlsx', '_Y_preprocessed.xlsx')

def file_exists(file_path):
    return os.path.exists(file_path)

# def calculate_age(birthdate):
#     today = datetime.today()
#     age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
#     return age

# def read_and_update_excel():
#     excel_file_path = 'FamilyOfficeEntityDataSampleV1.1.xlsx'
    
#     # Define the path for the preprocessed file
#     preprocessed_file_path = excel_file_path.replace('.xlsx', '_preprocessed.xlsx')
    
#     # Read Excel file using 'openpyxl' engine
#     df = pd.read_excel(excel_file_path, sheet_name='Client Profile', engine='openpyxl')
    
#     # Make a copy of the DataFrame
#     df_updated = df.copy()
    
#     # Convert 'Date of Birth' column to datetime
#     df_updated['Date of Birth'] = pd.to_datetime(df_updated['Date of Birth'])
    
#     # Calculate age
#     df_updated['Age'] = df_updated['Date of Birth'].apply(calculate_age)
    
#     # Save the updated Excel file using 'openpyxl' engine
#     with pd.ExcelWriter(preprocessed_file_path, engine='openpyxl') as writer:
#         df_updated.to_excel(writer, index=False)
    
#     print(f"Excel file updated and saved successfully to {preprocessed_file_path} on May 13")


# def calculate_age_family_members():
#     excel_file_path = 'FamilyOfficeEntityDataSampleV1.1.xlsx'
    
#     # Define the path for the preprocessed file specific to family members
#     preprocessed_file_path_Y = excel_file_path.replace('.xlsx', '_Y_preprocessed.xlsx')
    
#     # Read Excel file using 'openpyxl' engine for 'Family Members' sheet
#     df = pd.read_excel(excel_file_path, sheet_name='Family Members', engine='openpyxl')
#     df_updated = df.copy()
    
#     # Convert 'Date of Birth' column to datetime and calculate age
#     df_updated['Date of Birth'] = pd.to_datetime(df_updated['Date of Birth'])
#     df_updated['Age'] = df_updated['Date of Birth'].apply(calculate_age)
    
#     # Save the updated DataFrame to the new preprocessed file path
#     with pd.ExcelWriter(preprocessed_file_path_Y, engine='openpyxl') as writer:
#         df_updated.to_excel(writer, index=False)
    
#     print(f"Family Members Excel file updated successfully and saved to {preprocessed_file_path_Y} on May 13")



def calculate_age(birthdate):
    today = datetime.today()
    return today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

def read_and_update_excel():
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name='Client Profile', engine='openpyxl')
    df['Date of Birth'] = pd.to_datetime(df['Date of Birth'])
    df['Age'] = df['Date of Birth'].apply(calculate_age)
    df.to_excel(PREPROCESSED_FILE_PATH, index=False, engine='openpyxl')
    print(f"Excel file updated and saved successfully to {PREPROCESSED_FILE_PATH}")

def calculate_age_family_members():
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name='Family Members', engine='openpyxl')
    df['Date of Birth'] = pd.to_datetime(df['Date of Birth'])
    df['Age'] = df['Date of Birth'].apply(calculate_age)
    df.to_excel(PREPROCESSED_FILE_PATH_Y, index=False, engine='openpyxl')
    print(f"Family Members Excel file updated successfully and saved to {PREPROCESSED_FILE_PATH_Y}")
  
# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="streamlit_app",
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    task1 = PythonOperator(
        task_id="read_and_update_excel",
        python_callable=read_and_update_excel)
    
    task2 = PythonOperator(
        task_id="calculate_age_family_members",
        python_callable=calculate_age_family_members)
    

df = pd.read_excel(PREPROCESSED_FILE_PATH, engine='openpyxl')
# file_path_Y = preprocessed_file_path
df1 = pd.read_excel(PREPROCESSED_FILE_PATH_Y, engine='openpyxl')
# file_path_Y="/Users/atharvabapat/airflow/Y_New_Processed.xlsx"
# df1=pd.read_excel(file_path_Y, engine='openpyxl')
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Client Profile')
    processed_data = output.getvalue()
    return processed_data

def convert_df_to_excel_1(df1):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df1.to_excel(writer, index=False, sheet_name='Family Members')
    processed_data = output.getvalue()
    return processed_data

def get_dags(url):
    username = 'admin'
    password = 'Atharva123'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()

def run_dag(dag_id):
    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
    username = 'admin'
    password = 'Atharva123'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    response = requests.post(url, headers=headers, auth=auth)
    if response.status_code == 200:
        st.success("DAG run successfully triggered! on May 10")
    else:
        st.error("Failed to trigger DAG run.")


def trigger_dag(url):
    username = 'admin'
    password = 'Atharva123'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    dag_run_id = str(uuid.uuid4())
    logical_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "conf": {},
        "dag_run_id": dag_run_id,
        "data_interval_end": logical_date,
        "data_interval_start": logical_date,
        "logical_date": logical_date,
        "note": "string"
    }
    response = requests.post(url, headers=headers, auth=auth, json=payload)
    if response.status_code == 200:
        st.success("DAG run successfully triggered! on May 10")
    else:
        st.error("Failed to trigger DAG run.")

def main():
    if not file_exists(PREPROCESSED_FILE_PATH) or not file_exists(PREPROCESSED_FILE_PATH_Y):
        print("Required files not found, please check if DAG has run successfully.")
        return
    
    df = pd.read_excel(PREPROCESSED_FILE_PATH, engine='openpyxl')
    df1 = pd.read_excel(PREPROCESSED_FILE_PATH_Y, engine='openpyxl')

    st.title("DAGs Dashboard")

    show_dags = st.button("Show DAGs")
    if show_dags:
        url = "http://localhost:8080/api/v1/dags?limit=100&only_active=true&paused=false"
        dags = get_dags(url)
        st.write("## Available DAGs:")
        for dag in dags["dags"]:
            st.write(f"**DAG ID:** {dag['dag_id']}")

    

# Create columns for the checkboxes
st.title("Famiology Compute Engine")
col1, col2 = st.columns(2)

with col1:
    x_dataset = st.checkbox("Connect to X dataset")
    y_dataset = st.checkbox("Connect to Y dataset")

with col2:
    x_file= st.checkbox("Processed X file")
    y_file = st.checkbox("Processed Y file")

    show = st.button("Generate Results")
    if show:
        if x_file and x_dataset:
            data = convert_df_to_excel(df)
            print("Inside X dataset")
            url = "http://localhost:8080/api/v1/dags/streamlit_app/dagRuns"
            trigger_dag(url)
            st.success("DAG triggered successfully!")
            st.markdown("<h2 style='text-align: center;'>Download Updated CSV</h2>", unsafe_allow_html=True)
            st.download_button(
                label="Press to Download X processed file",
                data=data,
                file_name="Processed_X_file.xlsx",  # Update the file name here
            # key='download-csv'
            # mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        if y_file and y_dataset:
            data = convert_df_to_excel_1(df1)
            print("Inside Y dataset")
            url = "http://localhost:8080/api/v1/dags/streamlit_app/dagRuns"
            trigger_dag(url)
            st.download_button(
                label="Press to Download Y processed file",
                data=data,
                file_name="Processed_Y_file.xlsx",  # Update the file name here
            )
            st.success("DAG 'Processed_Y_file' triggered successfully!")
    # st.title("Processed_Y_file")
    # show_processed = st.button("Run Processed_Y_file")
    # if show_processed:
        

if __name__ == "__main__":
    main()
