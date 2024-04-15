import requests
import json
import openai
import pandas as pd
from flask import Flask, request, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_backend', methods=['POST'])
def execute_backend():
    def get_access_token():
        url = "https://accounts.zoho.com/oauth/v2/token"
        refresh_token = "1000.f2baa25b7c6854a0e1364c1ac4ce0fe2.0046087b3dc8c92b467635b665e48f06"
        client_id = "1000.GQTT0N1TQ2W08YWT92KL2DKGZXKVAJ"
        client_secret = "00c93a9bb335bcae057124aaddaffadd3eec373339"
        grant_type = "refresh_token"

        payload = {
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type
        }

        response = requests.post(url, data=payload)

        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            print("Failed to obtain token. Status code:", response.status_code)
            print("Response:", response.text)
            return None

    def generate_sql_query(user_input, database_schema):
        # Initialize OpenAI API with your API key
        openai.api_key = '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'

        # Prepare prompt for OpenAI API using user input and database schema
        prompt = f"User input: {user_input}\nDatabase Schema: {json.dumps(database_schema)}\nQuery:"

        # Generate SQL query using OpenAI API
        response = openai.completions.create(
            model="gpt-3.5-turbo-instruct",  # Use a model known for better understanding
            prompt=prompt,
            temperature=0.7,
            max_tokens=150,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )

        # Retrieve the generated SQL query from the API response
        generated_query = response.choices[0].text.strip()

        return generated_query

    def fetch_data_from_zoho(sql_query, access_token, org_id, workspace_id):
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "ZANALYTICS-ORGID": org_id,
            "Content-Type": "application/json"
        }

        config = {
            "responseFormat": "json",
            "sqlQuery": sql_query
        }

        params = {
            "CONFIG": json.dumps(config)
        }

        url = f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{workspace_id}/data"

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            job_id = data.get("data", {}).get("jobId")
            if job_id:
                return job_id
            else:
                print("Failed to retrieve job ID from response:", data)
                return None
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            print(response.json())
            return None

    def process_data(data):
        processed_data = []
        for item in data:
            for key, value in item.items():
                if pd.notna(value):  # Check if value is not NaN
                    processed_data.append({key: value})
        return processed_data

    user_input = request.form.get('user_input')
    org_id = "784670108"
    workspace_id = "2588669000002757017"

    database_schema = {
        "Projects": [
            "Project Name",
            "Owner",
            "Start Date",
            "End Date",
            "Status",
            "Delivery Team",
            "Project Efforts (Man Days)",
            {
                "column_name": "Project ID",
                "primary_key": True
            },
            "Created Time",
            {
                "column_name": "Owner ID",
                "foreign_key": True,
                "connected_to": "Users.User ID"
            }
        ],
        "Milestones": [
            "Project Name",
            "Milestone Name",
            "Owner Name",
            "Start Date",
            "End Date",
            "Milestone Status",
            "Milestone ID",
            {
                "column_name": "Owner ID",
                "foreign_key": True,
                "connected_to": "Users.User ID"
            },
            {
                "column_name": "Project ID",
                "foreign_key": True,
                "connected_to": "Projects.Project ID"
            },
            "Created Time",
            "Duration",
            "Actual Time Taken",
            "Milestone Completion Mode",
            "Milestone ID String",
            "Project ID String"
        ],
        "Tasks": [
            {
                "column_name": "Task ID",
                "primary_key": True
            },
            "Task Name",
            "Owner",
            "Priority",
            "Start Date",
            "Due Date",
            "Created Time",
            "Status",
            "Is Overdue",
            "Completion Percentage",
            "Project Name",
            "Milestone Name",
            {
                "column_name": "Project ID",
                "foreign_key": True,
                "connected_to": "Projects.Project ID"
            },
            {
                "column_name": "Milestone ID",
                "foreign_key": True,
                "connected_to": "Milestones.Milestone ID"
            },
            "Parent Task ID",
            {
                "column_name": "Created By ID",
                "foreign_key": True,
                "connected_to": "Users.User ID"
            },
            {
                "column_name": "Owner IDs",
                "foreign_key": True,
                "connected_to": "Users.User ID"
            },
            "Task Delay Time",
            "Task Completion Mode",
            "Actual Time Taken",
            "Time Spent So Far",
            "Duration_1",
            "Duration Unit",
            "Rate Per Hour",
            "Clarity Level",
            "QC Owner"
        ],
        "Users": [
            {
                "column_name": "User ID",
                "primary_key": True
            },
            "User Name",
            "User Email",
            "Role",
            "Profile",
            "Status",
            "ZUID",
            "Zpeople Employee ID",
            "Active Users",
            "Created Time",
            "Last Updated Time",
            "Last Accessed Time"
        ]
    }

    access_token = get_access_token()
    if access_token:
        print("Access token obtained successfully:", access_token)

        sql_query = generate_sql_query(user_input, database_schema)
        print("Generated SQL query:", sql_query)

        sql_query = sql_query.replace('"', "'").strip()

        job_id = fetch_data_from_zoho(sql_query, access_token, org_id, workspace_id)

        if job_id:

            headers = {
                "Authorization": f"Zoho-oauthtoken {access_token}",
                "ZANALYTICS-ORGID": org_id,
                "Content-Type": "application/json"
            }

            url3 = f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{workspace_id}/exportjobs/{job_id}/data"
            response3 = requests.get(url3, headers=headers)

            response_data = response3.json().get('data', [])
            if response_data:
                processed_data = process_data(response_data)
                print(processed_data)
                df = pd.DataFrame(processed_data)
                output = df.to_html(index=False)
                return render_template('output.html', output=output)
            else:
                return render_template('output.html', output="No data received in the response.")
        else:
            return render_template('output.html', output="Failed to fetch job ID.")
    else:
        return render_template('output.html', output="Failed to obtain access token.")

if __name__ == '__main__':
    app.run(debug=True)
