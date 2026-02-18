#verision v1.10.1
#this version includes the sms and Whatsapp data fetch and prepare and download option

import streamlit as st
import pandas as pd
import os
import streamlit.components.v1 as components
import io
import psycopg2
import time
import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


path = os.path.dirname(os.path.abspath(__file__))


# Page config
st.set_page_config(page_title="Data Processing Tool")

st.image(path + "/logo.png",width=100) 
#        <img src="{path}/asset/cmwssb_logo.jpeg" width="100">
st.header("Data Processing Tool")
#st.image()
#st.title("Welcome to CMWSSB Data Processing Tool")


def db_connection():
            with st.expander("Upload to Database", expanded=True):
                col1, col2, col3 = st.columns(3)

            # Left: Config file upload
            with col1:
                config_file = st.file_uploader("Upload DB Config (JSON)", type=["json"])

            # Middle: OR text
            with col2:
                st.markdown("<br><h4 style='text-align: center;'>OR</h4>", unsafe_allow_html=True)

            # Right: Manual inputs
            with col3:
                host = st.text_input("Host")
                port = st.text_input("Port")
                dbname = st.text_input("Database")
                user = st.text_input("User")
                password = st.text_input("Password", type="password")

            # Connect button
#            connect_btn = st.button("🔗 Connect to Database")
               
            if st.button("🔗 Connect to Database"):
                try:
                    if config_file:
                        import json
                        config = json.load(config_file)
                        conn = psycopg2.connect(**config)
                    else:
                        conn = psycopg2.connect(
                            host=host,
                            port=port,
                            dbname=dbname,
                            user=user,
                            password=password,
                        )
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}") 
                    conn = None
            else:
                    st.warning("Please provide connection details and click Connect.")
                    conn = None    
                                       
            return conn



# ✅ Safe initialization for session_state keys
for key, default in {
    "combined_df": None,
    "preview_df": None,
    "fetch_prepare_done": False,
    "df": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default




# New choice: what to do with the data
action =st.radio(
        "Choose action:",
        options=["Combine & Download",  "Split"],
        index=None
    )





if action != "Message" and action != None:

    # Input type
    input_format = st.radio("Input format:", options=["Excel", "CSV"], index=0)

    if input_format == "CSV":
        file_types = ["csv"]
        exclude_sheet = None   # No exclude for CSV
    elif input_format == "Excel":
        file_types = ["xlsx", "xls"]
        exclude_sheet = st.text_input("Enter the sheet name to exclude:")


        
    # File uploader
    uploaded_files = st.file_uploader(
        "Choose files", type=file_types, accept_multiple_files=True
    )

    # Process button
    if st.button("Process Files") and uploaded_files:
        dfs = []
        for file in uploaded_files:
            file.seek(0)  # reset pointer
            if input_format == "Excel":
                sheets = pd.read_excel(file, sheet_name=None)
                for sheet_name, df in sheets.items():
                    if exclude_sheet and sheet_name == exclude_sheet:
                        continue
                    df['file_name'] = os.path.basename(file.name)
                    df['sheet_name'] = sheet_name
                    dfs.append(df)
            else:  # CSV
                df = pd.read_csv(file)
                df['file_name'] = os.path.basename(file.name)
                dfs.append(df)

        if dfs:
            st.session_state["combined_df"] = pd.concat(dfs, ignore_index=True)
            st.success("✅ Files processed successfully! Now choose an output format below.")
        else:
            st.warning("No data found in the selected files.")

    print(st.session_state["combined_df"])
# After files are processed and combined_df is stored
if "combined_df" in st.session_state and st.session_state["combined_df"] is not None:
    combined_df = st.session_state["combined_df"]

    # Create a clean preview for display
    if combined_df is not None:
        preview_df = combined_df.head()
    else:
        preview_df = pd.DataFrame()

    # Clean preview only (columns + values)
    preview_df.columns = preview_df.columns.fillna("").astype(str)
#   due to the further warning the below line is commented and changed
#    preview_df = preview_df.fillna("")
    preview_df = preview_df.fillna("").infer_objects(copy=False)

    st.subheader("Preview of merged file")
    # Show only first rows
    st.dataframe(preview_df, width='stretch')
   

    if action == "Combine & Download":
        # same flow as before
        output_format = st.radio(
            "Need output format:",
            options=["Excel", "CSV"],
            index=None
        )

        if output_format == "Excel":
            output = io.BytesIO()
            combined_df.to_excel(output, index=False, engine="xlsxwriter")
            output.seek(0)
            st.download_button(
                label="📥 Download Excel",
                data=output,
                file_name="processed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        elif output_format == "CSV":
            csv_data = combined_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name="processed.csv",
                mime="text/csv"
            )
        else:
            st.info("ℹ️ Please select an output format to enable download.")

    elif action == "Upload to DB":


#        if st.button("🔗 Connect to Database"):
#            try:
        if(st.session_state.db_conn == None):
            conn = db_connection()

            st.session_state.db_conn = conn

        if st.session_state.db_conn:

                    # ✅ Fetch available schemas
            with st.session_state.db_conn.cursor() as cur:
                            cur.execute(
                                """select schema_name FROM information_schema.schemata
                                WHERE schema_name not like '%pg_%' and  schema_name <> 'information_schema';"""
                            )
                            schemas = [row[0] for row in cur.fetchall()]
            st.session_state.schemas = schemas

            st.success("✅ Database connection successful!")

                

            # If schemas are loaded, show dropdown outside expander
            if st.session_state.db_conn and st.session_state.schemas:
                schema_choice = st.selectbox(
                    "Select Schema",[None] + st.session_state.schemas,
                    key="schema_select"
                )
                if schema_choice != "None":
                    st.session_state.selected_schema = schema_choice
                st.info(f"📂 You selected schema: **{schema_choice}**")

            # Table input
            if st.session_state.selected_schema:
                table_name = st.text_input("Enter Table Name to Upload")

                if st.button("⬆️ Upload Data to Table"):
                    try:
                        with st.session_state.db_conn.cursor() as cur:
                            # Create table if not exists (basic structure: all text)
                            cols = [f'"{c}" TEXT' for c in combined_df.columns]
                            create_sql = f'CREATE TABLE IF NOT EXISTS "{st.session_state.selected_schema}"."{table_name}" ({",".join(cols)});'
                            cur.execute(create_sql)

                            # Insert data
    #                        for _, row in combined_df.iterrows():
    #                            placeholders = ",".join(["%s"] * len(row))
    #                            insert_sql = f'INSERT INTO "{st.session_state.selected_schema}"."{table_name}" VALUES ({placeholders});'
    #                            cur.execute(insert_sql, tuple(row.astype(str)))
    #                        st.session_state.db_conn.commit()

                            # Upload DataFrame to DB in batches with progress bar
                            def upload_dataframe_in_batches(df, cur,conn, schema, table_name, batch_size=10000):
                            #    print("get in upload_dataframe_in_batches")

                                total_rows = len(df)
                                num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)

                                progress_bar = st.progress(0, text="Starting upload...")
                                status_text = st.empty()

                                for i in range(num_batches):
                                    start = i * batch_size
                                    end = min(start + batch_size, total_rows)
                                    batch_df = df.iloc[start:end]

                                    # Prepare INSERT
                                    cols = ",".join([f'"{col}"' for col in batch_df.columns])
                                    values_placeholder = ",".join(["%s"] * len(batch_df.columns))
                                    insert_query = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES ({values_placeholder})'

                                    # Execute batch insert
                                    for row in batch_df.itertuples(index=False, name=None):
                                        cur.execute(insert_query, row)

                                    conn.commit()

                                    # Update progress
                                    percent_complete = int(((i + 1) / num_batches) * 100)
                                    progress_bar.progress((i + 1) / num_batches, text=f"Uploading... {percent_complete}% ({end}/{total_rows} rows)")
                                    status_text.write(f"✅ Batch {i+1}/{num_batches} uploaded ({end}/{total_rows} rows)")

                                    time.sleep(0.1)  # just for smoother progress update

                                cur.close()
                                conn.close()

                                progress_bar.empty()
                                status_text.success("🎉 Upload completed successfully!")
                            # Handle NaN/NaT values properly
                            # 1. Replace NaN/NaT with None
                            combined_df_null = combined_df.where(pd.notnull(combined_df), None)
                            
                            print(combined_df_null.dtypes)
                            # 2. Ensure pandas gives Python-native types (especially for datetimes)
    #                        combined_df_null = combined_df_null.astype(object)           

                            upload_dataframe_in_batches(combined_df_null, cur, st.session_state.db_conn, st.session_state.selected_schema, table_name)

                        st.success(f"✅ Data uploaded to {st.session_state.selected_schema}.{table_name}")

                    except Exception as e:
                        st.error(f"❌ Upload failed: {e}")
        else:
                st.warning("Please provide connection details and click Connect.")                        
        
    elif action == "Split":
        st.info("ℹ️ Split functionality: choose to split by column or by fixed number of rows.")
        split_mode = st.radio(
            "Split mode:",
            options=["By Column", "By Fixed Rows"],
            index=0
        )
        if split_mode == "By Column":
            selected_column = st.selectbox(
                "Select the column to split the data:",
                options=combined_df.columns.tolist(),
                index=None
            )
        else:
            selected_column = None
            num_rows = st.number_input(
                "Number of rows per split:",
                min_value=1,
                value=1000,
                step=1
            )
        exclude_columns = st.multiselect(
            "Select columns to exclude from split data:",
            options=[col for col in combined_df.columns if col != selected_column] if selected_column else combined_df.columns.tolist()
        )    
        file_or_sheet = st.radio(
            "Split by:",
            options=["File", "Sheet"],
            index=None
        )
        if split_mode == "By Column" and selected_column:
            combined_df = combined_df.sort_values(by=selected_column)
        if st.button("Split and Download"):
            if split_mode == "By Column" and selected_column:
                unique_values = combined_df[selected_column].dropna().unique()
                if file_or_sheet == "File":
                    import zipfile
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                        for value in unique_values:
                            subset_df = combined_df[combined_df[selected_column] == value]
                            if exclude_columns:
                                subset_df = subset_df.drop(columns=exclude_columns)
                            safe_value = str(value).replace('/', '_').replace('\\', '_')
                            excel_buffer = io.BytesIO()
                            subset_df.to_excel(excel_buffer, index=False, engine="xlsxwriter")
                            excel_buffer.seek(0)
                            zipf.writestr(f"{safe_value[:31]}.xlsx", excel_buffer.read())
                    zip_buffer.seek(0)
                    st.download_button(
                        label="📥 Download ZIP of Excel Files",
                        data=zip_buffer,
                        file_name="split_files.zip",
                        mime="application/zip"
                    )
                else:  # Sheet
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        for value in unique_values:
                            subset_df = combined_df[combined_df[selected_column] == value]
                            if exclude_columns:
                                subset_df = subset_df.drop(columns=exclude_columns)
                            safe_value = str(value).replace('/', '_').replace('\\', '_')
                            subset_df.to_excel(writer, sheet_name=safe_value[:31], index=False)
                    excel_buffer.seek(0)
                    st.download_button(
                        label="📥 Download Excel with Sheets",
                        data=excel_buffer,
                        file_name="split_sheets.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            elif split_mode == "By Fixed Rows":
                total_rows = len(combined_df)
                num_splits = (total_rows // num_rows) + (1 if total_rows % num_rows != 0 else 0)
                if file_or_sheet == "File":
                    import zipfile
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                        for i in range(num_splits):
                            start = i * num_rows
                            end = min(start + num_rows, total_rows)
                            subset_df = combined_df.iloc[start:end]
                            if exclude_columns:
                                subset_df = subset_df.drop(columns=exclude_columns)
                            excel_buffer = io.BytesIO()
                            subset_df.to_excel(excel_buffer, index=False, engine="xlsxwriter")
                            excel_buffer.seek(0)
                            zipf.writestr(f"split_{i+1}.xlsx", excel_buffer.read())
                    zip_buffer.seek(0)
                    st.download_button(
                        label="📥 Download ZIP of Excel Files",
                        data=zip_buffer,
                        file_name="split_files.zip",
                        mime="application/zip"
                    )
                else:  # Sheet
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        for i in range(num_splits):
                            start = i * num_rows
                            end = min(start + num_rows, total_rows)
                            subset_df = combined_df.iloc[start:end]
                            if exclude_columns:
                                subset_df = subset_df.drop(columns=exclude_columns)
                            sheet_name = f"split_{i+1}"
                            subset_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    excel_buffer.seek(0)
                    st.download_button(
                        label="📥 Download Excel with Sheets",
                        data=excel_buffer,
                        file_name="split_sheets.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.warning("Please select a valid split option.")
#                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                    )
        else:
            st.warning("Waiting for the inputs to be completed.")

else:
         st.warning("Waiting for the inputs to be completed.")


# Footer
st.footer = components.html(
    """
    <style>
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        height: 60px;
        color: #ffffff;
        text-align: center;
        padding: 10px;
    }
    </style>
    <div class="footer">
        <p>Design & Development by <b>Bala</b></p>
    </div>
    """
)
