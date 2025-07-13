# import library
import streamlit  as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import time as time_2
import warnings
import os
warnings.filterwarnings('ignore')

# import library dari function.py
from generate import get_data_plan_utilisasi, get_data_gps, get_data_validasi_ws, integrate_data
from functions import TransformVisualData, GetDataApi

# Konfigurasi tampilan
st.set_page_config(layout='wide', page_title='SAM Dashboard Utilisasi')
st.markdown("<h1 style='text-align: center;'>Dashboard Utilisasi</h1>", unsafe_allow_html=True)
st.write('')
st.write('')

# Inisialisasi hanya saat pertama kali  
if "date_input" not in st.session_state:
    st.session_state["date_input"] = datetime.today().date()

if "start_time_input" not in st.session_state:
    st.session_state["start_time_input"] = time(7, 0)  # jam 07:00

if "end_time_input" not in st.session_state:
    st.session_state["end_time_input"] = time(17, 0)  # jam 17:00

if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame({})

# Input layout
col1, col2, col3, col4, col5 = st.columns([0.4, 0.4, 0.4, 0.3, 2])
with col1:
    date_input = st.date_input("Tanggal", value=st.session_state["date_input"], format='DD/MM/YYYY')
    st.session_state['date_input'] = date_input

with col2:
    start_time_input = st.time_input("Start Time", value=st.session_state["start_time_input"])
    st.session_state["start_time_input"] = start_time_input

with col3:
    end_time_input = st.time_input("End Time", value=st.session_state["end_time_input"])
    st.session_state["end_time_input"] = end_time_input

with col4:
    st.write('Realtime')
    real_time = st.checkbox('')

# Output area placeholder
output_placeholder = st.empty()

# Logika proses
if date_input:
    if not real_time:
        current_time = datetime.now().strftime('%H:%M:%S')

        # Static mode
        # Ambil data sekali saja
        data = get_data_gps(date=date_input, startTime=start_time_input, endTime=end_time_input)
        st.session_state['data'] = data

        file_list = os.listdir('Datasets')
        try:
            file_list.remove('.DS_Store')
        except:
            file_list = file_list
        
        
        filename = (f"{len(file_list) + 1}. Data Utilisasi {date_input} "f"Waktu Data Diambil {pd.to_datetime(datetime.now()).strftime('%Y-%m-%d %H.%M')}.xlsx")
        data.to_excel(f'./Datasets/{filename}', index=False)
    else:
        # Real-time mode
        # Ambil data berulang setiap 5 detik
        while True:
            current_time = datetime.now().strftime('%H:%M:%S')

            # Ambil data dengan waktu terkini
            data = get_data_gps(date=date_input, startTime=start_time_input, endTime=current_time)
            st.session_state['data'] = data

            file_list = os.listdir('Datasets')
            
            try:
                file_list.remove('.DS_Store')
            except:
                file_list = file_list

            with output_placeholder:
                st.write(data)

            filename = (f"{len(file_list) + 1}. Data Utilisasi {date_input} "f"Waktu Data Diambil {pd.to_datetime(datetime.now()).strftime('%Y-%m-%d %H.%M')}.xlsx")
            data.to_excel(f'./Datasets/{filename}', index=False)

            time_2.sleep(5)