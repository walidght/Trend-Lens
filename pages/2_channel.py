import streamlit as st

st.title("📊 Channel Dashboard")

if st.session_state.get('active_sheet_id') is None:
    st.warning("👈 Please select a Client Sheet in the sidebar to view analytics.")
    st.stop()

st.info(f"Currently viewing analytics for: **{st.session_state['active_sheet_name']}**")
st.write("This is where the 30-day aggregations and scatter plots will go!")