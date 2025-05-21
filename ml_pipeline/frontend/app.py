import streamlit as st
import requests

# Configure the page
st.set_page_config(
    page_title="Spotify Track Popularity Predictor",
    page_icon="🎵",
    layout="wide"
)

# Title
st.title("🎵 Spotify Track Popularity Predictor")
st.write("Predict the popularity of a track based on its audio features")

# Sidebar
st.sidebar.header("About")
st.sidebar.write("This app predicts the popularity of a track \
                using machine learning models trained on Spotify data.")

# Main content
def main():
    # Placeholder for feature inputs
    st.header("Track Features")
    with st.form("prediction_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            acousticness = st.slider("Acousticness", 0.0, 1.0, 0.5)
            danceability = st.slider("Danceability", 0.0, 1.0, 0.5)
            energy = st.slider("Energy", 0.0, 1.0, 0.5)
            instrumentalness = st.slider("Instrumentalness", 0.0, 1.0, 0.5)
            key = st.selectbox("Key", range(12))
            liveness = st.slider("Liveness", 0.0, 1.0, 0.5)

        with col2:
            loudness = st.slider("Loudness", -60.0, 0.0, -30.0)
            mode = st.selectbox("Mode", [0, 1])
            speechiness = st.slider("Speechiness", 0.0, 1.0, 0.5)
            tempo = st.slider("Tempo", 0.0, 250.0, 120.0)
            time_signature = st.selectbox("Time Signature", range(3, 8))
            valence = st.slider("Valence", 0.0, 1.0, 0.5)

        submitted = st.form_submit_button("Predict Popularity")
        
        if submitted:
            # Prepare the features
            features = {
                "acousticness": acousticness,
                "danceability": danceability,
                "energy": energy,
                "instrumentalness": instrumentalness,
                "key": key,
                "liveness": liveness,
                "loudness": loudness,
                "mode": mode,
                "speechiness": speechiness,
                "tempo": tempo,
                "time_signature": time_signature,
                "valence": valence
            }
            
            try:
                # Make prediction request to API
                response = requests.post(
                    f"{st.secrets.get('API_URL', 'http://api:8080')}/predict",
                    json=features
                )
                
                if response.status_code == 200:
                    result = response.json()
                    st.success(f"Predicted Popularity Score: \
                               {result['popularity']:.2f}")
                    st.info(f"Model Version: {result['model_version']}")
                else:
                    st.error("Error making prediction. Please try again.")
            except Exception as e:
                st.error(f"Error connecting to API: {str(e)}")

if __name__ == "__main__":
    main()