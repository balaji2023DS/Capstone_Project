import streamlit as st
import easyocr
import numpy as np
import pandas as pd
import sqlite3
from PIL import Image

# Function to extract text using easyOCR
def extract_text_from_image(image):
    try:
        # Use PIL to open the image
        image = Image.open(image)
        # Convert the image to NumPy array
        image_np = np.array(image)
    except Exception as e:
        st.error(f"Error loading the image: {e}")
        return ""

    reader = easyocr.Reader(['en'])
    result = reader.readtext(image_np)
    extracted_text = "\n".join([res[1] for res in result])
    return extracted_text

# Function to create the SQLite database and table
def create_database():
    conn = sqlite3.connect('business_cards.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS business_cards
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image BLOB,
                    extracted_text TEXT)''')
    conn.commit()
    conn.close()

# Function to insert data into the database
def insert_data(image, extracted_text):
    conn = sqlite3.connect('business_cards.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO business_cards (image, extracted_text) VALUES (?, ?)', (image, extracted_text))
    conn.commit()
    conn.close()

# Function to fetch all data from the database
def fetch_all_data():
    conn = sqlite3.connect('business_cards.db')
    df = pd.read_sql_query('SELECT * FROM business_cards', conn)
    conn.close()
    return df

# Streamlit UI for user interaction
def main():
    st.title('BizCardX: Extract Business Card Data with easyOCR')

    uploaded_file = st.file_uploader("Upload a business card image", type=["jpg", "png", "jpeg"])

    if uploaded_file is not None:
        # Display the uploaded image
        image = Image.open(uploaded_file)
        st.image(image, caption='Uploaded Image', use_column_width=True)

        # Extract text from the uploaded image using easyOCR
        extracted_text = extract_text_from_image(uploaded_file)

        # Display the extracted information
        st.write("Extracted Data:")
        st.write(extracted_text)

        # Save extracted data and image to the database
        if st.button("Save Data"):
            create_database()
            insert_data(uploaded_file.read(), extracted_text)
            st.success("Data saved successfully!")

    # Ensure the database is created before fetching data
    create_database()

    # Display all stored data in a table
    st.subheader("Stored Data")
    df = fetch_all_data()
    st.dataframe(df)

if __name__ == "__main__":
    main()

