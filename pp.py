import pandas as pd

# Define paths
input_path = "/home/ebraheem/Documents/BDA3/Sampled_Amazon_eta.json"
output_path = "/home/ebraheem/Documents/BDA3/PreProcessedData.json"

# Function to preprocess a chunk of data
def preprocess_chunk(chunk):
    # Select only the columns you want to keep
    columns_to_keep = ['asin', 'price', 'title', 'description', 'brand', 'feature']
    
    # Check if 'imageURL' and 'categories' columns exist before selecting
    if 'imageURL' in chunk.columns:
        columns_to_keep.append('imageURL')
    if 'categories' in chunk.columns:
        columns_to_keep.append('categories')

    chunk = chunk[columns_to_keep]

    # Convert 'description' column to string type
    chunk['description'] = chunk['description'].astype(str)
    
    # Handle missing values by filling with defaults or removing
    chunk.fillna({
        'price': 0,  # Assuming missing prices are 0 (or could use an appropriate value)
        'imageURL': 'No Image Available',
        'brand': 'Unknown'
    }, inplace=True)

    # Example of normalizing text fields
    chunk['title'] = chunk['title'].str.title()  # Capitalize each word in the title
    chunk['description'] = chunk['description'].str.replace(r'\n', ' ', regex=True).str.strip()  # Remove new lines and trim spaces
    
    # Check if 'categories' column exists before processing
    if 'categories' in chunk.columns:
        # Convert lists in 'categories' into a comma-separated string if needed
        chunk['categories'] = chunk['categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    
    # Flatten 'features' list into a single string if it is a list
    if chunk['feature'].dtype == object and chunk['feature'].map(lambda x: isinstance(x, list)).any():
        chunk['feature'] = chunk['feature'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    
    return chunk

# Preprocess data in chunks
chunk_size = 100000  # Adjust this based on your system's memory capacity
preprocessed_chunks = []
for chunk in pd.read_json(input_path, chunksize=chunk_size, lines=True):
    preprocessed_chunk = preprocess_chunk(chunk)
    preprocessed_chunks.append(preprocessed_chunk)

# Concatenate preprocessed chunks into a single DataFrame
preprocessed_df = pd.concat(preprocessed_chunks)

# Save the cleaned data to a new JSON file
preprocessed_df.to_json(output_path, orient='records')

print("Data preprocessing complete. Preprocessed data is saved to", output_path)
