import pandas as pd
import re

def clean_text(text):
    """Cleans text by removing URLs, mentions, and special characters."""
    if not isinstance(text, str):
        return ''  # Return empty string if the value is not a string
    
    text = re.sub(r'http\S+|www\S+', '', text)  # Remove URLs
    text = re.sub(r'@\w+', '', text)  # Remove mentions
    text = re.sub(r'[^a-zA-Z0-9\s.,?!;\'\"]', '', text)  # Remove special characters
    return ' '.join(text.lower().split())  # Convert to lowercase and strip whitespace

def clean_csv(input_file, output_file):
    """Loads CSV, cleans text, and saves cleaned file."""
    # Load the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure 'comment_id' and 'text' columns exist
    if 'comment_id' not in df.columns or 'text' not in df.columns:
        print("Error: The CSV file does not contain 'comment_id' or 'text' columns.")
        return
    
    # Clean the text column
    df['text'] = df['text'].apply(clean_text)
    
    # Keep only 'comment_id' and 'text' columns
    cleaned_df = df[['comment_id', 'text']]
    
    # Save the cleaned DataFrame to a new CSV file
    cleaned_df.to_csv(output_file, encoding="utf-8", index=False)
    print(f"Cleaned file saved as: {output_file}")

# Usage
input_file = 'comments_with_replies.csv'  # Your input CSV file
output_file = 'cleaned_comments.csv'      # Output cleaned CSV file
clean_csv(input_file, output_file)
