import time
import csv
import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# Extract the data (tweets)
def extract_tweets():
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--remote-debugging-port=9222')

    # Set up WebDriver with Chrome options
    service = Service(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get('https://x.com/naval')
    time.sleep(5)

    # Initialize variables
    last_height = driver.execute_script("return document.body.scrollHeight")
    tweet_texts = set() 
    action = ActionChains(driver)

    # Main loop to scrape tweets
    while True:
        try:
            # Perform scrolling
            action.send_keys(Keys.PAGE_DOWN).perform()
            time.sleep(2)  # Wait for tweets to load

            # Find tweets
            tweets = driver.find_elements(By.XPATH, '//div/div/div[2]/main/div/div/div/div/div/div[3]/div/div/section/div/div/div/div/div/article/div/div/div[2]/div[2]/div[2]')
            for tweet in tweets:
                tweet_texts.add(tweet.text)  # Add tweet text to the set
            
            # Scroll and get new height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break  # Break if we have reached the bottom of the page
            last_height = new_height
        except Exception as e:
            print(f"An error occurred: {e}")
            break  

    # Save tweets to CSV
    with open('/tmp/naval_tweets.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Tweet'])  # Header
        for tweet in tweet_texts:
            writer.writerow([tweet])
            writer.writerow(['-'*50])

    driver.quit()

    # Transform the data a bit 
    df = pd.read_csv('/tmp/naval_tweets.csv')
    df_filtered = df[~df['Tweet'].str.contains('Show more|Replying to')] # In order to be as complete as possible, tweets that do not contain 'Show more' or 'Replying to' have been filtered out. 
    df_filtered.to_csv('/tmp/naval_tweets.csv', index=False)

# Load the data
def load_to_s3(bucket_name):
    s3_client = boto3.client('s3')
    s3_client.upload_file('/tmp/naval_tweets.csv', bucket_name, 'naval_tweets.csv')
    print(f'Uploaded tweets to S3 bucket: {bucket_name}')