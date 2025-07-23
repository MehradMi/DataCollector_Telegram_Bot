import os
from dotenv import load_dotenv
from openai import OpenAI

# Load Environment Variables Then OPEN_AI_KEY
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# =============================== 

# Initialize OpenAI Client
client = OpenAI(api_key=OPENAI_API_KEY)
# =============================== 

def calculate_timestamp(date, time_now):
    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"""
                                    You are a strict date parser.
                                    I will give you a date in one of the following formats:

                                    A calendar date like "9 June" (case-insensitive, no year)

                                    A relative time like "3h", "2d", "1w", 4m, or 1y, representing hours, days, weeks, months, or years ago.

                                    You must respond with:

                                    The exact date and time of that event in SQLite datetime format:

                                    ruby
                                    Copy
                                    Edit
                                    YYYY-MM-DD HH:MM:SS
                                    ⚠️ Rules:

                                    Return only the datetime. No labels. No comments. No other text.

                                    Assume system timezone is Asia/Tehran

                                    Assume current date and time is exactly: {time_now} (Tehran time)

                                    If ambiguous (e.g., "9 June"), assume it refers to the most recent past occurrence

                                    Now wait for me to input a date. Return only the SQLite-formatted date and nothing else.
                                """
                    },
                    {
                        "type": "input_text",
                        "text": date
                    }
                ]
            }
        ]
    )

    return response.output[0].content[0].text.strip()
