import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables and OPENAI_API_KEY
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# ===========================

# Initialize OpenAI Client
client = OpenAI(api_key=OPENAI_API_KEY)
# ===========================

def categorize(category):
    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": """
                                    You are a strict classifier. I will give you one word or short phrase in either Persian or English. Your job is to map that input to one of the following predefined categories:

                                    general

                                    clothing

                                    medical

                                    restaurant

                                    AI

                                    fun

                                    beauty

                                    meditation

                                    education

                                    inspirational

                                    other

                                    Rules:

                                    Only return one of the above category names — and nothing else.

                                    If the input is something like “food”, “غذا”, “رستوران” → return restaurant.

                                    If it matches none of the listed categories clearly, return other.

                                    Always match based on meaning, not exact wording.

                                    Do not include any explanation or formatting. Just output one word from the list.
                                """
                    },
                    {
                        "type": "input_text",
                        "text": category
                    }
                ]
            }
        ]
    )


    return response.output[0].content[0].text.strip()