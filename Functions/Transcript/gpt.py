import openai

openai.api_key = "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"


response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
            {"role": "system", "content": "You are a chatbot"},
            {"role": "user", "content": "My name is Juan"},
        ]
)

result = ''
for choice in response.choices:
    result += choice.message.content

print(result)



response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
            {"role": "user", "content": "What is my name?"},
        ]
)

result = ''
for choice in response.choices:
    result += choice.message.content

print(result)